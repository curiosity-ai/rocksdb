using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Db;

/// <summary>
/// Compaction scheduling, manual compaction and write stalls. Port of the compaction parts of
/// db/db_impl/db_impl_compaction_flush.cc (MaybeScheduleFlushOrCompaction, SchedulePendingCompaction,
/// BackgroundCompaction, CompactRange / CompactRangeInternal / RunManualCompaction, ReFitLevel, GetBGJobLimits),
/// DBImpl::DelayWrite (db/db_impl/db_impl_write.cc) and ColumnFamilyData::RecalculateWriteStallConditions.
/// <para>
/// <b>Threads</b>: each automatic compaction runs on its own long-running thread (upstream's LOW priority thread
/// pool) and uses blocking I/O; the number of concurrent automatic compactions is bounded by GetBGJobLimits.
/// Manual compactions run on the caller (sync API) or on a long-running thread awaited by the caller (async API).
/// All bookkeeping below is guarded by the DB mutex.
/// </para>
/// </summary>
internal sealed partial class DbImpl
{
    private readonly Queue<ColumnFamilyData> _compactionQueue = new();      // guarded by mutex
    private readonly HashSet<ColumnFamilyData> _stallTokenHolders = new();   // guarded by mutex
    private WriteController? _writeControllerField;
    private int _bgCompactionScheduled;   // automatic compactions running (guarded by mutex)
    private int _manualCompactionsRunning; // guarded by mutex
    private int _exclusiveManualCompactions; // guarded by mutex
    private int _ingestionsRunning;       // guarded by mutex (num_running_ingest_file_)
    private bool _compactionsEnabled;     // set once open completed (guarded by mutex)
    private readonly CancellationTokenSource _bgCancellation = new();

    /// <summary>Pause of a background compaction thread after a failure (1 s upstream); tests shorten it.</summary>
    internal static TimeSpan CompactionErrorRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    // Cumulative statistics (Interlocked).
    private long _statCompactions;
    private long _statTrivialMoves;
    private long _statCompactionBytesRead;
    private long _statCompactionBytesWritten;
    private long _statFlushBytesWritten;
    private long _statCompactionMicros;

    private WriteController WriteControllerInstance => _writeControllerField ??= new WriteController(Options.DelayedWriteRate);

    /// <summary>Number of compactions currently running (auto + manual).</summary>
    internal int NumRunningCompactions
    {
        get
        {
            lock (_mutex) return _bgCompactionScheduled + _manualCompactionsRunning;
        }
    }

    /// <summary>Total bytes written by compactions so far.</summary>
    internal long CompactionBytesWritten => Interlocked.Read(ref _statCompactionBytesWritten);

    /// <summary>Total bytes written by flushes so far.</summary>
    internal long FlushBytesWritten => Interlocked.Read(ref _statFlushBytesWritten);

    internal void RecordFlushBytes(ulong bytes) => Interlocked.Add(ref _statFlushBytesWritten, (long)bytes);

    // ---------------------------------------------------------------------------------------------------------
    // hooks

    /// <summary>
    /// Called after a new super version was installed for <paramref name="cfd"/> (InstallSuperVersionAndScheduleWork):
    /// recalculates its write stall condition and schedules compactions. REQUIRES: mutex.
    /// </summary>
    private void OnSuperVersionInstalled(ColumnFamilyData cfd)
    {
        RecalculateWriteStall(cfd);
        if (_compactionsEnabled)
        {
            SchedulePendingCompaction(cfd);
            MaybeScheduleCompaction();
        }
    }

    /// <summary>Starts automatic compactions once the DB is open (end of DB::Open). </summary>
    private void EnableBackgroundCompactions()
    {
        lock (_mutex)
        {
            ErrorHandler.EnableAutoRecovery();
            _compactionsEnabled = true;
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                RecalculateWriteStall(cfd);
                SchedulePendingCompaction(cfd);
            }
            MaybeScheduleCompaction();
        }
    }

    /// <summary>Asks running compactions to stop (shutdown).</summary>
    private void CancelBackgroundCompactions()
    {
        try
        {
            _bgCancellation.Cancel();
        }
        catch (AggregateException)
        {
            // Callbacks are not registered; nothing to report.
        }
        lock (_mutex) SignalBackgroundWork();
    }

    // ---------------------------------------------------------------------------------------------------------
    // write stalls

    /// <summary>Port of ColumnFamilyData::RecalculateWriteStallConditions for one CF. REQUIRES: mutex.</summary>
    private void RecalculateWriteStall(ColumnFamilyData cfd)
    {
        WriteController wc = WriteControllerInstance;
        ColumnFamilyCompactionState state = cfd.CompactionState;
        WriteStallCondition previous = state.Stall.Condition;
        if (cfd.Dropped || cfd.Current is null)
        {
            state.Stall.Release(wc);
            _stallTokenHolders.Remove(cfd);
        }
        else
        {
            state.Stall.Recalculate(wc, cfd.NumNotFlushed, cfd.Current.Storage, cfd.Options);
            _stallTokenHolders.Add(cfd);
            MaybeEnqueueStallNotification(cfd, previous, state.Stall.Condition);
        }
        _maybeStalled = wc.IsStopped || wc.NeedsDelay;
    }

    /// <summary>
    /// True if writes are stopped or delayed (WriteController::IsStopped || NeedsDelay). Releases the tokens of dropped
    /// column families first. REQUIRES: mutex.
    /// </summary>
    private bool IsWriteStoppedOrDelayed()
    {
        WriteController wc = WriteControllerInstance;
        if (_stallTokenHolders.Count > 0)
        {
            List<ColumnFamilyData>? dropped = null;
            foreach (ColumnFamilyData c in _stallTokenHolders)
            {
                if (c.Dropped) (dropped ??= new()).Add(c);
            }
            if (dropped is not null)
            {
                foreach (ColumnFamilyData c in dropped)
                {
                    c.CompactionState.Stall.Release(wc);
                    _stallTokenHolders.Remove(c);
                }
            }
        }
        return wc.IsStopped || wc.NeedsDelay;
    }

    /// <summary>
    /// Port of DBImpl::DelayWrite: sleeps according to the delayed write rate, then waits while writes are stopped.
    /// Async writers await (no thread is blocked); synchronous writers block.
    /// </summary>
    private async ValueTask DelayWriteAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken)
    {
        WriteController wc = WriteControllerInstance;
        ulong delayMicros = 0;
        lock (_mutex)
        {
            if (IsDbStopped && ErrorHandler.BgError is { } e) ThrowBackgroundError(e);
            if (!wc.IsStopped && wc.NeedsDelay)
            {
                ulong bytes = 0;
                for (int i = 0; i < _group.Count; i++) bytes += (ulong)(_group[i].Batch?.DataSize ?? 0);
                delayMicros = wc.GetDelay(bytes);
            }
        }
        if (delayMicros > 0)
        {
            if (noSlowdown) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Write stall");
            TimeSpan delay = TimeSpan.FromMicroseconds(delayMicros);
            if (synchronous) Thread.Sleep(delay);
            else await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        while (true)
        {
            Task wait;
            lock (_mutex)
            {
                if (IsDbStopped && ErrorHandler.BgError is { } e) ThrowBackgroundError(e);
                if (!wc.IsStopped) return;
                if (_shuttingDown) throw new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
                wait = _bgSignal.Task;
            }
            if (noSlowdown) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Write stall");
            if (synchronous) wait.Wait(cancellationToken);
            else await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // scheduling

    /// <summary>Port of GetBGJobLimits (compaction part) with the compaction-speedup rule of MaybeScheduleFlushOrCompaction.</summary>
    private int BgCompactionLimit()
    {
        int maxFlushes = Options.MaxBackgroundFlushes;
        int maxCompactions = Options.MaxBackgroundCompactions;
        if (maxFlushes == -1 && maxCompactions == -1)
        {
            // for our first stab implementing max_background_jobs, simply allocate a quarter of the threads to flushes.
            maxFlushes = Math.Max(1, Options.MaxBackgroundJobs / 4);
            maxCompactions = Math.Max(1, Options.MaxBackgroundJobs - maxFlushes);
        }
        maxCompactions = Math.Max(1, maxCompactions);
        if (!WriteControllerInstance.NeedSpeedupCompaction) maxCompactions = 1;
        return maxCompactions;
    }

    /// <summary>Port of SchedulePendingCompaction (EnqueuePendingCompaction). REQUIRES: mutex.</summary>
    private void SchedulePendingCompaction(ColumnFamilyData cfd)
    {
        ColumnFamilyCompactionState s = cfd.CompactionState;
        if (s.QueuedForCompaction || cfd.Dropped || cfd.Options.DisableAutoCompactions) return;
        if (!s.Picker.NeedsCompaction(cfd.Current.Storage)) return;
        s.QueuedForCompaction = true;
        _compactionQueue.Enqueue(cfd);
    }

    /// <summary>
    /// Port of MaybeScheduleFlushOrCompaction (compaction part) + the picking part of BackgroundCompaction: picks
    /// compactions for queued column families while below the limit and starts them. REQUIRES: mutex.
    /// </summary>
    private void MaybeScheduleCompaction()
    {
        if (!_compactionsEnabled || _shuttingDown || IsBgWorkStopped || _bgCancellation.IsCancellationRequested) return;
        if (_exclusiveManualCompactions > 0) return; // an exclusive manual compaction is pending or running
        if (_ingestionsRunning > 0) return; // wait for IngestExternalFile() calls to finish before picking compactions
        int limit = BgCompactionLimit();
        int attempts = _compactionQueue.Count;
        while (_bgCompactionScheduled < limit && attempts-- > 0 && _compactionQueue.TryDequeue(out ColumnFamilyData? cfd))
        {
            cfd.CompactionState.QueuedForCompaction = false;
            if (cfd.Dropped || cfd.Options.DisableAutoCompactions) continue;
            ulong[] snapshots = SnapshotsForCompaction();
            Compaction? c = cfd.CompactionState.Picker.PickCompaction(cfd.Current.Storage, snapshots);
            if (c is null) continue;
            c.InputVersion = cfd.Current;
            cfd.Current.Ref();
            _bgCompactionScheduled++;
            _backgroundJobs++;
            // There may be more work for this column family (another level, another key range).
            SchedulePendingCompaction(cfd);
            var job = new CompactionRun(this, cfd, c, snapshots, isManual: false);
            _ = Task.Factory.StartNew(static s => ((CompactionRun)s!).RunBackground(), job, CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }
    }

    /// <summary>Live snapshot sequence numbers, ascending. REQUIRES: mutex.</summary>
    private ulong[] SnapshotsForCompaction()
    {
        if (_snapshots.Count == 0) return [];
        var list = new List<ulong>();
        _snapshots.GetAll(list, ulong.MaxValue);
        return list.ToArray();
    }

    /// <summary>One compaction execution (BackgroundCompaction after picking).</summary>
    private sealed class CompactionRun
    {
        private readonly DbImpl _db;
        private readonly ColumnFamilyData _cfd;
        private readonly Compaction _c;
        private readonly ulong[] _snapshots;
        private readonly bool _isManual;

        public CompactionRun(DbImpl db, ColumnFamilyData cfd, Compaction c, ulong[] snapshots, bool isManual)
        {
            _db = db;
            _cfd = cfd;
            _c = c;
            _snapshots = snapshots;
            _isManual = isManual;
        }

        public void RunBackground()
        {
            try
            {
                _db.RunCompaction(_cfd, _c, _snapshots, _isManual);
            }
            catch (Exception)
            {
                // Recorded as background error (or ignored for shutdown / dropped column families) by RunCompaction.
            }
        }
    }

    /// <summary>
    /// Runs a registered compaction to completion: trivial move (MANIFEST-only), FIFO deletion or a full
    /// <see cref="CompactionJob"/>; installs the result and releases the inputs. Errors other than shutdown / dropped
    /// column family become the background error. Rethrows the failure for manual callers.
    /// </summary>
    private void RunCompaction(ColumnFamilyData cfd, Compaction c, ulong[] snapshots, bool isManual)
    {
        bool ok = false;
        CompactionJob? job = null;
        Exception? failure = null;
        long compactionStart = 0;
        int jobId = 0;
        try
        {
            CancellationToken ct = _bgCancellation.Token;
            TestHooks.Invoke("DBImpl::RunCompaction:Start");
            jobId = NewJobId();
            NotifyOnCompactionBegin(cfd, c, jobId);
            compactionStart = System.Diagnostics.Stopwatch.GetTimestamp();
            ct.ThrowIfCancellationRequested();
            var edit = new VersionEdit { ColumnFamily = cfd.Id };
            if (c.IsDeletionCompaction)
            {
                c.AddInputDeletions(edit);
            }
            else if (c.IsTrivialMove())
            {
                // Move the files to the next level without rewriting them (BackgroundCompaction trivial move branch).
                for (int l = 0; l < c.NumInputLevels; l++)
                {
                    if (c.Level(l) == c.OutputLevel) continue;
                    foreach (FileMetaData f in c.Inputs(l).Files)
                    {
                        edit.DeleteFile(c.Level(l), f.FileNumber);
                        edit.AddFile(c.OutputLevel, f.Clone());
                    }
                }
                Interlocked.Increment(ref _statTrivialMoves);
            }
            else
            {
                job = new CompactionJob(this, cfd, c, snapshots, NewPendingOutputFileNumber, ct);
                job.RunAsync(synchronous: true).GetAwaiter().GetResult();
                c.AddInputDeletions(edit);
                foreach (FileMetaData f in job.Outputs) edit.AddFile(c.OutputLevel, f);
                edit.BlobFileAdditions.AddRange(job.BlobFileAdditions);
                edit.BlobFileGarbages.AddRange(job.BlobFileGarbages);
                if (job.Outputs.Count > 0) FileSystem.SyncDirectory(DbPaths[(int)Math.Min(c.OutputPathId, (uint)(DbPaths.Count - 1))]);
            }
            lock (_mutex)
            {
                if (cfd.Dropped) ThrowDropped(cfd);
            }
            ct.ThrowIfCancellationRequested();
            TestHooks.Invoke("DBImpl::RunCompaction:BeforeLogAndApply");
            // From here on a failure may still have made the edit durable: leave the blob files to the obsolete-file scan.
            if (job?.BlobBuilder is { } blobOut) blobOut.Committed = true;
            Versions.LogAndApplyAsync(cfd, edit, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
            lock (_mutex)
            {
                InstallSuperVersion(cfd);
            }
            ok = true;
            Interlocked.Increment(ref _statCompactions);
            if (job is not null)
            {
                Interlocked.Add(ref _statCompactionBytesRead, (long)job.Stats.BytesRead);
                Interlocked.Add(ref _statCompactionBytesWritten, (long)job.Stats.BytesWritten);
                Interlocked.Add(ref _statCompactionMicros, (long)job.Stats.Elapsed.TotalMicroseconds);
            }
            OnCompactionCompleted(cfd, c, job, jobId, compactionStart);
        }
        catch (OperationCanceledException e)
        {
            failure = new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress: compaction aborted", StatusSubCode.None, e);
        }
        catch (RocksDbException e) when (e.Code is StatusCode.ColumnFamilyDropped or StatusCode.ShutdownInProgress || cfd.Dropped)
        {
            failure = e;
        }
        catch (ObjectDisposedException e) when (cfd.Dropped || _bgCancellation.IsCancellationRequested)
        {
            failure = new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress: compaction aborted", StatusSubCode.None, e);
        }
        catch (Exception e)
        {
            failure = e;
            SetBackgroundError(e, BackgroundErrorReason.Compaction, walRelated: false);
            if (!isManual)
            {
                // BackgroundCallCompaction: wait a little bit before retrying in case this is an environmental problem
                // (a retryable error is not recorded and the compaction is picked again).
                _bgCancellation.Token.WaitHandle.WaitOne(CompactionErrorRetryDelay);
            }
        }
        finally
        {
            if (jobId != 0)
            {
                try
                {
                    NotifyOnCompactionCompleted(cfd, c, job, jobId, ok ? null : failure ?? new RocksDbException(StatusCode.Incomplete, "Incomplete"));
                }
                catch (Exception e) when (e is IOException or RocksDbException or ObjectDisposedException)
                {
                }
            }
            lock (_mutex)
            {
                if (job is not null)
                {
                    foreach (ulong n in job.CreatedFileNumbers) _pendingOutputs.Remove(n);
                }
                cfd.CompactionState.Picker.ReleaseCompactionFiles(c, ok);
                if (c.InputVersion is not null) Versions.UnrefVersion(c.InputVersion);
                if (!cfd.Dropped && cfd.Current is not null)
                {
                    cfd.Current.Storage.ComputeCompactionScore();
                    RecalculateWriteStall(cfd);
                }
            }
            if (!ok && job is not null)
            {
                foreach (ulong n in job.CreatedFileNumbers) TryDeleteFile(FileNames.TableFileName(DbPaths, n, c.OutputPathId));
            }
            if (job?.BlobBuilder is { } blobBuilder)
            {
                // Unregisters the pending blob outputs; a job that failed before its MANIFEST write deletes its blob files.
                ReleaseBlobOutputs(blobBuilder);
                blobBuilder.Dispose();
            }
            try
            {
                // Before the job is accounted as finished, so shutdown does not race with file deletion.
                PurgeObsoleteFiles();
            }
            catch (Exception e) when (e is IOException or RocksDbException)
            {
                // Deleting obsolete files is best effort.
            }
            lock (_mutex)
            {
                if (isManual) _manualCompactionsRunning--;
                else _bgCompactionScheduled--;
                _backgroundJobs--;
                if (!cfd.Dropped) SchedulePendingCompaction(cfd);
                MaybeScheduleCompaction();
                SignalBackgroundWork();
            }
        }
        if (failure is not null && isManual)
        {
            if (failure is RocksDbException r) throw r.Rethrowable();
            throw RocksDbException.IOError(failure.Message, failure);
        }
    }

    /// <summary>Ingestion started: no new compaction is picked until it finishes (num_running_ingest_file_).</summary>
    private void BeginIngestion()
    {
        lock (_mutex) _ingestionsRunning++;
    }

    private void EndIngestion()
    {
        lock (_mutex)
        {
            _ingestionsRunning--;
            MaybeScheduleCompaction();
            SignalBackgroundWork();
        }
    }

    /// <summary>True if a running compaction of <paramref name="cfd"/> writes to <paramref name="level"/> within the user key range.</summary>
    internal bool RangeOverlapsRunningCompaction(ColumnFamilyData cfd, ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey, int level)
    {
        lock (_mutex) return cfd.CompactionState.Picker.RangeOverlapWithCompaction(smallestUserKey, largestUserKey, level);
    }

    private ulong NewPendingOutputFileNumber()
    {
        ulong n = Versions.NewFileNumber();
        lock (_mutex) _pendingOutputs.Add(n);
        return n;
    }

    // ---------------------------------------------------------------------------------------------------------
    // manual compaction

    /// <summary>
    /// Port of DBImpl::CompactRange / CompactRangeInternal: flushes the memtable, then compacts every level that
    /// overlaps [<paramref name="begin"/>, <paramref name="end"/>] (null = unbounded) down to the last level
    /// (leveled), all files together (universal) or runs the FIFO picker; finally optionally moves the result to
    /// <see cref="CompactRangeOptions.TargetLevel"/> (change_level).
    /// </summary>
    public async ValueTask CompactRangeAsync(ColumnFamilyData cfd, byte[]? begin, byte[]? end, CompactRangeOptions options, bool synchronous,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (options.TargetPathId >= DbPaths.Count) ThrowHelper.InvalidArgument("Invalid target path ID");
        CompactionStyle style = cfd.Options.CompactionStyle;
        if (options.ChangeLevel && style == CompactionStyle.Fifo) ThrowHelper.NotSupported("FIFO compaction does not support change_level.");
        if (style == CompactionStyle.None) ThrowHelper.NotSupported("Compaction style None does not support CompactRange");

        // Flush first, unless there is nothing in memory (flushing takes write-leadership, which a writer waiting on a
        // write stop holds; skipping the no-op flush lets a manual compaction resolve the stop).
        bool flushNeeded;
        lock (_mutex) flushNeeded = !cfd.Mem.IsEmpty || cfd.Imm.Count > 0;
        if (flushNeeded) await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.ManualCompaction, synchronous, cancellationToken).ConfigureAwait(false);

        bool exclusive = options.ExclusiveManualCompaction;
        if (exclusive) await BeginExclusiveManualAsync(synchronous, cancellationToken).ConfigureAwait(false);
        try
        {
            int finalOutputLevel = -1;
            if (style == CompactionStyle.Universal && cfd.NumLevels > 1)
            {
                // Always compact all files together.
                finalOutputLevel = cfd.NumLevels - 1;
                if (cfd.Options.CfAllowIngestBehind) finalOutputLevel--;
                await RunManualCompactionAsync(cfd, CompactionPicker.CompactAllLevels, finalOutputLevel, options, null, null, false, ulong.MaxValue,
                    synchronous, cancellationToken).ConfigureAwait(false);
            }
            else if (style == CompactionStyle.Fifo)
            {
                finalOutputLevel = 0;
                await RunManualCompactionAsync(cfd, 0, 0, options, begin, end, false, ulong.MaxValue, synchronous, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                int firstOverlappedLevel = -1;
                SuperVersion sv = GetReferencedSuperVersion(cfd);
                try
                {
                    VersionStorageInfo vs = sv.Current.Storage;
                    for (int level = 0; level < vs.NumNonEmptyLevels; level++)
                    {
                        if (vs.OverlapInLevel(level, begin, begin is not null, end, end is not null))
                        {
                            firstOverlappedLevel = level;
                            break;
                        }
                    }
                }
                finally
                {
                    ReturnSuperVersion(sv);
                }
                if (firstOverlappedLevel != -1)
                {
                    if (style == CompactionStyle.Universal)
                    {
                        await RunManualCompactionAsync(cfd, firstOverlappedLevel, firstOverlappedLevel, options, begin, end, true, ulong.MaxValue,
                            synchronous, cancellationToken).ConfigureAwait(false);
                        finalOutputLevel = firstOverlappedLevel;
                    }
                    else
                    {
                        ulong nextFileNumber = Versions.CurrentNextFileNumber;
                        // Start compaction from `first_overlapped_level`, one level down at a time, until output level >=
                        // max_overlapped_level.
                        int level = firstOverlappedLevel;
                        finalOutputLevel = level;
                        bool dynamic = cfd.Options.LevelCompactionDynamicLevelBytes;
                        while (true)
                        {
                            // Always allow L0 -> L1 compaction
                            if (level > 0)
                            {
                                if (dynamic)
                                {
                                    if (finalOutputLevel + 1 == cfd.NumLevels) break;
                                }
                                else
                                {
                                    int nonEmpty;
                                    lock (_mutex) nonEmpty = cfd.Current.Storage.NumNonEmptyLevels;
                                    if (finalOutputLevel + 1 >= nonEmpty) break;
                                }
                            }
                            int outputLevel = level + 1;
                            if (dynamic && level == 0) outputLevel = CompactionPicker.CompactToBaseLevel;
                            int baseLevel = await RunManualCompactionAsync(cfd, level, outputLevel, options, begin, end, false, ulong.MaxValue,
                                synchronous, cancellationToken).ConfigureAwait(false);
                            if (outputLevel == CompactionPicker.CompactToBaseLevel)
                            {
                                if (baseLevel <= 0)
                                {
                                    lock (_mutex) baseLevel = Math.Max(1, cfd.Current.Storage.BaseLevel);
                                }
                                level = baseLevel;
                            }
                            else
                            {
                                ++level;
                            }
                            finalOutputLevel = level;
                        }
                        // bottommost level intra-level compaction
                        bool hasFilter = cfd.Options.CompactionFilter is not null || cfd.Options.CompactionFilterFactory is not null;
                        if ((options.BottommostLevelCompaction == BottommostLevelCompaction.IfHaveCompactionFilter && hasFilter)
                            || options.BottommostLevelCompaction is BottommostLevelCompaction.ForceOptimized or BottommostLevelCompaction.Force)
                        {
                            // Use `next_file_number` as `max_file_num_to_ignore` to avoid rewriting newly compacted files
                            // when it is kForceOptimized or kIfHaveCompactionFilter with compaction filter set.
                            ulong ignore = options.BottommostLevelCompaction == BottommostLevelCompaction.Force ? ulong.MaxValue : nextFileNumber;
                            await RunManualCompactionAsync(cfd, finalOutputLevel, finalOutputLevel, options, begin, end, true, ignore,
                                synchronous, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }
            if (options.ChangeLevel && finalOutputLevel >= 0)
            {
                await ReFitLevelAsync(cfd, finalOutputLevel, options.TargetLevel, exclusive, synchronous, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (exclusive) EndExclusiveManual();
        }
    }

    /// <summary>Blocks automatic compaction scheduling and waits for running automatic compactions (exclusive_manual_compaction).</summary>
    private async ValueTask BeginExclusiveManualAsync(bool synchronous, CancellationToken cancellationToken)
    {
        lock (_mutex) _exclusiveManualCompactions++;
        try
        {
            while (true)
            {
                Task wait;
                lock (_mutex)
                {
                    if (_bgCompactionScheduled == 0) return;
                    wait = _bgSignal.Task;
                }
                if (synchronous) wait.Wait(cancellationToken);
                else await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            EndExclusiveManual();
            throw;
        }
    }

    private void EndExclusiveManual()
    {
        lock (_mutex)
        {
            _exclusiveManualCompactions--;
            MaybeScheduleCompaction();
            SignalBackgroundWork();
        }
    }

    private static byte[] MakeInternalKey(ReadOnlySpan<byte> userKey, ulong seq, EntryType type)
    {
        var k = new byte[userKey.Length + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(k, userKey, seq, type);
        return k;
    }

    /// <summary>
    /// Port of DBImpl::RunManualCompaction: repeatedly picks (PickCompactionForCompactRange) and runs compactions of
    /// [begin, end] from <paramref name="inputLevel"/> to <paramref name="outputLevel"/> until the range is covered,
    /// waiting for conflicting compactions. Returns the base level used when <paramref name="outputLevel"/> is
    /// <see cref="CompactionPicker.CompactToBaseLevel"/> (else -1).
    /// </summary>
    private async ValueTask<int> RunManualCompactionAsync(ColumnFamilyData cfd, int inputLevel, int outputLevel, CompactRangeOptions options,
        byte[]? begin, byte[]? end, bool disallowTrivialMove, ulong maxFileNumToIgnore, bool synchronous, CancellationToken cancellationToken)
    {
        byte[]? beginKey = begin is null ? null : MakeInternalKey(begin, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        byte[]? endKey = end is null ? null : MakeInternalKey(end, 0, 0);
        int baseLevel = -1;
        while (true)
        {
            Compaction? c = null;
            byte[]? compactionEnd = null;
            ulong[] snapshots = [];
            while (true)
            {
                Task wait;
                lock (_mutex)
                {
                    if (_shuttingDown || _bgCancellation.IsCancellationRequested || IsDisposed)
                        throw new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
                    if (IsBgWorkStopped && ErrorHandler.BgError is { } e) ThrowBackgroundError(e);
                    if (cfd.Dropped) ThrowDropped(cfd);
                    bool conflict = _ingestionsRunning > 0;
                    if (!conflict)
                    {
                        c = cfd.CompactionState.Picker.PickCompactionForCompactRange(cfd.Current.Storage, inputLevel, outputLevel,
                            options.MaxSubcompactions, beginKey, endKey, out compactionEnd, out conflict, maxFileNumToIgnore, disallowTrivialMove);
                    }
                    if (!conflict)
                    {
                        if (c is not null)
                        {
                            c.BlobGarbageCollectionPolicy = options.BlobGarbageCollectionPolicy;
                            c.BlobGarbageCollectionAgeCutoffOverride = options.BlobGarbageCollectionAgeCutoff;
                            c.InputVersion = cfd.Current;
                            cfd.Current.Ref();
                            snapshots = SnapshotsForCompaction();
                            _manualCompactionsRunning++;
                            _backgroundJobs++;
                        }
                        break;
                    }
                    if (_shuttingDown) throw new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
                    wait = _bgSignal.Task;
                }
                if (synchronous) wait.Wait(cancellationToken);
                else await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            if (c is null) return baseLevel;
            if (outputLevel == CompactionPicker.CompactToBaseLevel) baseLevel = c.OutputLevel;
            if (synchronous)
            {
                RunCompaction(cfd, c, snapshots, isManual: true);
            }
            else
            {
                var run = new ManualRun(this, cfd, c, snapshots);
                await Task.Factory.StartNew(static s => ((ManualRun)s!).Run(), run, CancellationToken.None,
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).ConfigureAwait(false);
            }
            if (compactionEnd is null) return baseLevel;
            beginKey = compactionEnd;
        }
    }

    private sealed class ManualRun(DbImpl db, ColumnFamilyData cfd, Compaction c, ulong[] snapshots)
    {
        public void Run() => db.RunCompaction(cfd, c, snapshots, isManual: true);
    }

    /// <summary>Port of DBImpl::FindMinimumEmptyLevelFitting. REQUIRES: mutex.</summary>
    private static int FindMinimumEmptyLevelFitting(VersionStorageInfo vs, int level)
    {
        int minimumLevel = level;
        for (int i = level - 1; i > 0; --i)
        {
            // stop if level i is not empty
            if (vs.NumLevelFiles(i) > 0) break;
            // stop if level i is too small (cannot fit the level files)
            if (vs.MaxBytesForLevel(i) < vs.NumLevelBytes(level)) break;
            minimumLevel = i;
        }
        return minimumLevel;
    }

    /// <summary>
    /// Port of DBImpl::ReFitLevel: moves all files of <paramref name="level"/> to <paramref name="targetLevel"/> (-1 =
    /// the minimum empty level that fits) with a MANIFEST-only edit, after waiting for running compactions.
    /// </summary>
    private async ValueTask ReFitLevelAsync(ColumnFamilyData cfd, int level, int targetLevel, bool alreadyExclusive, bool synchronous,
        CancellationToken cancellationToken)
    {
        if (targetLevel >= cfd.NumLevels) ThrowHelper.InvalidArgument("Target level exceeds number of levels");
        if (!alreadyExclusive) await BeginExclusiveManualAsync(synchronous, cancellationToken).ConfigureAwait(false);
        try
        {
            // Wait until no other compaction runs (the files must not be in a compaction).
            while (true)
            {
                Task wait;
                lock (_mutex)
                {
                    if (_bgCompactionScheduled == 0 && _manualCompactionsRunning == 0) break;
                    wait = _bgSignal.Task;
                }
                if (synchronous) wait.Wait(cancellationToken);
                else await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            var edit = new VersionEdit { ColumnFamily = cfd.Id };
            lock (_mutex)
            {
                VersionStorageInfo vs = cfd.Current.Storage;
                int toLevel = targetLevel < 0 ? FindMinimumEmptyLevelFitting(vs, level) : targetLevel;
                if (toLevel == level || vs.NumLevelFiles(level) == 0) return;
                if (toLevel > level)
                {
                    if (level == 0) ThrowHelper.NotSupported("Cannot change from level 0 to other levels.");
                    for (int l = level + 1; l <= toLevel; l++)
                    {
                        if (vs.NumLevelFiles(l) > 0) ThrowHelper.NotSupported("Levels between source and target are not empty for a move.");
                    }
                }
                else
                {
                    for (int l = toLevel; l < level; l++)
                    {
                        if (vs.NumLevelFiles(l) > 0) ThrowHelper.NotSupported("Levels between source and target are not empty for a move.");
                    }
                }
                foreach (FileMetaData f in vs.LevelFiles(level))
                {
                    edit.DeleteFile(level, f.FileNumber);
                    edit.AddFile(toLevel, f.Clone());
                }
            }
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, cancellationToken).ConfigureAwait(false);
            lock (_mutex) InstallSuperVersion(cfd);
        }
        finally
        {
            if (!alreadyExclusive) EndExclusiveManual();
        }
    }

    /// <summary>Waits until no compaction is running or pending (test/diagnostic helper, like TEST_WaitForCompact).</summary>
    internal async ValueTask WaitForCompactAsync(bool synchronous, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task wait;
            lock (_mutex)
            {
                if (IsBgWorkStopped && ErrorHandler.BgError is { } e) ThrowBackgroundError(e);
                bool pending = false;
                if (_compactionsEnabled && !_shuttingDown)
                {
                    foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
                    {
                        if (!cfd.Options.DisableAutoCompactions && cfd.CompactionState.Picker.NeedsCompaction(cfd.Current.Storage)) pending = true;
                    }
                }
                if (_bgCompactionScheduled == 0 && _manualCompactionsRunning == 0 && _runningFlushes == 0 && _flushQueue.Count == 0 && !pending) return;
                if (pending) MaybeScheduleCompaction();
                if (_bgCompactionScheduled == 0 && _manualCompactionsRunning == 0 && _runningFlushes == 0 && _flushQueue.Count == 0)
                {
                    // Pending work that cannot be scheduled (e.g. picker found nothing to do).
                    return;
                }
                if (_shuttingDown) throw new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
                wait = _bgSignal.Task;
            }
            if (synchronous) wait.Wait(cancellationToken);
            else await wait.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // properties

    /// <summary>Compaction-related DB properties (db/internal_stats.cc handlers). Returns false if unknown.</summary>
    private bool TryGetCompactionProperty(ColumnFamilyData cfd, SuperVersion sv, string property, out string? value)
    {
        switch (property)
        {
            case DbProperties.CompactionPending:
                lock (_mutex)
                {
                    // 1 if at least one compaction is pending (HandleCompactionPending).
                    bool pending = !cfd.Dropped && !cfd.Options.DisableAutoCompactions && cfd.CompactionState.Picker.NeedsCompaction(cfd.Current.Storage);
                    value = pending ? "1" : "0";
                }
                return true;
            case DbProperties.NumRunningCompactions:
                value = NumRunningCompactions.ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.EstimatePendingCompactionBytes:
                lock (_mutex) value = sv.Current.Storage.EstimatedCompactionNeededBytes.ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.LevelStats:
                lock (_mutex) value = sv.Current.Storage.LevelStats();
                return true;
            case DbProperties.BaseLevel:
                value = sv.Current.Storage.BaseLevel.ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.ActualDelayedWriteRate:
                value = (WriteControllerInstance.NeedsDelay ? WriteControllerInstance.DelayedWriteRate : 0).ToString(CultureInfo.InvariantCulture);
                return true;
            case DbProperties.IsWriteStopped:
                value = WriteControllerInstance.IsStopped ? "1" : "0";
                return true;
            case DbProperties.Stats:
            case DbProperties.CfStats:
                value = CompactionStatsString(sv);
                return true;
        }
        value = null;
        return false;
    }

    /// <summary>A subset of the "rocksdb.stats" text: level summary plus the cumulative compaction line.</summary>
    private string CompactionStatsString(SuperVersion sv)
    {
        var sb = new StringBuilder();
        lock (_mutex) sb.Append(sv.Current.Storage.LevelStats());
        double flushGb = Interlocked.Read(ref _statFlushBytesWritten) / 1073741824.0;
        double writeGb = (Interlocked.Read(ref _statCompactionBytesWritten) + Interlocked.Read(ref _statFlushBytesWritten)) / 1073741824.0;
        double readGb = Interlocked.Read(ref _statCompactionBytesRead) / 1073741824.0;
        double seconds = Interlocked.Read(ref _statCompactionMicros) / 1e6;
        sb.Append(string.Format(CultureInfo.InvariantCulture, "Flush(GB): cumulative {0:F3}, interval {0:F3}\n", flushGb));
        sb.Append(string.Format(CultureInfo.InvariantCulture,
            "Cumulative compaction: {0:F2} GB write, {1:F2} MB/s write, {2:F2} GB read, {3:F2} MB/s read, {4:F1} seconds\n",
            writeGb, seconds > 0 ? writeGb * 1024 / seconds : 0, readGb, seconds > 0 ? readGb * 1024 / seconds : 0, seconds));
        sb.Append(string.Format(CultureInfo.InvariantCulture, "Compactions: {0} (trivial moves {1}); compaction bytes written {2}, read {3}; flush bytes written {4}\n",
            Interlocked.Read(ref _statCompactions), Interlocked.Read(ref _statTrivialMoves), Interlocked.Read(ref _statCompactionBytesWritten),
            Interlocked.Read(ref _statCompactionBytesRead), Interlocked.Read(ref _statFlushBytesWritten)));
        return sb.ToString();
    }
}
