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
    // bg_bottom_compaction_scheduled_: compactions forwarded to the bottom-priority pool, queued or running (mutex).
    private int _bgBottomCompactionScheduled;
    private int _bgBottomCompactionRunning;  // bottom-priority threads busy (mutex)
    private readonly Queue<BottomPriorityCompaction> _bottomPriorityQueue = new(); // mutex
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
            lock (_mutex) return _bgCompactionScheduled + _bgBottomCompactionScheduled + _manualCompactionsRunning;
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
        RescheduleCompactionTrigger();
    }

    // ---------------------------------------------------------------------------------------------------------
    // periodic compaction trigger (PeriodicTaskType::kTriggerCompaction)

    private Timer? _compactionTriggerTimer;
    private ulong _compactionTriggerPeriod;

    /// <summary>
    /// Port of GetMinTimeBasedCompactionInterval (db/db_impl/db_impl.cc): the smallest time-based compaction interval of
    /// a column family (periodic_compaction_seconds, ttl, the first FIFO temperature age), 0 if none.
    /// (bottommost_file_compaction_delay is not an option of the managed port.)
    /// </summary>
    internal static ulong GetMinTimeBasedCompactionInterval(ColumnFamilyOptions o)
    {
        ulong minInterval = ulong.MaxValue;
        if (o.PeriodicCompactionSeconds > 0) minInterval = Math.Min(minInterval, o.PeriodicCompactionSeconds);
        if (o.Ttl > 0) minInterval = Math.Min(minInterval, o.Ttl);
        List<FileTemperatureAge> fifoThresholds = o.CompactionOptionsFifo.FileTemperatureAgeThresholds;
        // Thresholds are in increasing order by age, so first is smallest
        if (fifoThresholds.Count > 0 && fifoThresholds[0].Age > 0) minInterval = Math.Min(minInterval, fifoThresholds[0].Age);
        // Sentinel values like UINT64_MAX - 1 (ttl, periodic_compaction_seconds) are like disabling.
        return minInterval > ulong.MaxValue / 2 ? 0 : minInterval;
    }

    /// <summary>Port of DBImpl::ComputeTriggerCompactionPeriod.</summary>
    internal ulong ComputeTriggerCompactionPeriod()
    {
        // Start with the configured maximum, then reduce based on other options.
        ulong periodSec = Options.MaxCompactionTriggerWakeupSeconds;
        // Consider DB-level options that have the DB waking up periodically anyway.
        if (Options.StatsDumpPeriodSec > 0) periodSec = Math.Min(periodSec, Options.StatsDumpPeriodSec);
        if (Options.StatsPersistPeriodSec > 0) periodSec = Math.Min(periodSec, Options.StatsPersistPeriodSec);
        // Consider per-CF settings that can trigger compaction based on time.
        ulong compactionTriggerSec = ulong.MaxValue;
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            if (cfd.Dropped) continue;
            ulong cfMin = GetMinTimeBasedCompactionInterval(cfd.Options);
            if (cfMin > 0) compactionTriggerSec = Math.Min(compactionTriggerSec, cfMin);
        }
        // We might not align with those timings perfectly, so we tolerate only some proportional delay, up to
        // 1/kTriggerDivisor ~= 20%.
        const ulong TriggerDivisor = 5;
        periodSec = Math.Min(periodSec, compactionTriggerSec / TriggerDivisor);
        // But must be > 0
        return Math.Max(periodSec, 1UL);
    }

    /// <summary>
    /// (Re)arms the periodic compaction trigger with <see cref="ComputeTriggerCompactionPeriod"/> (DBImpl::
    /// StartPeriodicTaskScheduler / the period updates of SetOptions, SetDBOptions and column family changes).
    /// </summary>
    private void RescheduleCompactionTrigger()
    {
        if (_readOnly) return;
        ulong period = ComputeTriggerCompactionPeriod();
        lock (_mutex)
        {
            if (_shuttingDown || !_compactionsEnabled) return;
            if (_compactionTriggerTimer is not null && period == _compactionTriggerPeriod) return;
            _compactionTriggerPeriod = period;
            // Timer periods are limited to 0xfffffffe ms.
            TimeSpan due = TimeSpan.FromMilliseconds(Math.Min((double)period * 1000, 0xfffffffe));
            if (_compactionTriggerTimer is null)
            {
                _compactionTriggerTimer = new Timer(static s => ((DbImpl)s!).TriggerPeriodicCompaction(), this, due, due);
            }
            else
            {
                _compactionTriggerTimer.Change(due, due);
            }
        }
    }

    private void StopCompactionTrigger()
    {
        Timer? t;
        lock (_mutex)
        {
            t = _compactionTriggerTimer;
            _compactionTriggerTimer = null;
        }
        t?.Dispose();
    }

    /// <summary>
    /// Port of DBImpl::TriggerPeriodicCompaction: re-evaluates the compaction score (expired TTL files, periodic
    /// compaction, FIFO temperature, read-triggered files) of every column family with a time-based or read-triggered
    /// compaction setting that is not already queued, and schedules the compactions that are now needed.
    /// </summary>
    internal void TriggerPeriodicCompaction()
    {
        if (IsDisposed) return;
        TestHooks.Invoke("DBImpl::TriggerPeriodicCompaction:StartRunning");
        lock (_mutex)
        {
            if (_shuttingDown) return;
            foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
            {
                if (cfd.Dropped || cfd.CompactionState.QueuedForCompaction || cfd.Current is null) continue;
                if (GetMinTimeBasedCompactionInterval(cfd.Options) > 0 || cfd.Options.ReadTriggeredCompactionThreshold > 0.0)
                {
                    cfd.Current.Storage.ComputeCompactionScore();
                    SchedulePendingCompaction(cfd);
                }
            }
            MaybeScheduleCompaction();
            SignalBackgroundWork();
        }
    }

    /// <summary>Asks running compactions to stop (shutdown).</summary>
    private void CancelBackgroundCompactions()
    {
        StopCompactionTrigger();
        StopRecordSeqnoTimeWorker();
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
    private async ValueTask DelayWriteAsync(bool noSlowdown, bool synchronous, CancellationToken cancellationToken, bool countGroupBytes)
    {
        WriteController wc = WriteControllerInstance;
        ulong delayMicros = 0;
        lock (_mutex)
        {
            if (IsDbStopped && ErrorHandler.BgError is { } e) ThrowBackgroundError(e);
            if (!wc.IsStopped && wc.NeedsDelay)
            {
                // The main queue delays by its group's size; the WAL-only queue with num_bytes = 0 (WriteImplWALOnly).
                ulong bytes = 0;
                if (countGroupBytes)
                {
                    for (int i = 0; i < _group.Count; i++) bytes += (ulong)(_group[i].Batch?.DataSize ?? 0);
                }
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
            if (ShouldForwardToBottomPriorityPool(cfd, c))
            {
                ForwardToBottomPriorityPool(cfd, c, snapshots);
                SchedulePendingCompaction(cfd);
                continue;
            }
            c.InputVersion = cfd.Current;
            cfd.Current.Ref();
            _bgCompactionScheduled++;
            _backgroundJobs++;
            // There may be more work for this column family (another level, another key range).
            SchedulePendingCompaction(cfd);
            var job = new CompactionRun(this, cfd, c, snapshots, CompactionThread.Low);
            _ = Task.Factory.StartNew(static s => ((CompactionRun)s!).RunBackground(), job, CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Port of Version::OverlapWithLevelIterator + OverlapWithIterator (db/version_set.cc): does <paramref name="level"/>
    /// hold a key (or a range tombstone) in the user key range [<paramref name="smallestUserKey"/>,
    /// <paramref name="largestUserKey"/>]? L0: every file whose range overlaps is searched; sorted levels: the first key
    /// at or after the start decides, like a LevelIterator seek (empty files are skipped). Tombstones of the visited
    /// files are checked afterwards (RangeDelAggregator::IsRangeOverlapped). Returns false when a file cannot be read,
    /// so the caller falls back to file metadata.
    /// </summary>
    private static bool TryOverlapWithLevelIterator(ColumnFamilyData cfd, VersionStorageInfo vs, byte[] smallestUserKey, byte[] largestUserKey,
        int level, out bool overlap)
    {
        overlap = false;
        Comparator ucmp = cfd.UserComparator;
        var rangeDel = new RangeDelAggregator(cfd.Icmp, DbFormat.MaxSequenceNumber);
        var rangeStart = new byte[smallestUserKey.Length + DbFormat.InternalKeyFooterSize];
        DbFormat.AppendInternalKey(rangeStart, smallestUserKey, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        try
        {
            ReadOnlySpan<FileMetaData> files = vs.LevelFiles(level);
            int first = level == 0 ? 0 : vs.FindFile(level, rangeStart);
            for (int i = first; i < files.Length; i++)
            {
                FileMetaData f = files[i];
                // AfterFile / BeforeFile
                if (ucmp.CompareWithoutTimestamp(smallestUserKey, DbFormat.ExtractUserKey(f.Largest)) > 0
                    || ucmp.CompareWithoutTimestamp(largestUserKey, DbFormat.ExtractUserKey(f.Smallest)) < 0)
                {
                    if (level == 0) continue;
                    break; // sorted level: this and the following files start after the range
                }
                if (cfd.TableCache.GetRangeTombstones(f) is { } tombstones) rangeDel.AddTombstones(tombstones);
                using Iterators.InternalIterator it = cfd.TableCache.NewIterator(f);
                it.Seek(rangeStart);
                if (!it.Valid) continue; // empty past the start: the level iterator moves to the next file
                overlap = ucmp.CompareWithoutTimestamp(DbFormat.ExtractUserKey(it.Key), largestUserKey) <= 0;
                if (overlap || level != 0) break;
            }
        }
        catch (Exception e) when (e is RocksDbException or IOException)
        {
            return false;
        }
        if (!overlap && rangeDel.IsRangeOverlapped(smallestUserKey, largestUserKey, endExclusive: false)) overlap = true;
        return true;
    }

    /// <summary>The snapshot list with the job snapshot (taken last, so not smaller than any) added.</summary>
    private static ulong[] WithJobSnapshot(ulong[] snapshots, ulong jobSnapshot)
    {
        if (snapshots.Length > 0 && snapshots[^1] >= jobSnapshot) return snapshots;
        var a = new ulong[snapshots.Length + 1];
        snapshots.CopyTo(a, 0);
        a[^1] = jobSnapshot;
        return a;
    }

    /// <summary>Live snapshot sequence numbers, ascending. REQUIRES: mutex.</summary>
    private ulong[] SnapshotsForCompaction()
    {
        if (_snapshots.Count == 0) return [];
        var list = new List<ulong>();
        _snapshots.GetAll(list, ulong.MaxValue);
        return list.ToArray();
    }

    /// <summary>The thread (pool) a compaction runs on: its accounting and error handling differ.</summary>
    private enum CompactionThread : byte
    {
        /// <summary>Automatic compaction on the regular (Env::Priority::LOW) pool.</summary>
        Low,
        /// <summary>Automatic compaction on the bottom-priority pool (Env::Priority::BOTTOM).</summary>
        Bottom,
        /// <summary>Manual compaction (CompactRange / CompactFiles) on the caller's thread.</summary>
        Manual,
    }

    // ---------------------------------------------------------------------------------------------------------
    // bottom-priority pool (DBImpl::BackgroundCompaction forwarding, reduce_file_locking)

    /// <summary>A compaction forwarded to the bottom-priority pool (CompactionArg with a PrepickedCompaction).</summary>
    private sealed class BottomPriorityCompaction(ColumnFamilyData cfd, Compaction compaction, ulong[] snapshots, bool needRepick)
    {
        public ColumnFamilyData Cfd { get; } = cfd;
        /// <summary>The compaction to run, or with <see cref="NeedRepick"/> the intended compaction holding its files.</summary>
        public Compaction Compaction { get; } = compaction;
        public ulong[] Snapshots { get; } = snapshots;
        public bool NeedRepick { get; } = needRepick;
    }

    /// <summary>
    /// The forwarding condition of DBImpl::BackgroundCompaction: an automatic compaction that is neither a FIFO deletion
    /// nor a trivial move and outputs to the non-zero max output level goes to the bottom-priority pool when it has
    /// threads. REQUIRES: mutex.
    /// </summary>
    private bool ShouldForwardToBottomPriorityPool(ColumnFamilyData cfd, Compaction c) =>
        Options.BottomPriorityBackgroundThreads > 0 && !c.IsDeletionCompaction && !c.IsTrivialMove()
        && Compaction.OutputToNonZeroMaxOutputLevel(c.OutputLevel,
            cfd.Current.Storage.MaxOutputLevel(cfd.Options.CfAllowIngestBehind || Options.AllowIngestBehind));

    /// <summary>
    /// Forwards <paramref name="c"/> to the bottom-priority pool (DBImpl::BackgroundCompaction). If
    /// compaction_options_universal.reduce_file_locking is true, only a limited set of input files is locked by an
    /// intended compaction, and the files are repicked when a bottom-priority thread gets to execute it. REQUIRES: mutex.
    /// </summary>
    private void ForwardToBottomPriorityPool(ColumnFamilyData cfd, Compaction c, ulong[] snapshots)
    {
        TestHooks.Invoke("DBImpl::BackgroundCompaction:ForwardToBottomPriPool");
        bool needRepick = cfd.Options.CompactionOptionsUniversal.ReduceFileLocking;
        Compaction forwarded;
        if (needRepick)
        {
            forwarded = CreateIntendedCompactionForwardedToBottomPriorityPool(cfd, c);
        }
        else
        {
            forwarded = c;
            c.InputVersion = cfd.Current;
            cfd.Current.Ref();
        }
        _bottomPriorityQueue.Enqueue(new BottomPriorityCompaction(cfd, forwarded, snapshots, needRepick));
        _bgBottomCompactionScheduled++;
        _backgroundJobs++;
        MaybeStartBottomPriorityCompactions();
    }

    /// <summary>
    /// Port of DBImpl::CreateIntendedCompactionForwardedToBottomPriorityPool: releases <paramref name="c"/> and registers
    /// a compaction of only its last input level's files (the oldest of them when that level is L0, else the whole
    /// sorted run), which keeps those files reserved for the bottom thread. REQUIRES: mutex.
    /// </summary>
    private static Compaction CreateIntendedCompactionForwardedToBottomPriorityPool(ColumnFamilyData cfd, Compaction c)
    {
        VersionStorageInfo vstorage = c.InputStorage;
        CompactionInputFiles? maxInputLevelFiles = null;
        for (int i = c.NumInputLevels; i >= 1; --i)
        {
            if (c.Inputs(i - 1).Count > 0)
            {
                maxInputLevelFiles = c.Inputs(i - 1);
                break;
            }
        }
        var inputs = new CompactionInputFiles(maxInputLevelFiles!.Level);
        // The last input file (L0) or the last input sorted run.
        if (inputs.Level == 0) inputs.Files.Add(maxInputLevelFiles.Files[^1]);
        else inputs.Files.AddRange(maxInputLevelFiles.Files);
        CompactionPicker picker = cfd.CompactionState.Picker;
        picker.ReleaseCompactionFiles(c, succeeded: true);
        var intended = new Compaction(vstorage, c.Options, [inputs], c.OutputLevel, c.TargetOutputFileSize, c.MaxCompactionBytes, c.OutputPathId,
            c.OutputCompression, c.Grandparents, c.Reason, c.Score, l0FilesMightOverlap: true, isManual: false);
        picker.RegisterCompaction(intended);
        vstorage.ComputeCompactionScore();
        return intended;
    }

    /// <summary>Starts queued bottom-priority compactions while the pool has idle threads. REQUIRES: mutex.</summary>
    private void MaybeStartBottomPriorityCompactions()
    {
        while (_bgBottomCompactionRunning < Options.BottomPriorityBackgroundThreads && _bottomPriorityQueue.TryDequeue(out BottomPriorityCompaction? task))
        {
            _bgBottomCompactionRunning++;
            _ = Task.Factory.StartNew(static s => ((BottomPriorityRun)s!).Run(), new BottomPriorityRun(this, task), CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }
    }

    private sealed class BottomPriorityRun(DbImpl db, BottomPriorityCompaction task)
    {
        public void Run()
        {
            try
            {
                db.RunBottomPriorityCompaction(task);
            }
            catch (Exception)
            {
                // Recorded as background error (or ignored for shutdown / dropped column families) by RunCompaction.
            }
        }
    }

    /// <summary>
    /// A bottom-priority thread executing a forwarded compaction (BackgroundCallCompaction with a prepicked compaction):
    /// with need_repick the intended compaction is released (ResetBottomPriCompactionIntent) and a compaction to the
    /// max output level is picked afresh (require_max_output_level).
    /// </summary>
    private void RunBottomPriorityCompaction(BottomPriorityCompaction task)
    {
        TestHooks.Invoke("DBImpl::BGWorkBottomCompaction");
        ColumnFamilyData cfd = task.Cfd;
        Compaction? c = task.Compaction;
        ulong[] snapshots = task.Snapshots;
        lock (_mutex)
        {
            if (task.NeedRepick)
            {
                // ResetBottomPriCompactionIntent
                CompactionPicker picker = cfd.CompactionState.Picker;
                picker.ReleaseCompactionFiles(c, succeeded: true);
                c = null;
                if (!cfd.Dropped && cfd.Current is not null)
                {
                    cfd.Current.Storage.ComputeCompactionScore();
                    if (!cfd.Options.DisableAutoCompactions && !_shuttingDown && !IsBgWorkStopped && !_bgCancellation.IsCancellationRequested
                        && _exclusiveManualCompactions == 0)
                    {
                        TestHooks.Invoke("DBImpl::BackgroundCompaction():BeforePickCompaction");
                        snapshots = SnapshotsForCompaction();
                        c = picker.PickCompaction(cfd.Current.Storage, snapshots, requireMaxOutputLevel: true);
                        if (c is not null)
                        {
                            c.InputVersion = cfd.Current;
                            cfd.Current.Ref();
                        }
                    }
                }
                TestHooks.Invoke("DBImpl::BackgroundCompaction():AfterPickCompactionBottomPri");
                if (c is null)
                {
                    FinishBottomPriorityCompaction(cfd);
                    return;
                }
            }
        }
        RunCompaction(cfd, c, snapshots, CompactionThread.Bottom);
    }

    /// <summary>Accounts a finished bottom-priority job and schedules more work. REQUIRES: mutex.</summary>
    private void FinishBottomPriorityCompaction(ColumnFamilyData cfd)
    {
        _bgBottomCompactionScheduled--;
        _bgBottomCompactionRunning--;
        _backgroundJobs--;
        if (!cfd.Dropped && cfd.Current is not null)
        {
            RecalculateWriteStall(cfd);
            SchedulePendingCompaction(cfd);
        }
        MaybeStartBottomPriorityCompactions();
        MaybeScheduleCompaction();
        SignalBackgroundWork();
    }

    /// <summary>One compaction execution (BackgroundCompaction after picking).</summary>
    private sealed class CompactionRun
    {
        private readonly DbImpl _db;
        private readonly ColumnFamilyData _cfd;
        private readonly Compaction _c;
        private readonly ulong[] _snapshots;
        private readonly CompactionThread _thread;

        public CompactionRun(DbImpl db, ColumnFamilyData cfd, Compaction c, ulong[] snapshots, CompactionThread thread)
        {
            _db = db;
            _cfd = cfd;
            _c = c;
            _snapshots = snapshots;
            _thread = thread;
        }

        public void RunBackground()
        {
            try
            {
                _db.RunCompaction(_cfd, _c, _snapshots, _thread);
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
    private void RunCompaction(ColumnFamilyData cfd, Compaction c, ulong[] snapshots, CompactionThread thread)
    {
        bool isManual = thread == CompactionThread.Manual;
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
            OnCompactionStarted(cfd, c, jobId);
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
                // InitSnapshotContext: with a snapshot checker the inputs may contain values not visible to a snapshot
                // taken after the job starts; take one so that it appears in the snapshot list.
                SnapshotChecker? checker = CurrentSnapshotChecker;
                Snapshot? jobSnapshot = checker is null ? null : GetSnapshotImpl();
                try
                {
                    ulong[] jobSnapshots = jobSnapshot is null ? snapshots : WithJobSnapshot(snapshots, jobSnapshot.SequenceNumber);
                    job = new CompactionJob(this, cfd, c, jobSnapshots, NewPendingOutputFileNumber, ct)
                    {
                        SnapshotChecker = checker,
                        JobSnapshot = jobSnapshot?.SequenceNumber ?? 0,
                    };
                    job.RunAsync(synchronous: true).GetAwaiter().GetResult();
                }
                finally
                {
                    jobSnapshot?.Dispose();
                }
                c.AddInputDeletions(edit);
                foreach (FileMetaData f in job.Outputs) edit.AddFile(c.OutputLevel, f);
                // Per-key placement: entries too new for the last level were written to the proximal level.
                foreach (FileMetaData f in job.ProximalOutputs) edit.AddFile(c.ProximalLevel, f);
                edit.BlobFileAdditions.AddRange(job.BlobFileAdditions);
                edit.BlobFileGarbages.AddRange(job.BlobFileGarbages);
                if (job.Outputs.Count + job.ProximalOutputs.Count > 0) FileSystem.SyncDirectory(DbPaths[(int)Math.Min(c.OutputPathId, (uint)(DbPaths.Count - 1))]);
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
                if (thread == CompactionThread.Bottom)
                {
                    FinishBottomPriorityCompaction(cfd);
                }
                else
                {
                    if (isManual) _manualCompactionsRunning--;
                    else _bgCompactionScheduled--;
                    _backgroundJobs--;
                    if (!cfd.Dropped) SchedulePendingCompaction(cfd);
                    MaybeStartBottomPriorityCompactions();
                    MaybeScheduleCompaction();
                    SignalBackgroundWork();
                }
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

    internal ulong NewPendingOutputFileNumber()
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

        // CompactRangeInternal: update full_history_ts_low if it's set.
        if (options.FullHistoryTsLow is { Length: > 0 } tsLow)
        {
            if (begin is not null || end is not null) ThrowHelper.InvalidArgument("Cannot specify compaction range with full_history_ts_low");
            await IncreaseFullHistoryTsLowAsync(cfd, tsLow.ToArray(), synchronous, cancellationToken).ConfigureAwait(false);
        }

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
                    // Might need to query the partitioner
                    SstPartitioner? partitioner = null;
                    if (cfd.Options.SstPartitionerFactory is { } partitionerFactory && begin is not null && end is not null)
                    {
                        // Small lies about compaction range (output level unknown).
                        partitioner = partitionerFactory.CreatePartitioner(new SstPartitionerContext(IsFullCompaction: false, IsManualCompaction: true,
                            OutputLevel: -1, begin, end));
                    }
                    for (int level = 0; level < vs.NumNonEmptyLevels; level++)
                    {
                        bool overlap = true;
                        // Whether to look at specific keys within files for overlap with the compaction range, other
                        // than the largest and smallest keys of the files known in the version metadata.
                        bool checkOverlapWithinFile = false;
                        if (begin is not null && end is not null)
                        {
                            // Typically checking overlap within files in this case (WART upstream: not in one-sided
                            // bound cases). With user-defined timestamps the managed port keeps the file-level check.
                            checkOverlapWithinFile = cfd.TimestampSize == 0;
                            // Especially if the partitioner is new, the manual compaction might be used to enforce the
                            // partitioning: checking overlap within files could miss files that straddle a partition
                            // boundary inside the range (file "001".."111", range ["011", "101"], boundary "100"), so
                            // file-level overlap is used whenever the range contains a boundary. Hypothetical trivial
                            // move query: `begin` and `end` are both INCLUSIVE bounds here.
                            if (partitioner is not null && !partitioner.CanDoTrivialMove(begin, end)) checkOverlapWithinFile = false;
                        }
                        if (checkOverlapWithinFile && !TryOverlapWithLevelIterator(cfd, vs, begin!, end!, level, out overlap))
                        {
                            checkOverlapWithinFile = false;
                        }
                        if (!checkOverlapWithinFile) overlap = vs.OverlapInLevel(level, begin, begin is not null, end, end is not null);
                        if (overlap)
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
                    if (_bgCompactionScheduled + _bgBottomCompactionScheduled == 0) return;
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
                RunCompaction(cfd, c, snapshots, CompactionThread.Manual);
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
        public void Run() => db.RunCompaction(cfd, c, snapshots, CompactionThread.Manual);
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
                    if (_bgCompactionScheduled + _bgBottomCompactionScheduled == 0 && _manualCompactionsRunning == 0) break;
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
                if (_bgCompactionScheduled + _bgBottomCompactionScheduled == 0 && _manualCompactionsRunning == 0 && _runningFlushes == 0 && _flushQueue.Count == 0 && !pending) return;
                if (pending) MaybeScheduleCompaction();
                if (_bgCompactionScheduled + _bgBottomCompactionScheduled == 0 && _manualCompactionsRunning == 0 && _runningFlushes == 0 && _flushQueue.Count == 0)
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
                    // 1 if the system already determines at least one compaction is needed (HandleCompactionPending:
                    // the picker's NeedsCompaction, also with disable_auto_compactions).
                    bool pending = !cfd.Dropped && cfd.CompactionState.Picker.NeedsCompaction(cfd.Current.Storage);
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
