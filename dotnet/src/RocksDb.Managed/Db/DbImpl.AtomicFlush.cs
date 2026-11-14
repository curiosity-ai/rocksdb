namespace RocksDb.Managed.Db;

/// <summary>
/// Atomic flush: the flushes with atomic_flush and the error recovery flush of every column family (ResumeImpl's
/// FlushAllColumnFamilies with force_atomic_flush). Port of DBImpl::AtomicFlushMemTablesToOutputFiles and the atomic part of
/// MaybeScheduleFlushOrCompaction / BackgroundFlush (db/db_impl/db_impl_compaction_flush.cc) and of
/// InstallMemtableAtomicFlushResults (db/memtable_list.cc).
/// <para>
/// Like upstream, several atomic flushes may run at once: each picks, under one mutex hold, the memtables of every column
/// family sealed up to the newest completed switch that no other flush picked (PickMemTable with max_memtable_id), writes
/// their L0 files, then waits (atomic_flush_install_cv_) until, for every column family it flushes, its oldest memtable
/// is the oldest one in the list, i.e. until every older atomic flush was installed. Then all its results are written to
/// the MANIFEST as one atomic group. A failure rolls back the memtables of every column family of the flush
/// (RollbackMemtableFlush without rollback_succeeding_memtables); a newer flush waiting for it gives up once the error
/// stopped background work (or the MANIFEST failed, or shutdown began) and is rolled back too.
/// </para>
/// <para><b>Synchronization</b>: the fields are guarded by the DB mutex. The install wait is on <see cref="_bgSignal"/>,
/// which every finishing flush job signals (BackgroundCallFlush: atomic_flush_install_cv_.SignalAll()), as do shutdown
/// and the background error paths.</para>
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>Atomic flushes running (scheduled or the recovery flush). Guarded by the mutex.</summary>
    private int _runningAtomicFlushes;

    /// <summary>
    /// Scheduled atomic flushes that have not picked their memtables yet (unscheduled_flushes_ for atomic requests: such a
    /// job picks every memtable sealed by then, so no second one is scheduled before it picked). Guarded by the mutex.
    /// </summary>
    private int _unpickedAtomicFlushes;

    /// <summary>One column family of an atomic flush: the memtables it picked and the flush job's result.</summary>
    private sealed class AtomicFlushMember(ColumnFamilyData cfd, List<ImmutableMemTable> picked, ulong fileNumber, ulong epoch)
    {
        public ColumnFamilyData Cfd { get; } = cfd;
        /// <summary>The picked memtables, oldest first (FlushJob::mems_).</summary>
        public List<ImmutableMemTable> Picked { get; } = picked;
        public ulong FileNumber { get; } = fileNumber;
        public ulong Epoch { get; } = epoch;
        /// <summary>Identifies the memtables this member picked (<see cref="ImmutableMemTable.FlushOwner"/>).</summary>
        public FlushJobState Job { get; } = new();
        /// <summary>exec_status_[i].first: the flush job ran.</summary>
        public bool Executed { get; set; }
        /// <summary>exec_status_[i].second when not OK.</summary>
        public Exception? Failure { get; set; }
        /// <summary>The flush job's result when it succeeded.</summary>
        public PendingFlushResult? Result { get; set; }
        /// <summary>The result was part of the MANIFEST write (it may be durable even if the write failed).</summary>
        public bool InManifestWrite { get; set; }
        /// <summary>The result was installed: its memtables left the list.</summary>
        public bool Installed { get; set; }
    }

    /// <summary>True if an atomic flush is running or could be started (sealed memtables of a completed switch). REQUIRES: mutex.</summary>
    private bool AtomicFlushPending => Options.AtomicFlush && (_runningAtomicFlushes > 0 || HasPendingAtomicFlush());

    /// <summary>Some column family has a memtable sealed by a completed switch that no flush picked. REQUIRES: mutex.</summary>
    private bool HasPendingAtomicFlush()
    {
        ulong cut = _atomicCutCompleted;
        foreach (ColumnFamilyData c in Versions.ColumnFamilies)
        {
            if (!c.Dropped && c.HasPendingFlushUpTo(cut)) return true;
        }
        return false;
    }

    /// <summary>
    /// atomic_flush part of MaybeScheduleFlushOrCompaction: schedules an atomic flush job (BGWorkFlush) while one is needed
    /// and a flush slot (GetBGJobLimits().max_flushes) is free. Atomic flushes already running do not hold it back: the
    /// new job picks only memtables nobody picked and is installed after them. REQUIRES: mutex.
    /// </summary>
    private void MaybeScheduleAtomicFlush()
    {
        if (_atomicFlushBackoff) return; // a failed atomic flush is retried after the back-off or at the next request
        if (_unpickedAtomicFlushes > 0 || _runningFlushes >= _maxBackgroundFlushes) return;
        if (!HasPendingAtomicFlush()) return;
        _unpickedAtomicFlushes++;
        _runningAtomicFlushes++;
        _runningFlushes++;
        _backgroundJobs++;
        BackgroundThreadPool.High.Schedule(new AtomicFlushRun(this));
    }

    /// <summary>A scheduled atomic flush (BGWorkFlush -> BackgroundCallFlush) on the HIGH pool.</summary>
    private sealed class AtomicFlushRun(DbImpl db) : IThreadPoolWorkItem
    {
        public void Execute() => db.AtomicFlushMemTablesToOutputFilesAsync(synchronous: true, forRecovery: false).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Port of DBImpl::AtomicFlushMemTablesToOutputFiles (with the accounting of BackgroundCallFlush): picks the memtables
    /// of every column family up to the newest completed switch, writes one L0 file per column family, waits until every
    /// older atomic flush of those column families was installed, and commits all results as one MANIFEST atomic group
    /// (<see cref="InstallMemtableAtomicFlushResultsAsync"/>). <paramref name="forRecovery"/> is the error recovery flush
    /// of ResumeImpl, which runs on the caller's task (the background accounting is taken here). Returns the flush's
    /// status: null when it installed its results or had nothing to flush.
    /// </summary>
    private async Task<RocksDbException?> AtomicFlushMemTablesToOutputFilesAsync(bool synchronous, bool forRecovery)
    {
        bool unpicked = !forRecovery;
        bool counted = !forRecovery;
        List<AtomicFlushMember>? members = null;
        Exception? s = null;
        bool skipSetBgError = false;
        bool waitFailed = false;
        try
        {
            if (forRecovery)
            {
                // ResumeImpl runs this after WaitForBackgroundWork. An atomic flush started since (background work that the
                // error did not stop) is waited for before picking: its memtables are older than the ones picked here, and
                // a rolled back one would leave this flush waiting for its install forever.
                while (true)
                {
                    Task signal;
                    lock (_mutex)
                    {
                        if (_runningAtomicFlushes == 0)
                        {
                            _runningAtomicFlushes++;
                            _runningFlushes++;
                            _backgroundJobs++;
                            counted = true;
                            break;
                        }
                        signal = _bgSignal.Task;
                    }
                    if (synchronous) signal.Wait();
                    else await signal.ConfigureAwait(false);
                }
            }
            // SyncClosedWals + ApplyWALToManifest.
            try
            {
                await TrackClosedWalsAsync(synchronous).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                s = e;
            }
            lock (_mutex)
            {
                if (unpicked)
                {
                    _unpickedAtomicFlushes--;
                    unpicked = false;
                }
                if (s is null && !forRecovery && IsBgWorkStopped && ErrorHandler.BgError is { } bgError)
                {
                    // "Skip flush due to background error": nothing is picked.
                    s = bgError;
                    skipSetBgError = true;
                }
                if (s is null) members = PickAtomicFlushMemTables();
                // The next request may be scheduled now: it picks only what this one did not.
                MaybeScheduleFlush();
            }
            if (members is not null && members.Count > 0)
            {
                TestHooks.Invoke("DBImpl::AtomicFlushMemTablesToOutputFiles:Picked");
                // "TODO (yanqin): parallelize jobs with threads": the jobs of members 1..n-1 run first, then member 0's.
                for (int i = 1; i < members.Count; i++) await RunAtomicFlushMemberAsync(members[i], synchronous).ConfigureAwait(false);
                if (members.Count > 1)
                {
                    TestHooks.Invoke("DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1");
                    TestHooks.Invoke("DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2");
                }
                await RunAtomicFlushMemberAsync(members[0], synchronous).ConfigureAwait(false);
                // A failure of a dropped column family's job does not fail the flush (the column family is skipped).
                foreach (AtomicFlushMember m in members)
                {
                    if (m.Failure is not null && !m.Cfd.Dropped && !IsColumnFamilyDropped(m.Failure)) s ??= m.Failure;
                }
                if (s is null)
                {
                    s = await WaitToInstallAtomicFlushAsync(members, forRecovery, synchronous).ConfigureAwait(false);
                    waitFailed = s is not null;
                }
                if (s is null) s = await InstallMemtableAtomicFlushResultsAsync(members, synchronous).ConfigureAwait(false);
            }
        }
        finally
        {
            List<AtomicFlushMember>? toDelete = null;
            bool backoff = false;
            lock (_mutex)
            {
                if (unpicked) _unpickedAtomicFlushes--;
                if (members is not null)
                {
                    foreach (AtomicFlushMember m in members)
                    {
                        // "Since we are not installing these memtables, need to rollback to allow future flush job to pick
                        // up these memtables" (RollbackMemtableFlush, rollback_succeeding_memtables = false). A failed
                        // MANIFEST write already restored them (InstallMemtableAtomicFlushResults).
                        if (!m.Installed)
                        {
                            RollbackMemtableFlush(m.Cfd, m.Picked, m.Job, rollbackSucceedingMemtables: false);
                            (toDelete ??= new()).Add(m);
                        }
                        ReleasePickedMemTables(m.Cfd, m.Picked);
                        _pendingOutputs.Remove(m.FileNumber);
                    }
                }
                if (counted)
                {
                    _runningFlushes--;
                    _runningAtomicFlushes--;
                }
                // BackgroundCallFlush: "!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped() && reason !=
                // FlushReason::kErrorRecovery" waits before the next flush. When the error stopped background work nothing
                // is scheduled until the recovery.
                backoff = s is not null && !forRecovery && !_shuttingDown && !IsBgWorkStopped && members is { Count: > 0 }
                    && members[0].Picked[^1].FlushReason is not (FlushReason.ErrorRecovery or FlushReason.ErrorRecoveryRetryFlush);
                if (backoff)
                {
                    _atomicFlushBackoff = true;
                    ScheduleFlushRetryAfterBackoff(null);
                }
            }
            TestHooks.Invoke("DBImpl::BackgroundCallFlush:FlushFinish:0");
            if (toDelete is not null) DiscardAtomicFlushOutputs(toDelete);
            // Need to undo atomic flush if something went wrong: the error is recorded (a failure found while waiting to
            // install is the background error, the MANIFEST error or shutdown, already recorded).
            if (s is not null && !skipSetBgError && !waitFailed && !IsColumnFamilyDropped(s) && s is not ObjectDisposedException)
            {
                SetBackgroundError(s, Versions.IoStatus is not null ? BackgroundErrorReason.ManifestWriteNoWal : BackgroundErrorReason.FlushNoWal,
                    walRelated: false);
            }
            if (members is not null)
            {
                // Release the blob builders of the results (committed ones keep their files).
                foreach (AtomicFlushMember m in members)
                {
                    if (m.Result?.BlobBuilder is { } b)
                    {
                        ReleaseBlobOutputs(b);
                        b.Dispose();
                    }
                }
            }
            try
            {
                PurgeObsoleteFiles();
            }
            catch (Exception e) when (e is IOException or RocksDbException)
            {
                // Deleting obsolete files is best effort.
            }
            lock (_mutex)
            {
                // BackgroundCallFlush: MaybeScheduleFlushOrCompaction; atomic_flush_install_cv_.SignalAll(); bg_cv_.SignalAll().
                MaybeScheduleFlush();
                if (counted) _backgroundJobs--;
                SignalBackgroundWork();
            }
        }
        return s is null || IsColumnFamilyDropped(s) ? null : RocksDbException.FromException(s);
    }

    private static bool IsColumnFamilyDropped(Exception e) => e is RocksDbException { Code: StatusCode.ColumnFamilyDropped };

    /// <summary>
    /// FlushJob::PickMemTable for every column family (MemTableList::PickMemtablesToFlush with the max memtable id of the
    /// newest completed switch), in one mutex hold so that two atomic flushes never interleave their picks: an older flush
    /// that picked in one column family has picked in all of them. Column families with nothing to pick are not part of
    /// the flush (BackgroundFlush: "!cfd->imm()->IsFlushPending()"). REQUIRES: mutex.
    /// </summary>
    private List<AtomicFlushMember> PickAtomicFlushMemTables()
    {
        var members = new List<AtomicFlushMember>();
        ulong cut = _atomicCutCompleted;
        foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
        {
            if (cfd.Dropped) continue;
            List<ImmutableMemTable>? picked = null;
            var job = new FlushJobState();
            foreach (ImmutableMemTable m in cfd.Imm)
            {
                if (m.FlushInProgress || m.FlushCompleted)
                {
                    // Only consecutive memtables, never across one another flush picked.
                    if (picked is not null) break;
                    continue;
                }
                if (m.AtomicCut > cut) break;
                (picked ??= new List<ImmutableMemTable>()).Add(m);
            }
            if (picked is null) continue;
            var member = new AtomicFlushMember(cfd, picked, Versions.NewFileNumber(), cfd.NewEpochNumber());
            foreach (ImmutableMemTable m in picked)
            {
                m.FlushInProgress = true;
                m.FlushOwner = member.Job;
                // The flush reads the memtable without the mutex: keep it alive even if the CF is dropped meanwhile.
                m.Mem.Ref();
            }
            // PickMemtablesToFlush: "The flush request is complete only after every unstarted immutable memtable has been
            // picked."
            bool anyNotStarted = false;
            foreach (ImmutableMemTable m in cfd.Imm)
            {
                if (!m.FlushInProgress && !m.FlushCompleted) anyNotStarted = true;
            }
            if (!anyNotStarted) cfd.FlushRequested = false;
            _pendingOutputs.Add(member.FileNumber);
            members.Add(member);
        }
        return members;
    }

    /// <summary>FlushJob::Run of one member (write_manifest = false): writes its L0 file; the status is kept on the member.</summary>
    private async ValueTask RunAtomicFlushMemberAsync(AtomicFlushMember m, bool synchronous)
    {
        try
        {
            if (m.Cfd.Dropped) ThrowDropped(m.Cfd);
            m.Result = await RunFlushJobAsync(m.Cfd, m.Picked, m.FileNumber, m.Epoch, synchronous).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            m.Failure = e;
        }
        finally
        {
            m.Executed = true;
            TestHooks.Invoke("DBImpl::FlushMemTableToOutputFile:Finish");
        }
    }

    /// <summary>
    /// The wait of AtomicFlushMemTablesToOutputFiles before the install (wait_to_install_func on atomic_flush_install_cv_):
    /// until, for every column family of the flush that was not dropped, its first picked memtable is the oldest in the
    /// list. Ends early when the MANIFEST failed (versions_->io_status()), shutdown began, or (not a recovery flush) a
    /// background error stopped background work; the recovery flush, and any flush while the DB is stopped, waits until the
    /// recovery fails. Afterwards a background error (or, when resuming, the recovery error) fails the flush. Returns the
    /// failure, or null when the results may be installed.
    /// </summary>
    private async ValueTask<Exception?> WaitToInstallAtomicFlushAsync(List<AtomicFlushMember> members, bool forRecovery, bool synchronous)
    {
        Exception? s = null;
        bool resuming;
        lock (_mutex) resuming = ErrorHandler.IsDbStopped || forRecovery;
        while (true)
        {
            Task signal;
            lock (_mutex)
            {
                // while (!resuming_from_bg_err || error_handler_.GetRecoveryError().ok())
                if (resuming && ErrorHandler.RecoveryError is not null) break;
                if (Versions.IoStatus is { } io)
                {
                    // "Something went wrong elsewhere, we cannot count on waiting for our turn to write/sync to MANIFEST".
                    s = io;
                    break;
                }
                if (_shuttingDown)
                {
                    s = new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
                    break;
                }
                bool ready = true;
                foreach (AtomicFlushMember m in members)
                {
                    // "If the column family is dropped, then do not wait"; else wait while an older memtable of the
                    // column family is still in the list (another flush must install it first).
                    if (m.Cfd.Dropped || m.Result is null) continue;
                    List<ImmutableMemTable> imm = m.Cfd.Imm;
                    if (imm.Count > 0 && !ReferenceEquals(imm[0], m.Picked[0]))
                    {
                        ready = false;
                        break;
                    }
                }
                if (ready) break;
                // "If bg work is stopped, recovery thread first calls WaitForBackgroundWork() before proceeding to flush for
                // recovery. This flush can block WaitForBackgroundWork() while waiting for recovery flush to install
                // result. To avoid this deadlock, we should abort here if there is background error."
                if (!forRecovery && IsBgWorkStopped && ErrorHandler.BgError is { } bgError)
                {
                    s = bgError;
                    break;
                }
                signal = _bgSignal.Task;
            }
            // "We are not the oldest immutable memtable".
            TestHooks.Invoke("DBImpl::AtomicFlushMemTablesToOutputFiles:WaitCV");
            if (synchronous) signal.Wait();
            else await signal.ConfigureAwait(false);
            lock (_mutex) resuming = ErrorHandler.IsDbStopped || forRecovery;
        }
        lock (_mutex)
        {
            // "If not resuming from bg err, then we determine future action based on whether we hit background error";
            // when resuming, the recovery error decides.
            if (!resuming) s ??= ErrorHandler.BgError;
            else s ??= ErrorHandler.RecoveryError;
        }
        return s;
    }

    /// <summary>
    /// Port of InstallMemtableAtomicFlushResults (and the install part of AtomicFlushMemTablesToOutputFiles): marks the
    /// picked memtables flushed, writes the edits of every column family not dropped as one MANIFEST atomic group (with the
    /// min-log-number / WAL deletion edit), and in the same critical section removes the memtables and installs the super
    /// versions. A failed write restores the memtables so they can be flushed again (flush_in_progress_ /
    /// flush_completed_ cleared). Returns the failure, or null.
    /// </summary>
    private async ValueTask<Exception?> InstallMemtableAtomicFlushResultsAsync(List<AtomicFlushMember> members, bool synchronous)
    {
        var edits = new List<ManifestEditEntry>(members.Count);
        var installing = new List<AtomicFlushMember>(members.Count);
        lock (_mutex)
        {
            foreach (AtomicFlushMember m in members)
            {
                if (m.Cfd.Dropped || m.Result is not { } r) continue;
                foreach (ImmutableMemTable mem in m.Picked)
                {
                    mem.FlushCompleted = true;
                    mem.FlushFileNumber = m.FileNumber;
                }
                // In 2PC mode the WALs holding outstanding prepared sections (or prepared sections of committed data still
                // in memtables) must survive (PrecomputeMinLogNumberToKeep2PC). Without 2PC CommitAtomicFlushAsync computes
                // the minimum over all members (PrecomputeMinLogNumberToKeepNon2PC).
                if (Options.Allow2Pc) r.Edit.MinLogNumberToKeep = PrecomputeMinLogNumberToKeep2PC(m.Cfd, r.Edit.LogNumber!.Value, m.Picked);
                // From here on a failure may still have made the edit durable: leave the blob files to the obsolete-file scan.
                if (r.BlobBuilder is not null) r.BlobBuilder.Committed = true;
                edits.Add(new ManifestEditEntry(m.Cfd, r.Edit));
                m.InManifestWrite = true;
                installing.Add(m);
            }
        }
        if (edits.Count == 0) return null;

        // Removes the flushed memtables and installs the super versions, in the critical section that installs the new
        // versions (InstallMemtableAtomicFlushResults removes them under the mutex LogAndApply re-acquired, and
        // AtomicFlushMemTablesToOutputFiles installs the super versions before releasing it). REQUIRES: mutex.
        void InstallResults()
        {
            foreach (AtomicFlushMember m in installing)
            {
                foreach (ImmutableMemTable mem in m.Picked)
                {
                    // A drop that committed right after our MANIFEST write already released the list's references.
                    if (m.Cfd.Imm.Remove(mem)) RetireFlushedMemTable(m.Cfd, mem.Mem);
                }
                m.Installed = true;
                if (!m.Cfd.Dropped) InstallSuperVersion(m.Cfd);
            }
            _maybeStalled = IsWriteStalled();
        }
        try
        {
            await CommitAtomicFlushAsync(edits, synchronous, InstallResults).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            lock (_mutex)
            {
                // "Level-0 commit table ... failed": the memtables can be flushed again.
                foreach (AtomicFlushMember m in installing)
                {
                    foreach (ImmutableMemTable mem in m.Picked) ResetFlushState(mem);
                }
            }
            return e;
        }
        foreach (AtomicFlushMember m in installing)
        {
            PendingFlushResult r = m.Result!;
            if (r.Meta is not null) MaybeReportMaxAllowedSpaceReached(BackgroundErrorReason.Flush);
            if (m.Cfd.Dropped) continue;
            if (r.BlobBuilder is not null && r.Meta is not null) LogBlobFileCreationEvents(m.Cfd, r.JobId, r.BlobBuilder.Additions);
            OnFlushCompleted(m.Cfd, r.JobId, r.Meta, BlobBytesWritten(r.BlobBuilder), r.Edit.ManifestBytesWritten, r.FlushStart);
            NotifyOnFlushCompleted(m.Cfd, r.JobId, r.FileNumber, r.Meta, r.Path, r.Mems, r.Reason);
        }
        return null;
    }

    /// <summary>
    /// Drops the outputs of members whose results were not installed: evicted from the table cache ("the output files are
    /// cached in the table cache ... but never installed into any Version. Evict them now"), and deleted unless they were
    /// part of a failed MANIFEST write (it may have made them durable; they are then left to the obsolete-file scan).
    /// </summary>
    private void DiscardAtomicFlushOutputs(List<AtomicFlushMember> members)
    {
        foreach (AtomicFlushMember m in members)
        {
            if (!m.Executed) continue;
            m.Cfd.TableCache.Evict(m.FileNumber);
            if (!m.InManifestWrite) TryDeleteFile(FileNames.TableFileName(DbPaths, m.FileNumber, 0));
        }
    }
}
