namespace RocksDb.Managed.Db;

/// <summary>
/// Background error handling and recovery. Port of rocksdb::ErrorHandler (db/error_handler.{h,cc}) and the listener
/// notifications of db/event_helpers.cc (NotifyOnBackgroundError / NotifyOnErrorRecoveryEnd).
/// <para>
/// A failed background operation (flush, compaction, WAL write, MANIFEST write, memtable switch) reports its error with
/// a <see cref="BackgroundErrorReason"/>. The error is classified into a severity (ErrorSeverityMap /
/// DefaultErrorSeverityMap / DefaultReasonMap, the retryable-IO rules of SetBGErrorImpl):
/// <list type="bullet">
/// <item><b>soft</b>: writes continue; with <c>soft_error_no_bg_work</c> (retryable flush / MANIFEST errors) background
/// flushes and compactions pause until recovery.</item>
/// <item><b>hard</b>: writes fail with the error until recovery (automatic for retryable errors, otherwise
/// <see cref="RocksDatabase.Resume"/>).</item>
/// <item><b>fatal / unrecoverable</b>: writes fail until the DB is reopened.</item>
/// </list>
/// Retryable compaction errors are not recorded (the compaction is retried). Automatic recovery
/// (RecoverFromRetryableBGIOError) runs on a separate task, up to <see cref="DbOptions.MaxBgErrorResumeCount"/> times,
/// <see cref="DbOptions.BgErrorResumeRetryInterval"/> apart. Recovery itself (DBImpl::ResumeImpl) is
/// <see cref="DbImpl.ResumeImplAsync"/>. Out-of-space errors (NoSpace / SpaceLimit) are recovered by the DB's
/// SstFileManager once enough space is free (RecoverFromNoSpace; every DB has one, see SanitizeOptions), or by
/// <see cref="RocksDatabase.Resume"/>. Not ported: file quarantine, AsyncFileOpen.
/// </para>
/// <para>
/// <b>Synchronization</b>: all state is guarded by the DB mutex, except <see cref="IsDbStopped"/> which is also
/// published through a volatile field for the lock-free write path check (is_db_stopped_). Listeners are called without
/// the mutex (upstream releases it around the callbacks).
/// </para>
/// </summary>
internal sealed class ErrorHandler
{
    private readonly DbImpl _db;
    private RocksDbException? _bgError;          // bg_error_ (severity in RocksDbException.Severity)
    private RocksDbException? _recoveryError;    // recovery_error_
    private bool _recoveryInProgress;            // recovery_in_prog_
    private bool _softErrorNoBgWork;             // soft_error_no_bg_work_
    private bool _autoRecovery;                  // auto_recovery_ (enabled once the DB is open)
    private bool _endRecovery;                   // end_recovery_
    private FlushReason _recoveryFlushReason = FlushReason.ErrorRecovery; // recover_context_.flush_reason
    private bool _walWriteError;                 // DBRecoverContext::IsWALWriteErrorRecovery (simplified)
    private volatile bool _isDbStopped;          // is_db_stopped_
    private Task _recoveryTask = Task.CompletedTask;
    private readonly CancellationTokenSource _recoveryCancellation = new();
    private long _errorCount;

    public ErrorHandler(DbImpl db) => _db = db;

    /// <summary>Writes are stopped (severity hard or above). Lock-free.</summary>
    public bool IsDbStopped => _isDbStopped;

    /// <summary>Background flushes and compactions are stopped (IsBGWorkStopped). REQUIRES: mutex.</summary>
    public bool IsBgWorkStopped =>
        _bgError is not null && (_bgError.Severity >= ErrorSeverity.HardError || !_autoRecovery || _softErrorNoBgWork);

    /// <summary>REQUIRES: mutex.</summary>
    public bool IsRecoveryInProgress => _recoveryInProgress;

    /// <summary>The current background error, or null. REQUIRES: mutex.</summary>
    public RocksDbException? BgError => _bgError;

    /// <summary>The current background error, taking the DB mutex (GetBGError).</summary>
    public RocksDbException? CurrentBgError
    {
        get
        {
            lock (_db.Mutex) return _bgError;
        }
    }

    /// <summary>Error recorded while a recovery was running (recovery_error_). REQUIRES: mutex.</summary>
    public RocksDbException? RecoveryError => _recoveryError;

    /// <summary>The recovery must replace a WAL whose write failed (its handle is not used again). REQUIRES: mutex.</summary>
    public bool IsWalWriteErrorRecovery => _walWriteError;

    /// <summary>Retry the pending flushes only, without switching memtables (kErrorRecoveryRetryFlush). REQUIRES: mutex.</summary>
    public bool RetryFlushOnly => _recoveryFlushReason == FlushReason.ErrorRecoveryRetryFlush;

    /// <summary>Number of background errors reported (ERROR_HANDLER_BG_ERROR_COUNT).</summary>
    public long ErrorCount => Interlocked.Read(ref _errorCount);

    /// <summary>EnableAutoRecovery (end of DB::Open). REQUIRES: mutex.</summary>
    public void EnableAutoRecovery() => _autoRecovery = true;

    // -----------------------------------------------------------------------------------------------------
    // Classification (ErrorSeverityMap, DefaultErrorSeverityMap, DefaultReasonMap in db/error_handler.cc)

    /// <summary>
    /// Port of the three severity maps of error_handler.cc, from most to least specific: (reason, code, subcode,
    /// paranoid), (reason, code, paranoid), (reason, paranoid); kFatalError when no entry matches.
    /// </summary>
    internal static ErrorSeverity Classify(BackgroundErrorReason reason, StatusCode code, StatusSubCode subCode, bool paranoid)
    {
        // ErrorSeverityMap
        if (code == StatusCode.IOError)
        {
            switch (subCode)
            {
                case StatusSubCode.IOFenced:
                    if (reason is BackgroundErrorReason.Compaction or BackgroundErrorReason.Flush or BackgroundErrorReason.WriteCallback
                        or BackgroundErrorReason.ManifestWrite or BackgroundErrorReason.FlushNoWal or BackgroundErrorReason.ManifestWriteNoWal)
                    {
                        return ErrorSeverity.FatalError;
                    }
                    break;
                case StatusSubCode.NoSpace:
                    switch (reason)
                    {
                        case BackgroundErrorReason.Compaction: return paranoid ? ErrorSeverity.SoftError : ErrorSeverity.NoError;
                        case BackgroundErrorReason.Flush:
                        case BackgroundErrorReason.FlushNoWal: return paranoid ? ErrorSeverity.HardError : ErrorSeverity.NoError;
                        case BackgroundErrorReason.WriteCallback:
                        case BackgroundErrorReason.ManifestWrite:
                        case BackgroundErrorReason.ManifestWriteNoWal: return ErrorSeverity.HardError;
                    }
                    break;
                case StatusSubCode.SpaceLimit:
                    if (paranoid && reason is BackgroundErrorReason.Compaction or BackgroundErrorReason.Flush or BackgroundErrorReason.FlushNoWal)
                    {
                        return ErrorSeverity.HardError;
                    }
                    break;
            }
        }
        // DefaultErrorSeverityMap
        switch (reason)
        {
            case BackgroundErrorReason.Compaction:
            case BackgroundErrorReason.Flush:
            case BackgroundErrorReason.WriteCallback:
            case BackgroundErrorReason.FlushNoWal:
                if (code == StatusCode.Corruption) return paranoid ? ErrorSeverity.UnrecoverableError : ErrorSeverity.NoError;
                if (code == StatusCode.IOError) return paranoid ? ErrorSeverity.FatalError : ErrorSeverity.NoError;
                break;
            case BackgroundErrorReason.ManifestWrite:
            case BackgroundErrorReason.ManifestWriteNoWal:
                if (code == StatusCode.IOError) return ErrorSeverity.FatalError;
                break;
            case BackgroundErrorReason.AsyncFileOpen:
                if (code == StatusCode.Corruption) return paranoid ? ErrorSeverity.UnrecoverableError : ErrorSeverity.NoError;
                if (code is StatusCode.Incomplete or StatusCode.NotSupported) return paranoid ? ErrorSeverity.FatalError : ErrorSeverity.NoError;
                break;
        }
        // DefaultReasonMap
        return reason switch
        {
            BackgroundErrorReason.Compaction or BackgroundErrorReason.Flush => paranoid ? ErrorSeverity.FatalError : ErrorSeverity.NoError,
            BackgroundErrorReason.WriteCallback or BackgroundErrorReason.MemTable => ErrorSeverity.FatalError,
            BackgroundErrorReason.AsyncFileOpen => paranoid ? ErrorSeverity.SoftError : ErrorSeverity.NoError,
            _ => ErrorSeverity.FatalError,
        };
    }

    // -----------------------------------------------------------------------------------------------------
    // SetBGError

    /// <summary>
    /// Port of ErrorHandler::SetBGError / SetBGErrorImpl + HandleKnownErrors. Called without the DB mutex. Returns the
    /// background error in effect afterwards (null if the error was ignored or suppressed).
    /// <paramref name="walRelated"/>: the failed operation wrote or synced the WAL (the recovery then replaces it).
    /// </summary>
    public RocksDbException? SetBgError(Exception exception, BackgroundErrorReason reason, bool walRelated)
    {
        RocksDbException err = RocksDbException.FromException(exception);
        if (err.Code == StatusCode.Ok) return null;
        Interlocked.Increment(ref _errorCount);
        if (_db.Stats is { } st)
        {
            st.RecordTick(Ticker.ErrorHandlerBgErrorCount);
            if (err.Code == StatusCode.IOError) st.RecordTick(Ticker.ErrorHandlerBgIoErrorCount);
        }
        _db.LogWarn("Background IO error " + err.Message + ", reason " + (int)reason);
        DbOptions o = _db.Options;

        // 1. Data loss (not file scoped): unrecoverable, overrides any existing error.
        if (err.Scope != IOErrorScope.File && err.DataLoss)
        {
            // SetBGErrorImpl sets the error BEFORE notifying the listeners (CheckAndSetRecoveryAndBGError, then
            // NotifyOnBackgroundError on a copy): unlike an error classified by HandleKnownErrors, a listener can neither
            // suppress nor replace it. The same holds for the two cases below.
            RocksDbException unrecoverable = err.WithSeverity(ErrorSeverity.UnrecoverableError);
            lock (_db.Mutex) CheckAndSetRecoveryAndBgError(unrecoverable, walRelated, FlushReason.ErrorRecovery);
            NotifyOnBackgroundError(reason, unrecoverable, autoRecovery: false, out _);
            lock (_db.Mutex) return _bgError;
        }
        // 2. With manual_wal_flush a WAL failure may drop buffered WAL writes: fatal until reopen.
        if (o.ManualWalFlush && walRelated && err.Code == StatusCode.IOError)
        {
            RocksDbException fatal = err.WithSeverity(ErrorSeverity.FatalError);
            lock (_db.Mutex) CheckAndSetRecoveryAndBgError(fatal, walRelated, FlushReason.ErrorRecovery);
            NotifyOnBackgroundError(reason, fatal, autoRecovery: false, out _);
            lock (_db.Mutex) return _bgError;
        }
        // 3. Retryable (or file scoped) IO errors, except out-of-space ones (left to the SstFileManager upstream).
        if (err.SubCode != StatusSubCode.NoSpace && (err.Scope == IOErrorScope.File || err.Retryable))
        {
            _db.Stats?.RecordTick(Ticker.ErrorHandlerBgRetryableIoErrorCount);
            _db.LogInfo("ErrorHandler: Set background retryable IO error");
            bool autoResumeEnabled = o.MaxBgErrorResumeCount > 0;
            if (reason == BackgroundErrorReason.Compaction && autoResumeEnabled)
            {
                // A compaction reschedules itself: the error is not recorded (soft, no state change).
                _db.Stats?.RecordTick(Ticker.ErrorHandlerAutoresumeCount);
                _db.LogInfo("ErrorHandler: Compaction will schedule by itself to resume");
                NotifyOnBackgroundError(reason, err.WithSeverity(ErrorSeverity.SoftError), autoRecovery: false, out _);
                lock (_db.Mutex) return _bgError;
            }
            ErrorSeverity severity;
            bool noBgWork = false;
            if (!autoResumeEnabled)
            {
                // Nothing retries on the user's behalf: stop writes until Resume().
                severity = ErrorSeverity.HardError;
            }
            else if (reason is BackgroundErrorReason.FlushNoWal or BackgroundErrorReason.ManifestWriteNoWal)
            {
                // Writes continue (the WAL is fine) but background work pauses until the flush is retried.
                severity = ErrorSeverity.SoftError;
                noBgWork = true;
            }
            else
            {
                severity = ErrorSeverity.HardError;
            }
            RocksDbException retryable = err.WithSeverity(severity);
            lock (_db.Mutex)
            {
                // soft_error_no_bg_work_ is set whatever the current error is; the recovery context (flush reason) is
                // only replaced when this error becomes the background error (UpdateRecoveryContext).
                if (noBgWork) _softErrorNoBgWork = true;
                CheckAndSetRecoveryAndBgError(retryable, walRelated, noBgWork ? FlushReason.ErrorRecoveryRetryFlush : FlushReason.ErrorRecovery);
            }
            // The error is already set: the listeners are only told about it (a suppressed or replaced error changes
            // nothing). Managed extension kept: OnErrorRecoveryBegin can still veto the automatic recovery (upstream
            // ignores the listeners' auto_recovery here and always calls StartRecoverFromRetryableBGIOError).
            NotifyOnBackgroundError(reason, retryable, autoResumeEnabled, out bool autoRecovery);
            RocksDbException? current;
            bool recoveryEndedByShutdown = false;
            lock (_db.Mutex)
            {
                if (autoRecovery) recoveryEndedByShutdown = StartRecoverFromRetryableBgIoError();
                current = _bgError;
            }
            // StartRecoverFromRetryableBGIOError with end_recovery_ set: the listeners are still told that the recovery
            // of this error ended with ShutdownInProgress (error_handler.cc).
            if (recoveryEndedByShutdown && current is not null) NotifyOnErrorRecoveryEnd(current, ShutdownStatus());
            return current;
        }
        return HandleKnownErrors(err, reason, walRelated);
    }

    /// <summary>Port of ErrorHandler::HandleKnownErrors (with OverrideNoSpaceError and RecoverFromNoSpace).</summary>
    private RocksDbException? HandleKnownErrors(RocksDbException err, BackgroundErrorReason reason, bool walRelated)
    {
        DbOptions o = _db.Options;
        ErrorSeverity sev = Classify(reason, err.Code, err.SubCode, o.ParanoidChecks);
        bool autoRecovery;
        lock (_db.Mutex) autoRecovery = _autoRecovery;
        if (sev >= ErrorSeverity.FatalError) autoRecovery = false;
        bool noSpace = err.SubCode is StatusSubCode.NoSpace or StatusSubCode.SpaceLimit;
        if (noSpace && sev < ErrorSeverity.FatalError)
        {
            // OverrideNoSpaceError: recovery from out-of-space errors relies on the SstFileManager polling for free space.
            if (o.SstFileManager is null)
            {
                autoRecovery = false;
            }
            else if (o.Allow2Pc && sev <= ErrorSeverity.SoftError)
            {
                // The contents of the current WAL may be inconsistent and needed for 2PC.
                autoRecovery = false;
                sev = ErrorSeverity.FatalError;
            }
            else
            {
                try { _ = _db.FileSystem.GetFreeSpace(_db.DbPaths[0]); }
                catch (RocksDbException e) when (e.Code == StatusCode.NotSupported) { autoRecovery = false; }
                catch (Exception e) when (e is IOException or RocksDbException) { }
            }
        }
        RocksDbException classified = err.WithSeverity(sev);
        lock (_db.Mutex)
        {
            // Recovery in progress: remember the error so the recovery knows it failed.
            if (_recoveryInProgress && _recoveryError is null) _recoveryError = classified;
        }
        if (sev == ErrorSeverity.NoError)
        {
            _db.LogInfo("ErrorHandler: Hit less severe background error");
            return null;
        }
        RocksDbException? s = NotifyOnBackgroundError(reason, classified, autoRecovery, out bool autoRecoveryAfterListeners);
        autoRecovery = autoRecoveryAfterListeners;
        lock (_db.Mutex)
        {
            if (s is not null && s.Severity > Severity(_bgError))
            {
                _bgError = s;
                _recoveryFlushReason = FlushReason.ErrorRecovery; // UpdateRecoveryContext(context, replace_existing_context = true)
                if (walRelated) _walWriteError = true;
            }
            else
            {
                if (s is not null && walRelated) _walWriteError = true;
                _db.LogInfo("ErrorHandler: Hit less severe background error");
                return _bgError;
            }
            if (_bgError.Severity >= ErrorSeverity.HardError) _isDbStopped = true;
            _db.LogInfo("ErrorHandler: Set regular background error, auto_recovery=" + (autoRecovery ? 1 : 0) + ", stop=" + (_isDbStopped ? 1 : 0));
            if (autoRecovery && noSpace && _db.Options.SstFileManager is { } sfm && !_endRecovery)
            {
                // RecoverFromNoSpace: the SstFileManager recovers once enough space is free.
                _recoveryInProgress = true;
                sfm.StartErrorRecovery(this, _bgError);
            }
            return _bgError;
        }
    }

    private static ErrorSeverity Severity(RocksDbException? e) => e?.Severity ?? ErrorSeverity.NoError;

    /// <summary>
    /// Port of CheckAndSetRecoveryAndBGError. The recovery context of <paramref name="err"/> (its flush reason) replaces
    /// the current one only when the error becomes the background error (UpdateRecoveryContext with
    /// replace_existing_context = severity above the current one). REQUIRES: mutex.
    /// </summary>
    private void CheckAndSetRecoveryAndBgError(RocksDbException err, bool walRelated, FlushReason flushReason)
    {
        if (_recoveryInProgress && _recoveryError is null) _recoveryError = err;
        if (err.Severity > Severity(_bgError))
        {
            _bgError = err;
            _recoveryFlushReason = flushReason;
        }
        if (walRelated) _walWriteError = true;
        if (Severity(_bgError) >= ErrorSeverity.HardError) _isDbStopped = true;
    }

    // -----------------------------------------------------------------------------------------------------
    // Recovery

    /// <summary>Port of ErrorHandler::ClearBGError. Returns the recovery error (null on success). REQUIRES: mutex.</summary>
    public RocksDbException? ClearBgError(out RocksDbException? clearedError)
    {
        clearedError = null;
        if (_recoveryError is null)
        {
            clearedError = _bgError;
            _isDbStopped = false;
            _bgError = null;
            _recoveryInProgress = false;
            _softErrorNoBgWork = false;
            _walWriteError = false;
            _recoveryFlushReason = FlushReason.ErrorRecovery;
        }
        return _recoveryError;
    }

    /// <summary>
    /// Port of ErrorHandler::RecoverFromBGError. Called without the mutex. Returns null on success, otherwise the
    /// reason the recovery failed.
    /// </summary>
    public async ValueTask<RocksDbException?> RecoverFromBgErrorAsync(bool isManual)
    {
        bool softNoBgWorkOnEntry;
        RocksDbException? cleared = null;
        bool clearedDirectly = false;
        lock (_db.Mutex)
        {
            softNoBgWorkOnEntry = _softErrorNoBgWork;
            if (isManual)
            {
                if (_recoveryInProgress) return new RocksDbException(StatusCode.Busy, "Resource busy: Recovery already in progress");
                _recoveryInProgress = true;
            }
            // A soft error that needs no flush is cleared directly.
            if (_bgError?.Severity == ErrorSeverity.SoftError && _recoveryFlushReason == FlushReason.ErrorRecovery && !softNoBgWorkOnEntry)
            {
                _recoveryError = null;
                ClearBgError(out cleared);
                clearedDirectly = true;
            }
            else
            {
                _recoveryError = null;
            }
        }
        if (clearedDirectly)
        {
            if (cleared is not null) NotifyOnErrorRecoveryEnd(cleared, null);
            return null;
        }
        RocksDbException? s;
        try
        {
            s = await _db.ResumeImplAsync().ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            // A failure thrown by the resume is its status: recovery_in_prog_ must not stay set (a later Resume would
            // be refused with Busy forever).
            s = RocksDbException.FromException(e);
        }
        lock (_db.Mutex)
        {
            if (s is not null) _softErrorNoBgWork = softNoBgWorkOnEntry;
            // Manual recovery, shutdown and fatal errors end the recovery; an automatic recovery keeps the flag (retried).
            if (isManual || s?.Code == StatusCode.ShutdownInProgress || Severity(_bgError) >= ErrorSeverity.FatalError)
            {
                _recoveryInProgress = false;
            }
        }
        return s;
    }

    /// <summary>
    /// Port of StartRecoverFromRetryableBGIOError. Returns true when no recovery is started because recovery was ended
    /// for shutdown (end_recovery_): the caller then reports OnErrorRecoveryEnd(bg_error, ShutdownInProgress) outside
    /// the mutex. REQUIRES: mutex.
    /// </summary>
    private bool StartRecoverFromRetryableBgIoError()
    {
        if (_bgError is null) return false;
        if (_db.Options.MaxBgErrorResumeCount <= 0 || _recoveryInProgress) return false;
        if (_endRecovery) return true;
        _db.Stats?.RecordTick(Ticker.ErrorHandlerAutoresumeCount);
        _db.LogInfo("ErrorHandler: Call StartRecoverFromRetryableBGIOError to resume");
        // Set in the same lock hold as the error, otherwise intervening writes could see an error without a recovery.
        _recoveryInProgress = true;
        Task previous = _recoveryTask;
        _recoveryTask = Task.Run(async () =>
        {
            try { await previous.ConfigureAwait(false); }
            catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException or OperationCanceledException) { }
            await RecoverFromRetryableBgIoErrorAsync().ConfigureAwait(false);
        });
        return false;
    }

    /// <summary>Port of ErrorHandler::RecoverFromRetryableBGIOError (the automatic recovery loop).</summary>
    private async Task RecoverFromRetryableBgIoErrorAsync()
    {
        DbOptions o = _db.Options;
        int resumeCount = o.MaxBgErrorResumeCount;
        TimeSpan interval = TimeSpan.FromMicroseconds(o.BgErrorResumeRetryInterval);
        ulong retryCount = 0;
        RocksDbException? oldError;
        while (resumeCount > 0)
        {
            lock (_db.Mutex)
            {
                oldError = _bgError;
                if (_endRecovery)
                {
                    _recoveryInProgress = false;
                }
            }
            if (_endRecovery)
            {
                if (oldError is not null) NotifyOnErrorRecoveryEnd(oldError, ShutdownStatus());
                return;
            }
            TestHooks.Invoke("RecoverFromRetryableBGIOError:BeforeResume");
            lock (_db.Mutex) _recoveryError = null;
            retryCount++;
            RocksDbException? s;
            try
            {
                s = await _db.ResumeImplAsync().ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                s = RocksDbException.FromException(e);
            }
            _db.Stats?.RecordTick(Ticker.ErrorHandlerAutoresumeRetryTotalCount);
            RocksDbException? bg, recoveryError;
            lock (_db.Mutex)
            {
                bg = _bgError;
                recoveryError = _recoveryError;
            }
            if (s?.Code == StatusCode.ShutdownInProgress || Severity(bg) >= ErrorSeverity.FatalError)
            {
                // Shutdown, or the error became worse than hard: stop.
                lock (_db.Mutex) _recoveryInProgress = false;
                _db.Stats?.RecordInHistogram(Histogram.ErrorHandlerAutoresumeRetryCount, retryCount);
                NotifyOnErrorRecoveryEnd(bg ?? oldError!, bg ?? s);
                return;
            }
            // recovery_error_.GetRetryable() only: a file-scoped error that is not retryable ends the recovery.
            if (recoveryError is not null && recoveryError.Severity <= ErrorSeverity.HardError && recoveryError.Retryable)
            {
                // A new retryable error happened during the recovery: wait and retry.
                TestHooks.Invoke("RecoverFromRetryableBGIOError:BeforeWait");
                try
                {
                    await Task.Delay(interval, _recoveryCancellation.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
            else
            {
                if (recoveryError is null && s is null)
                {
                    // Recovered (ClearBGError notified the listeners).
                    TestHooks.Invoke("RecoverFromRetryableBGIOError:RecoverSuccess");
                    _db.Stats?.RecordTick(Ticker.ErrorHandlerAutoresumeSuccessCount);
                    _db.Stats?.RecordInHistogram(Histogram.ErrorHandlerAutoresumeRetryCount, retryCount);
                    return;
                }
                // The recovery error is more serious or not retryable, or another error happened: stop.
                lock (_db.Mutex) _recoveryInProgress = false;
                _db.Stats?.RecordInHistogram(Histogram.ErrorHandlerAutoresumeRetryCount, retryCount);
                NotifyOnErrorRecoveryEnd(bg ?? oldError!, recoveryError ?? s);
                return;
            }
            resumeCount--;
        }
        lock (_db.Mutex)
        {
            _recoveryInProgress = false;
            oldError = _bgError;
        }
        if (oldError is not null)
        {
            NotifyOnErrorRecoveryEnd(oldError, new RocksDbException(StatusCode.Aborted, "Operation aborted: Exceeded resume retry count"));
        }
        TestHooks.Invoke("RecoverFromRetryableBGIOError:LoopOut");
        _db.Stats?.RecordInHistogram(Histogram.ErrorHandlerAutoresumeRetryCount, retryCount);
    }

    private static RocksDbException ShutdownStatus() => new(StatusCode.ShutdownInProgress, "Shutdown in progress");

    /// <summary>
    /// Port of CancelErrorRecoveryForShutDown + EndAutoRecovery: stops automatic recovery and waits for the recovery
    /// task. Called without the mutex, before close waits for background work.
    /// </summary>
    public async ValueTask EndAutoRecoveryAsync(bool synchronous)
    {
        Task t;
        lock (_db.Mutex)
        {
            _autoRecovery = false;
            _endRecovery = true;
            t = _recoveryTask;
        }
        // CancelErrorRecoveryForShutDown: a recovery waiting in the SstFileManager is dropped; one running right now sees
        // the shutdown and ends.
        if (_db.Options.SstFileManager is { } sfm && sfm.CancelErrorRecovery(this))
        {
            lock (_db.Mutex) _recoveryInProgress = false;
        }
        try
        {
            _recoveryCancellation.Cancel();
        }
        catch (AggregateException)
        {
        }
        try
        {
            if (synchronous) t.GetAwaiter().GetResult();
            else await t.ConfigureAwait(false);
        }
        catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException or OperationCanceledException)
        {
        }
    }

    // -----------------------------------------------------------------------------------------------------
    // Listener notifications (db/event_helpers.cc)

    /// <summary>
    /// Port of EventHelpers::NotifyOnBackgroundError: OnBackgroundError for every listener (each can replace or suppress
    /// the error), then, if auto recovery is planned, OnErrorRecoveryBegin (each can disable it). Called without the
    /// mutex. Returns the (possibly replaced) error, or null when suppressed.
    /// </summary>
    private RocksDbException? NotifyOnBackgroundError(BackgroundErrorReason reason, RocksDbException error, bool autoRecovery, out bool autoRecoveryOut)
    {
        autoRecoveryOut = autoRecovery;
        List<EventListener> listeners = _db.Options.Listeners;
        if (listeners.Count == 0) return error;
        var args = new BackgroundErrorEventArgs(reason, error);
        foreach (EventListener l in listeners)
        {
            try
            {
                l.OnBackgroundError(args);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                _db.LogWarn("EventListener.OnBackgroundError threw: " + e.Message);
            }
            if (args.Error is null) break;
        }
        RocksDbException? result = args.Error;
        if (result is not null && result.Severity == ErrorSeverity.NoError && error.Severity != ErrorSeverity.NoError)
        {
            // A replacement error without a severity keeps the classified one.
            result = result.WithSeverity(error.Severity);
        }
        if (result is not null && autoRecovery)
        {
            var begin = new ErrorRecoveryBeginEventArgs(reason, result);
            foreach (EventListener l in listeners)
            {
                try
                {
                    l.OnErrorRecoveryBegin(begin);
                }
                catch (Exception e) when (e is not OutOfMemoryException)
                {
                    _db.LogWarn("EventListener.OnErrorRecoveryBegin threw: " + e.Message);
                }
            }
            autoRecoveryOut = begin.AutoRecovery;
        }
        return result;
    }

    /// <summary>Port of EventHelpers::NotifyOnErrorRecoveryEnd. Called without the mutex.</summary>
    internal void NotifyOnErrorRecoveryEnd(RocksDbException oldError, RocksDbException? newError)
    {
        _db.LogInfo(newError is null ? "ErrorHandler: Recovered from background error" : "ErrorHandler: Recovery ended: " + newError.Message);
        List<EventListener> listeners = _db.Options.Listeners;
        if (listeners.Count == 0) return;
        var info = new BackgroundErrorRecoveryInfo { OldBackgroundError = oldError, NewBackgroundError = newError };
        foreach (EventListener l in listeners)
        {
            try
            {
                l.OnErrorRecoveryEnd(info);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                _db.LogWarn("EventListener.OnErrorRecoveryEnd threw: " + e.Message);
            }
        }
    }
}

/// <summary>
/// Error recovery of the DB: DBImpl::Resume and DBImpl::ResumeImpl (db/db_impl/db_impl.cc).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// Port of DBImpl::Resume: manual recovery from a background error. No-op when writes and background work are
    /// not stopped; Busy while an automatic recovery runs; NotSupported for read-only instances.
    /// </summary>
    public async ValueTask ResumeAsync(bool synchronous)
    {
        ThrowIfDisposed();
        LogInfo("Resuming DB");
        lock (_mutex)
        {
            if (!IsDbStopped && !IsBgWorkStopped) return; // nothing to do
            if (_readOnly) ThrowHelper.NotSupported("Cannot resume a read-only DB");
            if (ErrorHandler.IsRecoveryInProgress) throw new RocksDbException(StatusCode.Busy, "Resource busy: Recovery in progress");
        }
        RocksDbException? s;
        if (synchronous)
        {
            // The recovery awaits background work; run it off the caller's synchronization context.
            s = Task.Run(async () => await ErrorHandler.RecoverFromBgErrorAsync(isManual: true).ConfigureAwait(false)).GetAwaiter().GetResult();
        }
        else
        {
            s = await ErrorHandler.RecoverFromBgErrorAsync(isManual: true).ConfigureAwait(false);
        }
        if (s is not null) ThrowBackgroundError(s);
    }

    /// <summary>
    /// Port of DBImpl::ResumeImpl: waits for running background jobs, refuses fatal errors, switches to a new MANIFEST
    /// after a MANIFEST write error, replaces a failed WAL, flushes the memtables (all column families, or only the
    /// pending immutable memtables for kErrorRecoveryRetryFlush), deletes obsolete files and clears the error.
    /// Returns null on success. Called without the mutex, by <see cref="ErrorHandler"/> only.
    /// </summary>
    internal async ValueTask<RocksDbException?> ResumeImplAsync()
    {
        await WaitForBackgroundWorkAsync(synchronous: false).ConfigureAwait(false);
        TestHooks.Invoke("DBImpl::ResumeImpl:Start");
        RocksDbException? s = null;
        bool walError, retryOnly;
        lock (_mutex)
        {
            if (_shuttingDown || IsDisposed)
            {
                s = new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
            }
            else if (ErrorHandler.BgError is { Severity: > ErrorSeverity.HardError } bg)
            {
                LogInfo("DB resume requested but failed due to Fatal/Unrecoverable error");
                s = bg;
            }
            walError = ErrorHandler.IsWalWriteErrorRecovery;
            retryOnly = ErrorHandler.RetryFlushOnly && !walError;
        }

        if (s is null && Versions.IoStatus is not null)
        {
            // Recovering from a MANIFEST write error: the old MANIFEST may be corrupted (torn record), so write a dummy
            // edit, which starts a new MANIFEST, even if there is nothing to flush.
            ColumnFamilyData defaultCf = GetColumnFamilyData(DefaultColumnFamilyName);
            try
            {
                await Versions.LogAndApplyAsync(defaultCf, new VersionEdit { ColumnFamily = defaultCf.Id }, synchronous: false, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                SetBackgroundError(e, BackgroundErrorReason.ManifestWrite, walRelated: false);
                s = RocksDbException.FromException(e);
            }
        }

        if (s is null) s = await FlushForRecoveryAsync(walError, retryOnly).ConfigureAwait(false);

        if (s is null && walError)
        {
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in Versions.ColumnFamilies)
                {
                    if (!cfd.Dropped && cfd.LogNumber <= _failedWalNumber)
                    {
                        s = RocksDbException.Corruption("WAL write error recovery did not advance column family " + cfd.Name + " past WAL " + _failedWalNumber);
                        break;
                    }
                }
            }
        }

        try
        {
            PurgeObsoleteFiles();
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
            // Best effort.
        }

        RocksDbException? cleared = null;
        lock (_mutex)
        {
            if (s is null)
            {
                // Will notify and unblock threads waiting for error recovery to finish.
                s = ErrorHandler.ClearBgError(out cleared);
                if (s is null)
                {
                    MaybeScheduleFlush();
                    MaybeScheduleCompaction();
                }
            }
            if (_shuttingDown) s ??= new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
            SignalBackgroundWork();
        }
        if (cleared is not null) ErrorHandler.NotifyOnErrorRecoveryEnd(cleared, null);
        if (s is not null) LogInfo("DB resume requested but failed: " + s.Message);
        return s;
    }

    /// <summary>
    /// The flush part of ResumeImpl (FlushAllColumnFamilies / RetryFlushesForErrorRecovery): switches the memtables
    /// holding data (every column family after a WAL error, which also replaces the failed WAL), then runs the flushes
    /// on this task (background flush scheduling is stopped while the error is set). Returns the first failure.
    /// </summary>
    private async ValueTask<RocksDbException?> FlushForRecoveryAsync(bool walError, bool retryOnly)
    {
        List<ColumnFamilyData> cfds = GetColumnFamilies();
        if (!retryOnly)
        {
            WriteThread.Writer w;
            try
            {
                w = await EnterUnbatchedAsync(synchronous: false, CancellationToken.None).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                return new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
            }
            try
            {
                if (walError && Versions.LastAllocatedSequence > Versions.LastSequence)
                {
                    // ResumeImpl: the failed WAL write may have reached storage before its sequence range was published.
                    // Publish the reservation so later writes cannot reuse an ambiguous sequence; the recovery flush
                    // edits below persist it (their last_sequence covers it).
                    Versions.SetLastSequence(Versions.LastAllocatedSequence);
                }
                // One switch for all column families: the recovery flush below picks exactly these memtables.
                BeginAtomicSwitch();
                try
                {
                    foreach (ColumnFamilyData cfd in cfds)
                    {
                        if (cfd.Dropped) continue;
                        if (walError || !cfd.Mem.IsEmpty) await SwitchMemtableAsync(cfd, FlushReason.ErrorRecovery, synchronous: false, CancellationToken.None).ConfigureAwait(false);
                    }
                }
                finally
                {
                    EndAtomicSwitch();
                }
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                return RocksDbException.FromException(e);
            }
            finally
            {
                ExitUnbatched(w, synchronous: false);
            }
        }
        // RetryFlushesForErrorRecovery without atomic_flush: a flush request per column family with sealed memtables.
        if (retryOnly && !Options.AtomicFlush)
        {
            var members = new List<ColumnFamilyData>();
            lock (_mutex)
            {
                foreach (ColumnFamilyData cfd in cfds)
                {
                    if (cfd.Dropped || cfd.Imm.Count == 0) continue;
                    cfd.FlushRequested = true;
                    // A column family whose sealed memtables are all picked already is left to the flush that picked them.
                    if (!cfd.HasPendingFlushUpTo(ulong.MaxValue)) continue;
                    cfd.RunningFlushes++;
                    _runningFlushes++;
                    _backgroundJobs++;
                    members.Add(cfd);
                }
            }
            for (int i = 0; i < members.Count; i++)
            {
                // BackgroundFlushAsync releases the accounting taken above for its column family.
                await BackgroundFlushAsync(members[i]).ConfigureAwait(false);
                lock (_mutex)
                {
                    RocksDbException? stop = ErrorHandler.RecoveryError
                        ?? (_shuttingDown ? new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress") : null);
                    if (stop is null) continue;
                    // WaitForFlushMemTables stops waiting at a recovery error, but upstream's scheduled flushes still end
                    // and release their background job. Release the accounting of the flushes that will not run here, or
                    // the next recovery attempt waits for them forever (WaitForBackgroundWork), and keep them queued for
                    // the next attempt (their memtables stay FlushRequested).
                    for (int j = i + 1; j < members.Count; j++)
                    {
                        ColumnFamilyData skipped = members[j];
                        skipped.RunningFlushes--;
                        _runningFlushes--;
                        _backgroundJobs--;
                        EnqueueFlush(skipped);
                    }
                    MaybeScheduleFlush();
                    SignalBackgroundWork();
                    return stop;
                }
            }
            return null;
        }
        lock (_mutex)
        {
            // FlushAllColumnFamilies: cfd->imm()->FlushRequested() for every column family with sealed memtables.
            foreach (ColumnFamilyData cfd in cfds)
            {
                if (!cfd.Dropped && cfd.Imm.Count > 0) cfd.FlushRequested = true;
            }
        }
        // FlushAllColumnFamilies with force_atomic_flush (and RetryFlushesForErrorRecovery with atomic_flush): one atomic
        // flush of every column family, so the per-CF flush results (and with them the WAL replay boundaries) are committed
        // as one MANIFEST atomic group and a crash in between cannot recover only part of a write batch that spans column
        // families. Like any atomic flush it is installed after the older flushes of its column families.
        RocksDbException? s = await AtomicFlushMemTablesToOutputFilesAsync(synchronous: false, forRecovery: true).ConfigureAwait(false);
        lock (_mutex)
        {
            if (ErrorHandler.RecoveryError is { } re) return re;
            if (_shuttingDown) return new RocksDbException(StatusCode.ShutdownInProgress, "Shutdown in progress");
        }
        return s;
    }
}
