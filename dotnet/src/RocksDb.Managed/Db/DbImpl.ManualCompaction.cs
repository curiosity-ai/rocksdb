namespace RocksDb.Managed.Db;

/// <summary>
/// DisableManualCompaction / EnableManualCompaction (db/db_impl/db_impl_compaction_flush.cc): while disabled
/// (manual_compaction_paused_ &gt; 0) CompactRange / CompactFiles fail with Incomplete(kManualCompactionPaused), manual
/// compactions waiting to start fail the same way and running ones are canceled through their cancellation token
/// (upstream: the manual_compaction_canceled_ flag polled by the compaction iterator).
/// </summary>
internal sealed partial class DbImpl
{
    private int _manualCompactionPaused; // manual_compaction_paused_. Guarded by mutex.
    private CancellationTokenSource _manualCompactionCancel = new(); // canceled by DisableManualCompaction. Guarded by mutex.

    /// <summary>
    /// The cancellation of a manual compaction run (dispose it when the run ends): canceled by shutdown or by
    /// <see cref="DisableManualCompactionAsync"/> (already canceled while manual compactions are disabled).
    /// </summary>
    private CancellationTokenSource NewManualCompactionCancellation()
    {
        lock (_mutex) return CancellationTokenSource.CreateLinkedTokenSource(_bgCancellation.Token, _manualCompactionCancel.Token);
    }

    private static RocksDbException ManualCompactionPausedException(Exception? inner) =>
        new(StatusCode.Incomplete, "Incomplete: Manual compaction paused", StatusSubCode.ManualCompactionPaused, inner);

    private void ThrowIfManualCompactionPaused()
    {
        if (Volatile.Read(ref _manualCompactionPaused) > 0) throw ManualCompactionPausedException(null);
    }

    /// <summary>
    /// DBImpl::DisableManualCompaction: pauses manual compactions and waits until none is pending or running (they fail
    /// with Incomplete), so no manual compaction commits while disabled. Calls nest.
    /// </summary>
    public async ValueTask DisableManualCompactionAsync(bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        lock (_mutex)
        {
            _manualCompactionPaused++;
            // Mark the running and queued manual compactions canceled, and wake the ones waiting to start.
            _manualCompactionCancel.Cancel();
            SignalBackgroundWork();
        }
        // Wait for any pending manual compactions to finish (typically through failing with Status::Incomplete).
        while (true)
        {
            Task wait;
            lock (_mutex)
            {
                if (_manualCompactionsRunning == 0 || IsDisposed) return;
                wait = _bgSignal.Task;
            }
            try
            {
                if (synchronous) wait.Wait(ct);
                else await wait.WaitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                // The caller gets no chance to call EnableManualCompaction for a disable that failed: undo it here
                // (the manual compactions canceled above stay canceled).
                EnableManualCompaction();
                throw;
            }
        }
    }

    /// <summary>
    /// DBImpl::EnableManualCompaction: undoes one <see cref="DisableManualCompactionAsync"/>; manual compactions are
    /// allowed again when every disable was matched.
    /// </summary>
    public void EnableManualCompaction()
    {
        lock (_mutex)
        {
            if (_manualCompactionPaused == 0) ThrowHelper.InvalidArgument("EnableManualCompaction() without a matching DisableManualCompaction()");
            if (--_manualCompactionPaused == 0)
            {
                _manualCompactionCancel.Dispose();
                _manualCompactionCancel = new CancellationTokenSource();
            }
        }
    }
}
