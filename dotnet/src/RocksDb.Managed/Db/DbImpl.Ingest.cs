namespace RocksDb.Managed.Db;

/// <summary>
/// External file ingestion. Port of DBImpl::IngestExternalFile(s) (db/db_impl/db_impl.cc): prepare (validate + copy
/// or link into the DB), stop writes, flush the memtables if they overlap the ingested range (allow_blocking_flush),
/// assign levels and global sequence numbers, consume a sequence number if one was assigned, LogAndApply and install
/// a new super version.
/// </summary>
internal sealed partial class DbImpl
{
    internal ulong ReserveFileNumberForIngestion()
    {
        ulong n = Versions.NewFileNumber();
        lock (_mutex) _pendingOutputs.Add(n);
        return n;
    }

    internal ulong NewEpochNumberForIngestion(ColumnFamilyData cfd)
    {
        lock (_mutex) return cfd.NewEpochNumber();
    }

    public async ValueTask IngestExternalFilesAsync(ColumnFamilyData cfd, IReadOnlyList<string> files, IngestExternalFileOptions options, bool synchronous,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (files.Count == 0) ThrowHelper.InvalidArgument("The list of files is empty");
        var job = new ExternalSstFileIngestionJob(this, cfd, options);
        bool success = false;
        BeginIngestion();
        try
        {
            await job.PrepareAsync(files, synchronous, ct).ConfigureAwait(false);
            for (int attempt = 0; ; attempt++)
            {
                // Stop writes to the DB (WriteThread::EnterUnbatched), then check the memtables.
                WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
                bool needFlush;
                try
                {
                    ThrowIfDbStopped();
                    if (cfd.Dropped) ThrowDropped(cfd);
                    SuperVersion sv = GetReferencedSuperVersion(cfd);
                    try
                    {
                        needFlush = job.OverlapsWithMemtables(sv);
                        if (!needFlush)
                        {
                            bool hasSnapshots = SnapshotCount > 0;
                            ulong last = Versions.LastSequence;
                            VersionEdit edit = job.Run(sv, last, hasSnapshots, out bool consumed);
                            if (consumed) Versions.AllocateSequence(1);
                            // The MANIFEST edit records the files' largest seqno itself (VersionEdit.AddFile), so the last
                            // sequence is published only once the files are installed: a snapshot taken while the MANIFEST
                            // is written must not get the ingested files' seqno before they are visible (DBImpl::
                            // IngestExternalFiles updates the last seqno after LogAndApply for the same reason).
                            await Versions.LogAndApplyAsync(cfd, edit, synchronous, CancellationToken.None).ConfigureAwait(false);
                            lock (_mutex)
                            {
                                if (consumed) Versions.SetLastSequence(last + 1);
                                InstallSuperVersion(cfd);
                                foreach (IngestedFileInfo f in job.Files) _pendingOutputs.Remove(f.FileNumber);
                            }
                            if (consumed) NotifySequenceWaiters();
                            success = true;
                        }
                    }
                    finally
                    {
                        ReturnSuperVersion(sv);
                    }
                }
                finally
                {
                    ExitUnbatched(w, synchronous);
                }
                if (success) break;
                if (!options.AllowBlockingFlush) ThrowHelper.InvalidArgument("External file requires flush");
                if (attempt >= 8) throw new RocksDbException(StatusCode.TryAgain, "Operation failed. Try again.: memtable keeps overlapping the ingested files");
                await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.ExternalFileIngestion, synchronous, ct).ConfigureAwait(false);
            }
            job.DeleteExternalLinks();
            NotifyOnExternalFileIngested(cfd, job);
        }
        finally
        {
            EndIngestion();
            if (!success)
            {
                job.DeleteInternalFiles();
                lock (_mutex)
                {
                    foreach (IngestedFileInfo f in job.Files) _pendingOutputs.Remove(f.FileNumber);
                }
            }
        }
    }
}
