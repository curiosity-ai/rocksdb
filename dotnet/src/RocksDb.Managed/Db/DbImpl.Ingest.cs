namespace RocksDb.Managed.Db;

/// <summary>
/// External file ingestion. Port of DBImpl::IngestExternalFile(s) (db/db_impl/db_impl.cc): prepare (validate + copy
/// or link into the DB), stop writes, flush the memtables if they overlap the ingested range (allow_blocking_flush),
/// assign levels and global sequence numbers, consume a sequence number if one was assigned, LogAndApply and install
/// a new super version.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// Port of DBImpl::VerifyChecksumInternal(use_file_checksum = true) and VerifyFullFileChecksum: every live table file
    /// of every column family, then its blob files, on referenced super versions.
    /// </summary>
    public void VerifyFileChecksums()
    {
        ThrowIfDisposed();
        FileChecksumGenFactory? factory = Options.FileChecksumGenFactory;
        if (factory is null) ThrowHelper.InvalidArgument("Cannot verify file checksum if options.file_checksum_gen_factory is null");
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            if (cfd.Dropped) continue;
            SuperVersion sv = GetReferencedSuperVersion(cfd);
            try
            {
                VersionStorageInfo s = sv.Current.Storage;
                for (int l = 0; l < s.NumLevels; l++)
                {
                    foreach (FileMetaData f in s.LevelFiles(l))
                    {
                        VerifyFullFileChecksum(factory!, f.FileChecksum, f.FileChecksumFuncName, FileNames.TableFileName(DbPaths, f.FileNumber, f.PathId));
                    }
                }
                foreach (Blob.BlobFileMetaData b in s.BlobFiles)
                {
                    VerifyFullFileChecksum(factory!, b.Addition.ChecksumValue, b.Addition.ChecksumMethod, FileNames.BlobFileName(DbPaths[0], b.BlobFileNumber));
                }
            }
            finally
            {
                ReturnSuperVersion(sv);
            }
        }
    }

    private void VerifyFullFileChecksum(FileChecksumGenFactory factory, byte[] expected, string funcName, string path)
    {
        // kUnknownFileChecksum: nothing to verify.
        if (expected.Length == 0) return;
        // Env::IOActivity::kVerifyFileChecksums: FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS.
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadVerifyFileChecksumsMicros);
        byte[] actual;
        try
        {
            (actual, _) = FileChecksumHelper.GenerateOneFileChecksum(FileSystem, path, factory, funcName, Stats);
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        if (!actual.AsSpan().SequenceEqual(expected))
        {
            ThrowHelper.Corruption(path + " file checksum mismatch, expecting " + FileChecksumHelper.Hex(expected) + ", but actual "
                + FileChecksumHelper.Hex(actual));
        }
    }

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

    public ValueTask IngestExternalFilesAsync(ColumnFamilyData cfd, IReadOnlyList<string> files, IngestExternalFileOptions options, bool synchronous,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(options);
        return IngestExternalFilesAsync(cfd, new IngestExternalFileArg { ExternalFiles = files, Options = options }, synchronous, ct);
    }

    /// <summary>DBImpl::IngestExternalFiles for one IngestExternalFileArg (files, options, checksums, temperature hint).</summary>
    public async ValueTask IngestExternalFilesAsync(ColumnFamilyData cfd, IngestExternalFileArg arg, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(arg);
        IReadOnlyList<string> files = arg.ExternalFiles;
        IngestExternalFileOptions options = arg.Options;
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (files.Count == 0) ThrowHelper.InvalidArgument("The list of files is empty");
        // DBImpl::IngestExternalFiles argument checks.
        if (options.IngestBehind)
        {
            if (cfd.TimestampSize > 0) ThrowHelper.NotSupported("Column family with user-defined timestamps enabled doesn't support ingest behind.");
            if (!cfd.Options.AllowIngestBehind)
            {
                // (sic: upstream's message keeps the unexpanded %s)
                ThrowHelper.InvalidArgument("Can't ingest_behind file in ColumnFamily %s with cf_allow_ingest_behind=false");
            }
        }
        if (arg.AtomicReplaceRange is { } replaceRange)
        {
            if (cfd.Options.CompactionStyle == CompactionStyle.Fifo) ThrowHelper.NotSupported("atomic_replace_range is not supported with FIFO compaction.");
            if (options.IngestBehind) ThrowHelper.InvalidArgument("Can't combine atomic_replace_range with ingest_behind.");
            // TODO upstream: support generating and ingesting a big tombstone file, which might depend on non-null start
            // and limit.
            if (options.SnapshotConsistency) ThrowHelper.NotSupported("atomic_replace_range not yet supported with snapshot_consistency.");
            if ((replaceRange.Start is null) != (replaceRange.Limit is null))
            {
                ThrowHelper.NotSupported("Only one of atomic_replace_range.{start,limit}.has_value() is not supported.");
            }
        }
        if (options.MoveFiles && options.LinkFiles) ThrowHelper.InvalidArgument("`move_files` and `link_files` can not both be true.");
        var job = new ExternalSstFileIngestionJob(this, cfd, options);
        bool success = false;
        BeginIngestion();
        try
        {
            SuperVersion prepareSv = GetReferencedSuperVersion(cfd);
            try
            {
                await job.PrepareAsync(files, arg.FilesChecksums, arg.FilesChecksumFuncNames, arg.FileTemperature, arg.AtomicReplaceRange,
                    prepareSv.Current.Storage, synchronous, ct).ConfigureAwait(false);
            }
            finally
            {
                ReturnSuperVersion(prepareSv);
            }
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
                        needFlush = job.NeedsFlush(sv);
                        if (!needFlush)
                        {
                            VersionEdit edit;
                            ulong last;
                            lock (_mutex)
                            {
                                // Run picks levels from the current version and checks running compactions; RegisterRange
                                // then keeps compactions picked while LogAndApply releases the mutex away from the
                                // ingested ranges and the replaced files.
                                bool hasSnapshots = SnapshotCount > 0;
                                last = Versions.LastSequence;
                                edit = job.Run(last, hasSnapshots);
                                job.RegisterRange();
                            }
                            // Every file of an overlapping batch (or every file with snapshots) gets its own sequence
                            // number; DB-generated files bring their own. max_assigned_seqno > last_seqno consumes them.
                            ulong maxAssigned = job.MaxAssignedSeqno;
                            bool consumed = maxAssigned > last;
                            bool applied = false;
                            try
                            {
                                if (consumed) Versions.AllocateSequence(maxAssigned - last);
                                TestHooks.Invoke("DBImpl::IngestExternalFiles:BeforeLogAndApply");
                                // The MANIFEST edit records the files' largest seqno itself (VersionEdit.AddFile), so the
                                // last sequence is published only once the files are installed: a snapshot taken while
                                // the MANIFEST is written must not get the ingested files' seqno before they are visible
                                // (DBImpl::IngestExternalFiles updates the last seqno after LogAndApply for the same
                                // reason).
                                await Versions.LogAndApplyAsync(cfd, edit, synchronous, CancellationToken.None).ConfigureAwait(false);
                                applied = true;
                            }
                            finally
                            {
                                lock (_mutex)
                                {
                                    job.UnregisterRange(restoreCompactionCandidates: !applied);
                                    if (applied)
                                    {
                                        if (consumed) Versions.SetLastSequence(maxAssigned);
                                        InstallSuperVersion(cfd);
                                        foreach (ulong n in job.ReservedFileNumbers) _pendingOutputs.Remove(n);
                                    }
                                    else if (_compactionsEnabled)
                                    {
                                        // Compaction may have been skipped while replacement files were reserved. Retry
                                        // now that the failed ingestion has released them.
                                        SchedulePendingCompaction(cfd);
                                        MaybeScheduleCompaction();
                                    }
                                }
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
                if (cfd.TimestampSize > 0)
                {
                    // ExternalSstFileIngestionJob::NeedsFlush: no flush on behalf of a timestamped ingestion.
                    ThrowHelper.InvalidArgument("Column family enables user-defined timestamps, please make sure the key range (without timestamp) " +
                                                "of external file does not overlap with key range in the memtables.");
                }
                if (attempt >= 8) throw new RocksDbException(StatusCode.TryAgain, "Operation failed. Try again.: memtable keeps overlapping the ingested files");
                await FlushColumnFamilyAsync(cfd, wait: true, FlushReason.ExternalFileIngestion, synchronous, ct).ConfigureAwait(false);
            }
            job.DeleteUnusedRangeTombstone();
            job.DeleteExternalLinks();
            // Replaced files (atomic_replace_range) are obsolete now.
            if (job.NumFilesReplaced > 0) PurgeObsoleteFiles();
            if (InfoLog is not null)
            {
                // ExternalSstFileIngestionJob::UpdateStats: the ingest_finished event.
                var ingested = new List<(string Path, int Level)>(job.Files.Count);
                foreach (IngestedFileInfo f in job.Files) ingested.Add((f.InternalFilePath, f.PickedLevel));
                LogIngestFinishedEvent(cfd, ingested);
            }
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
                    foreach (ulong n in job.ReservedFileNumbers) _pendingOutputs.Remove(n);
                }
            }
        }
    }
}
