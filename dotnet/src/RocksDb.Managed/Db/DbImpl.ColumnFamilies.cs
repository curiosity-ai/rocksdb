namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private readonly SemaphoreSlim _optionsFileLock = new(1, 1);

    /// <summary>
    /// Set by a transaction DB: PessimisticTransactionDB::CreateColumnFamily / CreateColumnFamilies /
    /// CreateColumnFamilyWithImport run VerifyCFOptions on the options of a column family created after the open, before
    /// creating it (the managed transaction DB exposes the RocksDatabase itself, so the check hooks in here).
    /// </summary>
    internal Action<ColumnFamilyOptions>? VerifyNewColumnFamilyOptions { get; set; }

    /// <summary>Port of DBImpl::CreateColumnFamily(Impl).</summary>
    public async ValueTask<ColumnFamilyData> CreateColumnFamilyAsync(string name, ColumnFamilyOptions options, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        ColumnFamilyData cfd = await CreateColumnFamilyCoreAsync(name, options, writeOptionsFile: true, synchronous, ct).ConfigureAwait(false);
        lock (_mutex) EnsureSeqnoToTimeMapping(cfd.Options);
        RegisterRecordSeqnoTimeWorker();
        RescheduleCompactionTrigger();
        return cfd;
    }

    private async ValueTask<ColumnFamilyData> CreateColumnFamilyCoreAsync(string name, ColumnFamilyOptions options, bool writeOptionsFile, bool synchronous,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(options);
        VerifyNewColumnFamilyOptions?.Invoke(options);
        ColumnFamilyOptions opts = PrepareCfOptions(options);
        RegisterComparator(opts);
        // Leads synchronously once the DB has synchronous writers (see LeadsSynchronously); an asynchronous call
        // moves to a write wake-up thread first, so that it never blocks its caller.
        bool exclusive = LeadsSynchronously(synchronous);
        if (exclusive && !synchronous && !WriteThread.OnWakeupThread) await WriteThread.WakeupThreads.SwitchTo();
        // CreateColumnFamilyImpl enters the write thread without WaitForPendingWrites: allowed while the WAL is locked.
        WriteThread.Writer w = await EnterUnbatchedIgnoringWalLockAsync(exclusive, ct).ConfigureAwait(false);
        ColumnFamilyData cfd;
        try
        {
            ulong logNumber;
            uint id;
            lock (_mutex)
            {
                if (Versions.GetColumnFamily(name) is not null) ThrowHelper.InvalidArgument("Column family already exists");
                // ColumnFamilySet::GetNextColumnFamilyID (++max_column_family_): the id is consumed before the MANIFEST
                // write, so a failed creation does not hand its id to the next column family.
                id = ++Versions.MaxColumnFamily;
                logNumber = _logNumber;
            }
            cfd = new ColumnFamilyData(id, name, opts, this);
            var edit = new VersionEdit
            {
                ColumnFamily = id,
                ComparatorName = cfd.UserComparator.Name,
                PersistUserDefinedTimestamps = opts.PersistUserDefinedTimestamps,
                LogNumber = logNumber,
            };
            edit.AddColumnFamily(name);
            // The memtable and the super version are installed in the same mutex hold that makes the column family
            // visible (DBImpl::CreateColumnFamilyImpl holds mutex_ across LogAndApply and
            // InstallSuperVersionForConfigChange). Installed in a later critical section, the new column family was
            // visible without them for a moment: a stats dump, GetLiveFilesMetaData or a read through db.ColumnFamilies
            // on another thread hit a null super version.
            ColumnFamilyData created = cfd;
            await Versions.LogAndApplyAsync(cfd, edit, exclusive, () =>
            {
                created.NextEpochNumber = 1;
                created.Mem = created.ConstructNewMemtable(Versions.LastSequence);
                InstallSuperVersion(created);
            }, ct).ConfigureAwait(false);
            RegisterCfForWrite(cfd);
        }
        finally
        {
            ExitUnbatched(w, exclusive);
            // Back to the thread pool before the caller resumes.
            if (!synchronous && WriteThread.OnWakeupThread) await WriteThread.ResumeOnThreadPoolAsync();
        }
        // The column family is committed: a cancellation now would report a creation that happened as not done.
        if (writeOptionsFile) await WriteOptionsFileAsync(synchronous, CancellationToken.None).ConfigureAwait(false);
        return cfd;
    }

    /// <summary>
    /// Port of DBImpl::DropColumnFamily(Impl): records the drop in the MANIFEST and stops writes, flushes and compactions
    /// of the CF. Like upstream ("RocksDB's contract guarantees that we're still able to read dropped CF" while its
    /// ColumnFamilyHandle exists, db/column_family_test.cc ReadDroppedColumnFamily), reads and iterators through the
    /// <see cref="ColumnFamily"/> keep working on the data as of the drop: <see cref="ColumnFamily"/> objects live as
    /// long as the database, so the CF's memtables and table files are released (and the files deleted) when the
    /// database is closed.
    /// </summary>
    public async ValueTask DropColumnFamilyAsync(ColumnFamilyData cfd, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (cfd.Id == 0) ThrowHelper.InvalidArgument("Can't drop default column family");
        // Leads synchronously once the DB has synchronous writers (see LeadsSynchronously); an asynchronous call
        // moves to a write wake-up thread first, so that it never blocks its caller.
        bool exclusive = LeadsSynchronously(synchronous);
        if (exclusive && !synchronous && !WriteThread.OnWakeupThread) await WriteThread.WakeupThreads.SwitchTo();
        WriteThread.Writer w = await EnterUnbatchedAsync(exclusive, ct).ConfigureAwait(false);
        try
        {
            if (cfd.Dropped) ThrowHelper.InvalidArgument("Column family already dropped!\n");
            var edit = new VersionEdit { ColumnFamily = cfd.Id };
            edit.DropColumnFamily();
            await Versions.LogAndApplyAsync(cfd, edit, exclusive, ct).ConfigureAwait(false);
            UnregisterCfForWrite(cfd);
            lock (_mutex)
            {
                // The super version, memtables and current version stay referenced until close (see the summary): the
                // ColumnFamilyData is the handle's cfd, which upstream unrefs in ~ColumnFamilyHandleImpl.
                _droppedColumnFamilies.Add(cfd);
                _maybeStalled = IsWriteStalled();
                SignalBackgroundWork();
            }
        }
        finally
        {
            ExitUnbatched(w, exclusive);
            // Back to the thread pool before the caller resumes.
            if (!synchronous && WriteThread.OnWakeupThread) await WriteThread.ResumeOnThreadPoolAsync();
        }
        PurgeObsoleteFiles();
        RegisterRecordSeqnoTimeWorker();
        RescheduleCompactionTrigger();
        // The drop is committed: not cancellable any more (a cancellation would report it as not done).
        await WriteOptionsFileAsync(synchronous, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Port of DBImpl::WriteOptionsFile + DeleteObsoleteOptionsFiles: persists the DB options and every live CF's
    /// options to a new OPTIONS-N file and keeps only the two newest OPTIONS files (and those a running remote compaction
    /// may read).
    /// </summary>
    private async ValueTask WriteOptionsFileAsync(bool synchronous, CancellationToken ct)
    {
        var file = new OptionsFile { DbOptions = Options };
        foreach (ColumnFamilyData cfd in GetColumnFamilies())
        {
            file.ColumnFamilies.Add(new ColumnFamilyDescriptor(cfd.Name, cfd.Options));
        }
        if (synchronous) _optionsFileLock.Wait(ct);
        else await _optionsFileLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ulong number = Versions.NewFileNumber();
            await file.WriteToDbDirectoryAsync(FileSystem, DbName, number, synchronous, ct).ConfigureAwait(false);
            var existing = new List<(ulong Number, string Name)>();
            foreach (string child in FileSystem.GetChildren(DbName))
            {
                if (FileNames.TryParseFileName(child, out ulong n, out FileType type) && type == FileType.OptionsFile) existing.Add((n, child));
            }
            existing.Sort(static (a, b) => b.Number.CompareTo(a.Number));
            // Old OPTIONS files are obsolete files: not deleted while file deletions are disabled (upstream purges them
            // through PurgeObsoleteFiles), or a checkpoint / backup that captured the latest OPTIONS file lost it to
            // the second SetOptions after it. They are trimmed by the next write of an OPTIONS file.
            bool deletionsDisabled;
            lock (_mutex) deletionsDisabled = FileDeletionsDisabled;
            if (existing.Count > 2 && !deletionsDisabled)
            {
                ulong keepFrom = MinOptionsFileNumberToKeep(existing[1].Number);
                for (int i = 2; i < existing.Count; i++)
                {
                    if (existing[i].Number < keepFrom) TryDeleteFile(DbName + "/" + existing[i].Name);
                }
            }
        }
        finally
        {
            _optionsFileLock.Release();
        }
    }
}
