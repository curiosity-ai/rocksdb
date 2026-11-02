namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private readonly SemaphoreSlim _optionsFileLock = new(1, 1);

    /// <summary>Port of DBImpl::CreateColumnFamily(Impl).</summary>
    public async ValueTask<ColumnFamilyData> CreateColumnFamilyAsync(string name, ColumnFamilyOptions options, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        ColumnFamilyData cfd = await CreateColumnFamilyCoreAsync(name, options, writeOptionsFile: true, synchronous, ct).ConfigureAwait(false);
        return cfd;
    }

    private async ValueTask<ColumnFamilyData> CreateColumnFamilyCoreAsync(string name, ColumnFamilyOptions options, bool writeOptionsFile, bool synchronous,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(options);
        ColumnFamilyOptions opts = PrepareCfOptions(options);
        RegisterComparator(opts);
        WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
        ColumnFamilyData cfd;
        try
        {
            ulong logNumber;
            uint id;
            lock (_mutex)
            {
                if (Versions.GetColumnFamily(name) is not null) ThrowHelper.InvalidArgument("Column family already exists");
                id = Versions.MaxColumnFamily + 1;
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
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, ct).ConfigureAwait(false);
            lock (_mutex)
            {
                cfd.NextEpochNumber = 1;
                cfd.Mem = cfd.ConstructNewMemtable(Versions.LastSequence);
                InstallSuperVersion(cfd);
            }
            RegisterCfForWrite(cfd);
        }
        finally
        {
            ExitUnbatched(w, synchronous);
        }
        if (writeOptionsFile) await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
        return cfd;
    }

    /// <summary>
    /// Port of DBImpl::DropColumnFamily(Impl): records the drop in the MANIFEST, discards the CF's memtables and makes
    /// its table files obsolete. Reads through existing handles fail with ColumnFamilyDropped afterwards.
    /// </summary>
    public async ValueTask DropColumnFamilyAsync(ColumnFamilyData cfd, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        if (cfd.Id == 0) ThrowHelper.InvalidArgument("Can't drop default column family");
        WriteThread.Writer w = await EnterUnbatchedAsync(synchronous, ct).ConfigureAwait(false);
        try
        {
            if (cfd.Dropped) ThrowHelper.InvalidArgument("Column family already dropped!");
            var edit = new VersionEdit { ColumnFamily = cfd.Id };
            edit.DropColumnFamily();
            await Versions.LogAndApplyAsync(cfd, edit, synchronous, ct).ConfigureAwait(false);
            UnregisterCfForWrite(cfd);
            lock (_mutex)
            {
                DbVersion current = cfd.Current;
                // Readers holding a super version and running flushes / compactions may still read the CF's memtables
                // and table files: memtables are reference counted, the table cache is closed at shutdown.
                ReleaseColumnFamilyMemTables(cfd);
                _droppedColumnFamilies.Add(cfd);
                Versions.UnrefVersion(current);
                _maybeStalled = IsWriteStalled();
                SignalBackgroundWork();
            }
        }
        finally
        {
            ExitUnbatched(w, synchronous);
        }
        PurgeObsoleteFiles();
        await WriteOptionsFileAsync(synchronous, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Port of DBImpl::WriteOptionsFile + DeleteObsoleteOptionsFiles: persists the DB options and every live CF's
    /// options to a new OPTIONS-N file and keeps only the two newest OPTIONS files.
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
            await AwaitIo(file.WriteToDbDirectoryAsync(FileSystem, DbName, number, ct), synchronous).ConfigureAwait(false);
            var existing = new List<(ulong Number, string Name)>();
            foreach (string child in FileSystem.GetChildren(DbName))
            {
                if (FileNames.TryParseFileName(child, out ulong n, out FileType type) && type == FileType.OptionsFile) existing.Add((n, child));
            }
            existing.Sort(static (a, b) => b.Number.CompareTo(a.Number));
            for (int i = 2; i < existing.Count; i++) TryDeleteFile(DbName + "/" + existing[i].Name);
        }
        finally
        {
            _optionsFileLock.Release();
        }
    }
}
