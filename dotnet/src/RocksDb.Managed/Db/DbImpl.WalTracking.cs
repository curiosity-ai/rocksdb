namespace RocksDb.Managed.Db;

/// <summary>
/// WAL tracking in the MANIFEST (track_and_verify_wals_in_manifest) and the commit of atomic flushes. Ports
/// DBImpl::SyncClosedWals / ApplyWALToManifest / MarkLogsSynced (db/db_impl/db_impl.cc), the WAL deletion records of
/// GetDBRecoveryEditForObsoletingMemTables (db/db_impl/db_impl_files.cc) and DBImpl::SwitchMemtable
/// (db/db_impl/db_impl_write.cc), and the WalSet checks of DBImpl::Recover (db/db_impl/db_impl_open.cc).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>First WAL created by this instance; older WALs (recovered at open) are not recorded as synced. Guarded by mutex.</summary>
    private ulong _walTrackingStart = ulong.MaxValue;

    /// <summary>
    /// Closed WALs that were fully synced (number -> synced size) and are not yet recorded in the MANIFEST
    /// (MarkLogsSynced's synced_wals). Closed WALs synced lazily (single column family) only appear once a sync write,
    /// SyncWAL or FlushWAL(sync) actually synced them. Guarded by mutex.
    /// </summary>
    private readonly Dictionary<ulong, ulong> _syncedClosedWals = new();

    /// <summary>A closed (inactive) WAL was fully synced: remember its size for the next WalAddition. Leader-only caller.</summary>
    private void NoteClosedWalSynced(Wal.LogWriter w)
    {
        if (!Options.TrackAndVerifyWalsInManifest) return;
        long size = w.File?.Size ?? 0;
        if (size <= 0) return;
        lock (_mutex) _syncedClosedWals[w.LogNumber] = (ulong)size;
    }

    /// <summary>
    /// Records the synced sizes of the closed WALs of this instance in the MANIFEST (MarkLogsSynced: inactive WALs with
    /// a non-zero pre-sync size become WalAddition(number, size)), whichever path synced them: a memtable switch with several
    /// column families, or the lazy sync of closed WALs (sync writes of either write queue, SyncWAL / FlushWAL(sync)),
    /// including the switches made by SwitchWAL (max_total_wal_size). A WAL whose write or sync failed is never recorded.
    /// </summary>
    private async ValueTask TrackClosedWalsAsync(bool synchronous)
    {
        if (!Options.TrackAndVerifyWalsInManifest || _readOnly) return;
        VersionEdit? edit = null;
        lock (_mutex)
        {
            if (_syncedClosedWals.Count == 0) return;
            foreach (KeyValuePair<ulong, ulong> w in _syncedClosedWals)
            {
                ulong n = w.Key;
                if (n < _walTrackingStart || n <= _failedWalNumber || n < Versions.MinWalNumberToKeep) continue;
                if (Versions.TrackedWals.TryGetValue(n, out ulong? size) && size is ulong s && s >= w.Value) continue;
                (edit ??= new VersionEdit()).WalAdditions.Add(new WalAddition(n, w.Value));
            }
            _syncedClosedWals.Clear();
        }
        if (edit is null) return;
        try
        {
            await Versions.LogAndApplyAsync(null, edit, synchronous, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Not recorded: retry with the next flush.
            lock (_mutex)
            {
                foreach (WalAddition w in edit.WalAdditions) _syncedClosedWals.TryAdd(w.LogNumber, w.SyncedSizeInBytes!.Value);
            }
            throw;
        }
    }

    /// <summary>
    /// Makes sure WALs below <paramref name="minWalToKeep"/> are recorded as deleted in the MANIFEST before their files are
    /// deleted (WalDeletion; upstream writes it with the flush or memtable switch that makes them obsolete). Returns
    /// false if the MANIFEST write failed: the WALs must then stay.
    /// </summary>
    private bool TryPersistWalDeletion(ulong minWalToKeep)
    {
        if (!Options.TrackAndVerifyWalsInManifest || minWalToKeep == 0 || minWalToKeep == ulong.MaxValue) return true;
        lock (_mutex)
        {
            if (minWalToKeep <= Versions.MinWalNumberToKeep) return true;
        }
        try
        {
            ValueTask t = Versions.LogAndApplyAsync(null, new VersionEdit { WalDeletionBefore = minWalToKeep }, synchronous: true, CancellationToken.None);
            t.GetAwaiter().GetResult();
            return true;
        }
        catch (Exception e) when (e is IOException or RocksDbException or ObjectDisposedException)
        {
            LogWarn("Failed to record WAL deletion before #" + minWalToKeep + " in the MANIFEST: " + e.Message);
            return false;
        }
    }

    /// <summary>
    /// Verifies the WALs tracked in the MANIFEST against the WAL directory at open (DBImpl::Recover): with
    /// track_and_verify_wals_in_manifest a missing or shrunk synced WAL is Corruption. With tracking disabled but WALs
    /// still tracked, they are untracked (DeleteWalsBefore(max + 1)) so re-enabling the option later cannot fail on
    /// WALs deleted meanwhile. Returns the edit to write with the recovery edits, or null.
    /// </summary>
    private VersionEdit? VerifyTrackedWalsAtOpen()
    {
        IReadOnlyDictionary<ulong, ulong?> tracked;
        lock (_mutex) tracked = new Dictionary<ulong, ulong?>(Versions.TrackedWals);
        if (Options.TrackAndVerifyWalsInManifest)
        {
            if (Options.BestEffortsRecovery) return null; // best-efforts recovery does not replay WALs
            var onDisk = new Dictionary<ulong, string>();
            foreach (string child in FileSystem.GetChildren(WalDir))
            {
                if (FileNames.TryParseFileName(child, out ulong number, out FileType type) && type == FileType.WalFile)
                {
                    onDisk[number] = FileNames.LogFileName(WalDir, number);
                }
            }
            WalSet.CheckWals(tracked, onDisk, FileSystem.GetFileSize);
            return null;
        }
        if (tracked.Count == 0) return null;
        ulong max = 0;
        foreach (ulong n in tracked.Keys) max = Math.Max(max, n);
        return new VersionEdit { WalDeletionBefore = max + 1 };
    }

    /// <summary>
    /// Writes the edits of an atomic flush as one atomic group (InstallMemtableAtomicFlushResults), followed by the
    /// group's min_log_number_to_keep / WAL deletion edit (PrecomputeMinLogNumberToKeepNon2PC over all members, appended
    /// to the last column family's edits, inside the group); <paramref name="installedUnderMutex"/> runs in the critical
    /// section that installs the new versions.
    /// </summary>
    internal async ValueTask CommitAtomicFlushAsync(List<ManifestEditEntry> edits, bool synchronous, Action installedUnderMutex)
    {
        var all = new List<ManifestEditEntry>(edits.Count + 1);
        all.AddRange(edits);
        lock (_mutex)
        {
            if (!Options.Allow2Pc)
            {
                ulong min = ulong.MaxValue;
                foreach (ColumnFamilyData c in Versions.ColumnFamilies)
                {
                    if (c.Dropped) continue;
                    ulong ln = c.LogNumber;
                    foreach (ManifestEditEntry e in edits)
                    {
                        if (e.Cfd == c && e.Edit.LogNumber is ulong l) ln = l;
                    }
                    min = Math.Min(min, ln);
                }
                if (min != ulong.MaxValue)
                {
                    var walDeletion = new VersionEdit();
                    if (min > Versions.MinLogNumberToKeep) walDeletion.MinLogNumberToKeep = min;
                    if (Options.TrackAndVerifyWalsInManifest && min > Versions.MinWalNumberToKeep) walDeletion.WalDeletionBefore = min;
                    if (walDeletion.MinLogNumberToKeep is not null || walDeletion.IsWalDeletion) all.Add(new ManifestEditEntry(null, walDeletion));
                }
            }
        }
        TestHooks.Invoke("DBImpl::AtomicFlush:BeforeLogAndApply");
        await Versions.LogAndApplyAsync(all, atomicGroup: true, synchronous, installedUnderMutex, CancellationToken.None).ConfigureAwait(false);
    }
}
