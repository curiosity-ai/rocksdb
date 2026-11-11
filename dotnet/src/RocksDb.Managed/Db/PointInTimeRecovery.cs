namespace RocksDb.Managed.Db;

/// <summary>
/// Checks whether a file referenced by the MANIFEST can be used (VersionEditHandlerPointInTime::VerifyFile /
/// VerifyBlobFile, db/version_edit_handler.cc, and VersionSet::VerifyFileMetadata, db/version_set.cc). A file that is
/// missing, has the wrong size or cannot be opened counts as missing.
/// </summary>
public interface IRecoveryFileVerifier
{
    /// <summary>True when the table file is present and valid.</summary>
    bool VerifyTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta);

    /// <summary>True when the blob file is present and valid.</summary>
    bool VerifyBlobFile(ColumnFamilyVersion cf, BlobFileAddition addition);

    /// <summary>
    /// Opens a table file of a point-in-time version about to be created (VersionBuilder::LoadTableHandlers called by
    /// VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit). False when the file is corrupt, which skips
    /// that point in time; any other failure is thrown and fails the recovery.
    /// </summary>
    bool LoadTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta) => true;
}

/// <summary>
/// Best-efforts (point-in-time) MANIFEST replay. Port of VersionEditHandlerPointInTime (db/version_edit_handler.cc) and
/// the found/missing file tracking of VersionBuilder (db/version_builder.cc: ApplyFileAddition, ValidVersionAvailable,
/// ContainsCompleteVersion, OnlyMissingL0Suffix, MissingL0FilesAreL0Suffix, RemainingSstFilesNotMissingBlobFiles,
/// GetMinOldestBlobFileNumber, AddBlobFileIfNeeded).
/// <para>
/// Each column family keeps the latest point in time ("version") that is valid: no table file is missing, or only a
/// suffix of the newest L0 files (with the blob files only they reference) is, and every missing blob file is older than
/// all blob files still referenced. As upstream, a version is created only on a negative edge (valid before an edit,
/// invalid after it: the state before the edit), when an atomic group begins and at the end, and only when every table
/// file of it can be opened (LoadTableHandlers): a version with a corrupt table file is skipped, so the column family
/// stays at its previous version (or empty). Atomic groups: the valid states of all column families are saved when a group starts,
/// no version is created inside it, and the versions created after it are only used once every column family that
/// existed at its start has one (atomic_update_versions_). A column family edited inside an atomic group only accepts
/// complete versions from then on (edited_in_atomic_group_).
/// </para>
/// </summary>
internal sealed class PointInTimeTracker
{
    private sealed class CfState
    {
        public readonly HashSet<ulong> MissingL0 = new();
        public readonly HashSet<ulong> MissingNonL0 = new();
        public readonly HashSet<ulong> MissingBlobs = new();
        public ulong MissingBlobsHigh;
        public bool EditedInAtomicGroup;
        public bool EverMissing;
        /// <summary>The latest valid point in time recorded so far (null: none).</summary>
        public ColumnFamilyVersion? Version;
        /// <summary>The valid state before the edit being applied (null: it was not valid, or the edit adds no file).</summary>
        public ColumnFamilyVersion? BeforeEdit;
    }

    private readonly IRecoveryFileVerifier _verifier;
    private readonly Dictionary<uint, CfState> _cfs = new();
    private bool _inAtomicGroup;
    // atomic_update_versions_: column family -> version created after the last atomic group (null = not yet).
    private Dictionary<uint, ColumnFamilyVersion?>? _atomicUpdate;

    public PointInTimeTracker(IRecoveryFileVerifier verifier)
    {
        _verifier = verifier;
    }

    /// <summary>
    /// MaybeCreateVersionBeforeApplyEdit's missing_info: no version is created while the edits replayed so far (not
    /// counting the one being applied) have not given a log number, a next file number and a last sequence. Set by the
    /// replayer before <see cref="BeforeApply"/> and <see cref="OnAtomicGroupBegin"/>.
    /// </summary>
    public bool MissingInfo { get; set; }

    public bool HasMissingFiles
    {
        get
        {
            foreach (CfState s in _cfs.Values)
            {
                if (s.EverMissing) return true;
            }
            return false;
        }
    }

    private CfState State(uint id)
    {
        if (!_cfs.TryGetValue(id, out CfState? s)) _cfs[id] = s = new CfState();
        return s;
    }

    /// <summary>MaybeCreateVersionBeforeApplyEdit (the version before an edit that may make the column family invalid).</summary>
    public void BeforeApply(ColumnFamilyVersion cf, VersionEdit edit)
    {
        CfState s = State(cf.Id);
        if (_inAtomicGroup)
        {
            // VersionBuilder::Rep::Apply (db/version_builder.cc): "if (track_found_and_missing_files_ && version_updated)
            // ... if (!edited_in_atomic_group_ && edit->IsInAtomicGroup()) edited_in_atomic_group_ = true" - only an edit
            // that changes table or blob files counts (an atomic flush member without an output only carries a log number).
            if (edit.NewFiles.Count != 0 || edit.DeletedFiles.Count != 0 || edit.BlobFileAdditions.Count != 0 || edit.BlobFileGarbages.Count != 0)
            {
                s.EditedInAtomicGroup = true;
            }
            return;
        }
        // Only added table or blob files can turn a valid state invalid.
        if (edit.NewFiles.Count == 0 && edit.BlobFileAdditions.Count == 0) return;
        if (MissingInfo) return;
        // valid_pit_before_edit: remember the state the builder would be rolled back to (RollbackLastApply).
        if (IsValid(cf, s)) s.BeforeEdit = Snapshot(cf, s, includeFiles: true);
    }

    /// <summary>ApplyFileAddition / ApplyFileDeletion / ApplyBlobFileAddition bookkeeping of found and missing files.</summary>
    public void AfterApply(ColumnFamilyVersion cf, VersionEdit edit)
    {
        CfState s = State(cf.Id);
        foreach ((_, ulong number) in edit.DeletedFiles)
        {
            if (!s.MissingL0.Remove(number)) s.MissingNonL0.Remove(number);
        }
        foreach (NewFileEntry nf in edit.NewFiles)
        {
            if (_verifier.VerifyTableFile(cf, nf.Level, nf.Meta)) continue;
            s.EverMissing = true;
            if (nf.Level == 0) s.MissingL0.Add(nf.Meta.FileNumber);
            else s.MissingNonL0.Add(nf.Meta.FileNumber);
        }
        foreach (BlobFileAddition a in edit.BlobFileAdditions)
        {
            if (_verifier.VerifyBlobFile(cf, a)) continue;
            s.EverMissing = true;
            s.MissingBlobs.Add(a.BlobFileNumber);
            s.MissingBlobsHigh = Math.Max(s.MissingBlobsHigh, a.BlobFileNumber);
        }
        // Negative edge: valid before this edit, not valid after it.
        ColumnFamilyVersion? before = s.BeforeEdit;
        s.BeforeEdit = null;
        if (before is not null && !_inAtomicGroup && !IsValid(cf, s) && LoadTables(before)) InstallVersion(before, s);
    }

    /// <summary>
    /// LoadTableHandlers on the version about to be created: false (the point in time is skipped) when one of its table
    /// files is corrupt.
    /// </summary>
    private bool LoadTables(ColumnFamilyVersion v)
    {
        foreach ((int level, FileMetaData f) in v.AllFiles())
        {
            if (!_verifier.LoadTableFile(v, level, f)) return false;
        }
        return true;
    }

    /// <summary>OnAtomicGroupReplayBegin: saves the valid states of all column families first.</summary>
    public void OnAtomicGroupBegin(RecoveredVersionState state)
    {
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
        {
            CfState s = State(cf.Id);
            if (!MissingInfo && IsValid(cf, s)) SaveVersion(cf, s);
        }
        _inAtomicGroup = true;
        // Older incomplete atomic updates are dropped; every column family existing now takes part in this one.
        _atomicUpdate = new Dictionary<uint, ColumnFamilyVersion?>();
        foreach (uint id in state.ColumnFamilies.Keys) _atomicUpdate[id] = null;
    }

    public void OnAtomicGroupEnd() => _inAtomicGroup = false;

    /// <summary>DestroyCfAndCleanup: a dropped column family no longer takes part in a pending atomic update.</summary>
    public void OnColumnFamilyDrop(uint id)
    {
        _cfs.Remove(id);
        if (_atomicUpdate is not null && _atomicUpdate.Remove(id)) MaybeApplyAtomicUpdate();
    }

    /// <summary>
    /// CheckIterationResult: each column family ends at its final state if that is valid, else at its last valid
    /// version (empty if it never had one).
    /// </summary>
    public void Finish(RecoveredVersionState state)
    {
        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
        {
            CfState s = State(cf.Id);
            if (IsValid(cf, s)) SaveVersion(cf, s);
        }
        var ids = new List<uint>(state.ColumnFamilies.Keys);
        foreach (uint id in ids)
        {
            ColumnFamilyVersion cf = state.ColumnFamilies[id];
            ColumnFamilyVersion chosen = State(id).Version ?? Snapshot(cf, new CfState(), includeFiles: false);
            // Column family level fields are not versioned (ExtractInfoFromVersionEdit applies them as they come).
            chosen.LogNumber = cf.LogNumber;
            chosen.ComparatorName = cf.ComparatorName;
            chosen.PersistUserDefinedTimestamps = cf.PersistUserDefinedTimestamps;
            chosen.FullHistoryTsLow = cf.FullHistoryTsLow;
            state.ColumnFamilies[id] = chosen;
        }
    }

    /// <summary>MaybeCreateVersionBeforeApplyEdit with force_create_version for a valid column family.</summary>
    private void SaveVersion(ColumnFamilyVersion cf, CfState s)
    {
        ColumnFamilyVersion v = Snapshot(cf, s, includeFiles: true);
        if (LoadTables(v)) InstallVersion(v, s);
    }

    private void InstallVersion(ColumnFamilyVersion v, CfState s)
    {
        ColumnFamilyVersion cf = v;
        if (_atomicUpdate is not null && _atomicUpdate.ContainsKey(cf.Id))
        {
            if (_inAtomicGroup) return; // no versions inside a group (the group-begin save happens before it starts)
            _atomicUpdate[cf.Id] = v;
            MaybeApplyAtomicUpdate();
            return;
        }
        s.Version = v;
    }

    private void MaybeApplyAtomicUpdate()
    {
        if (_atomicUpdate is null) return;
        foreach (ColumnFamilyVersion? v in _atomicUpdate.Values)
        {
            if (v is null) return;
        }
        foreach (KeyValuePair<uint, ColumnFamilyVersion?> kv in _atomicUpdate) State(kv.Key).Version = kv.Value;
        _atomicUpdate = null;
    }

    // ---- validity (VersionBuilder::ValidVersionAvailable) --------------------------------------------------------

    private static bool IsLinked(ColumnFamilyVersion cf, ulong blobFileNumber, out bool onlyMissingL0, HashSet<ulong> missingL0)
    {
        bool linked = false;
        onlyMissingL0 = true;
        foreach ((int level, FileMetaData f) in cf.AllFiles())
        {
            if (f.OldestBlobFileNumber != blobFileNumber) continue;
            linked = true;
            if (level != 0 || !missingL0.Contains(f.FileNumber)) onlyMissingL0 = false;
        }
        return linked;
    }

    /// <summary>GetMinOldestBlobFileNumber: the smallest blob file number referenced by an SST (0 = none).</summary>
    private static ulong MinOldestBlobFileNumber(ColumnFamilyVersion cf)
    {
        ulong min = 0;
        foreach ((_, FileMetaData f) in cf.AllFiles())
        {
            ulong b = f.OldestBlobFileNumber;
            if (b != FileMetaData.InvalidBlobFileNumber && (min == 0 || b < min)) min = b;
        }
        return min;
    }

    private static bool IsValid(ColumnFamilyVersion cf, CfState s)
    {
        ulong minOldest = MinOldestBlobFileNumber(cf);
        // ContainsCompleteVersion
        if (s.MissingL0.Count == 0 && s.MissingNonL0.Count == 0 && (s.MissingBlobsHigh == 0 || s.MissingBlobsHigh < minOldest)) return true;
        if (s.EditedInAtomicGroup) return false;
        // OnlyMissingL0Suffix
        if (s.MissingNonL0.Count != 0) return false;
        if (s.MissingL0.Count != 0 && !MissingL0FilesAreL0Suffix(cf, s.MissingL0)) return false;
        foreach (ulong b in s.MissingBlobs)
        {
            // RemainingSstFilesNotMissingBlobFiles
            if (minOldest != 0 && b < minOldest) continue;
            bool linked = IsLinked(cf, b, out bool onlyMissingL0, s.MissingL0);
            if (linked && s.MissingL0.Count == 0) return false;
            if (linked && !onlyMissingL0) return false;
        }
        return true;
    }

    /// <summary>MissingL0FilesAreL0Suffix: the missing L0 files are the newest ones.</summary>
    private static bool MissingL0FilesAreL0Suffix(ColumnFamilyVersion cf, HashSet<ulong> missingL0)
    {
        if (cf.NumLevels == 0) return true;
        var l0 = new List<FileMetaData>(cf.Levels[0]);
        l0.Sort(ManifestReplayer.CompareNewestFirst);
        int unaddressed = missingL0.Count;
        foreach (FileMetaData f in l0)
        {
            if (missingL0.Contains(f.FileNumber)) unaddressed--;
            else if (unaddressed != 0) return false;
            else break;
        }
        return true;
    }

    // ---- versions (VersionBuilder::SaveTo without the missing files) ---------------------------------------------

    private static ColumnFamilyVersion Snapshot(ColumnFamilyVersion cf, CfState s, bool includeFiles)
    {
        var v = new ColumnFamilyVersion
        {
            Id = cf.Id,
            Name = cf.Name,
            ComparatorName = cf.ComparatorName,
            PersistUserDefinedTimestamps = cf.PersistUserDefinedTimestamps,
            LogNumber = cf.LogNumber,
            FullHistoryTsLow = cf.FullHistoryTsLow,
        };
        if (!includeFiles) return v;
        for (int l = 0; l < cf.NumLevels; l++)
        {
            List<FileMetaData> level = v.MutableLevel(l);
            foreach (FileMetaData f in cf.Levels[l])
            {
                if (l == 0 && s.MissingL0.Contains(f.FileNumber)) continue;
                level.Add(f);
                v.FileLevels[f.FileNumber] = l;
            }
        }
        foreach (BlobFileState b in cf.BlobFiles.Values)
        {
            // AddBlobFileIfNeeded: missing blob files and blob files only referenced by missing L0 files are dropped.
            if (s.MissingBlobs.Contains(b.BlobFileNumber)) continue;
            if (IsLinked(cf, b.BlobFileNumber, out bool onlyMissingL0, s.MissingL0) && onlyMissingL0 && s.MissingL0.Count != 0) continue;
            v.BlobFiles[b.BlobFileNumber] = new BlobFileState
            {
                Addition = b.Addition,
                GarbageBlobCount = b.GarbageBlobCount,
                GarbageBlobBytes = b.GarbageBlobBytes,
            };
        }
        foreach (KeyValuePair<int, byte[]> c in cf.CompactCursors) v.CompactCursors[c.Key] = c.Value;
        return v;
    }
}
