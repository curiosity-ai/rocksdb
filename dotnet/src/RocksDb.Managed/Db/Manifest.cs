using System.Globalization;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// SEAM: yields the logical records of a MANIFEST file (log-format payloads, already reassembled from
/// fragments and checksum-verified). The production implementation is <see cref="ManifestLogReader"/>, an adapter
/// over the WAL <c>Wal.LogReader</c> (db/log_reader.cc; the MANIFEST uses the same 32KiB block log format).
/// </summary>
public interface IManifestRecordReader
{
    /// <summary>
    /// Reads the next record. Returns false at end of file. The span is only valid until the next call.
    /// Corruption in the log framing is reported by throwing <see cref="RocksDbException"/>.
    /// </summary>
    bool TryReadRecord(out ReadOnlySpan<byte> record);
}

/// <summary>
/// SEAM: accepts MANIFEST records and frames them in the log format. The production implementation is
/// <see cref="ManifestLogWriter"/>, an adapter over <c>Wal.LogWriter</c> (db/log_writer.cc) writing to an <see cref="IWritableFile"/>.
/// </summary>
public interface IManifestRecordWriter
{
    /// <summary>Appends one record. The caller may reuse <paramref name="record"/>'s buffer after completion.</summary>
    ValueTask AddRecordAsync(ReadOnlyMemory<byte> record, CancellationToken cancellationToken);
    /// <summary>Flushes and fsyncs the underlying file (SyncManifest).</summary>
    ValueTask SyncAsync(CancellationToken cancellationToken);
}

/// <summary>Live blob file state after replay (subset of BlobFileMetaData, db/blob/blob_file_meta.h).</summary>
public sealed class BlobFileState
{
    public required BlobFileAddition Addition { get; init; }
    public ulong GarbageBlobCount { get; set; }
    public ulong GarbageBlobBytes { get; set; }
    public ulong BlobFileNumber => Addition.BlobFileNumber;
}

/// <summary>Per column family state recovered from a MANIFEST (the "current Version" of that CF).</summary>
public sealed class ColumnFamilyVersion
{
    internal readonly Dictionary<ulong, int> FileLevels = new();
    private readonly List<List<FileMetaData>> _levels = new();

    public uint Id { get; init; }
    public required string Name { get; init; }
    public string? ComparatorName { get; set; }
    /// <summary>Recorded persist_user_defined_timestamps flag (true when never recorded).</summary>
    public bool PersistUserDefinedTimestamps { get; set; } = true;
    /// <summary>WALs with numbers below this have been flushed for this column family.</summary>
    public ulong LogNumber { get; set; }
    public byte[]? FullHistoryTsLow { get; set; }
    /// <summary>Live files per level; L0 newest first, other levels ordered by smallest key.</summary>
    public IReadOnlyList<IReadOnlyList<FileMetaData>> Levels => _levels;
    public SortedDictionary<ulong, BlobFileState> BlobFiles { get; } = new();
    /// <summary>Round-robin compaction cursors by level.</summary>
    public SortedDictionary<int, byte[]> CompactCursors { get; } = new();

    public int NumLevels => _levels.Count;

    internal List<FileMetaData> MutableLevel(int level)
    {
        while (_levels.Count <= level) _levels.Add(new List<FileMetaData>());
        return _levels[level];
    }

    /// <summary>Enumerates all live files as (level, file).</summary>
    public IEnumerable<(int Level, FileMetaData File)> AllFiles()
    {
        for (int l = 0; l < _levels.Count; l++)
        {
            foreach (FileMetaData f in _levels[l]) yield return (l, f);
        }
    }
}

/// <summary>
/// Everything needed to open a DB, recovered from the MANIFEST: the DB-wide counters plus per-CF live files.
/// Port of the state VersionSet::Recover / VersionEditHandler extract (db/version_set.cc,
/// db/version_edit_handler.cc), without refcounted Version objects.
/// </summary>
public sealed class RecoveredVersionState
{
    public string? DbId { get; set; }
    /// <summary>Log number from the last edit that recorded one (VersionEditParams::GetLogNumber).</summary>
    public ulong LogNumber { get; set; }
    public ulong PrevLogNumber { get; set; }
    /// <summary>The next_file_number recorded in the MANIFEST (VersionSet adds 1 after recovery).</summary>
    public ulong NextFileNumber { get; set; }
    public ulong LastSequence { get; set; }
    public uint MaxColumnFamily { get; set; }
    public ulong MinLogNumberToKeep { get; set; }
    public ulong? CommittedOptionsFileNumber { get; set; }
    public ulong? LastCompactedManifestFileSize { get; set; }
    /// <summary>Live column families by id.</summary>
    public SortedDictionary<uint, ColumnFamilyVersion> ColumnFamilies { get; } = new();
    /// <summary>Tracked WALs (track_and_verify_wals_in_manifest) with their synced size, if known.</summary>
    public SortedDictionary<ulong, ulong?> Wals { get; } = new();
    public ulong MinWalNumberToKeep { get; set; }

    public ColumnFamilyVersion? FindColumnFamily(string name)
    {
        foreach (ColumnFamilyVersion cf in ColumnFamilies.Values)
        {
            if (cf.Name == name) return cf;
        }
        return null;
    }

    /// <summary>Smallest per-CF log number: WAL replay must start at this WAL.</summary>
    public ulong MinLogNumberWithUnflushedData
    {
        get
        {
            ulong min = ulong.MaxValue;
            foreach (ColumnFamilyVersion cf in ColumnFamilies.Values) min = Math.Min(min, cf.LogNumber);
            return min == ulong.MaxValue ? 0 : min;
        }
    }
}

/// <summary>
/// "VersionBuilder-lite": replays decoded <see cref="VersionEdit"/>s into a <see cref="RecoveredVersionState"/>.
/// Ports the semantics of VersionEditHandler::ApplyVersionEdit / ExtractInfoFromVersionEdit /
/// CheckIterationResult (db/version_edit_handler.cc), AtomicGroupReadBuffer (db/version_set.cc), WalSet
/// (db/wal_edit.cc) and the add/delete checks of VersionBuilder::Rep::Apply (db/version_builder.cc).
/// </summary>
public sealed class ManifestReplayer
{
    private readonly RecoveredVersionState _state = new();
    private readonly Func<string, Comparator?>? _comparatorResolver;
    private readonly List<VersionEdit> _atomicGroup = new();
    private int _atomicGroupSize;
    private bool _hasLogNumber, _hasNextFile, _hasLastSequence;

    /// <param name="comparatorResolver">Maps a comparator name to an implementation used to order non-L0 files;
    /// null or an unknown name falls back to bytewise ordering.</param>
    public ManifestReplayer(Func<string, Comparator?>? comparatorResolver)
    {
        _comparatorResolver = comparatorResolver;
        // VersionEditHandler::Initialize: the default column family always exists.
        _state.ColumnFamilies[0] = new ColumnFamilyVersion { Id = 0, Name = "default" };
    }

    public int EditsApplied { get; private set; }

    /// <summary>Applies one decoded edit (buffering members of an atomic group until it is complete).</summary>
    public void Apply(VersionEdit edit)
    {
        // AtomicGroupReadBuffer::AddEdit
        if (edit.IsInAtomicGroup)
        {
            if (_atomicGroup.Count == 0) _atomicGroupSize = checked((int)edit.RemainingEntries + 1);
            _atomicGroup.Add(edit);
            if (_atomicGroup.Count + edit.RemainingEntries != _atomicGroupSize) ThrowHelper.Corruption("corrupted atomic group");
            if (_atomicGroup.Count == _atomicGroupSize)
            {
                foreach (VersionEdit e in _atomicGroup) ApplyVersionEdit(e);
                _atomicGroup.Clear();
                _atomicGroupSize = 0;
            }
            return;
        }
        if (_atomicGroup.Count != 0) ThrowHelper.Corruption("corrupted atomic group");
        ApplyVersionEdit(edit);
    }

    /// <summary>
    /// Port of CheckIterationResult: requires log number, next file number and last sequence to have been seen.
    /// An incomplete trailing atomic group is discarded. Returns the recovered state.
    /// </summary>
    public RecoveredVersionState Finish()
    {
        _atomicGroup.Clear();
        if (!_hasLogNumber || !_hasNextFile || !_hasLastSequence)
        {
            var msg = new System.Text.StringBuilder("no ");
            if (!_hasLogNumber) msg.Append("log_file_number, ");
            if (!_hasNextFile) msg.Append("next_file_number, ");
            if (!_hasLastSequence) msg.Append("last_sequence, ");
            msg.Length -= 2;
            msg.Append(" entry in MANIFEST");
            ThrowHelper.Corruption(msg.ToString());
        }
        foreach (ColumnFamilyVersion cf in _state.ColumnFamilies.Values)
        {
            SortLevels(cf);
            // Drop blob files that are fully garbage and no longer referenced by any SST.
            List<ulong>? obsolete = null;
            foreach (BlobFileState b in cf.BlobFiles.Values)
            {
                if (b.GarbageBlobCount >= b.Addition.TotalBlobCount && !IsBlobFileReferenced(cf, b.BlobFileNumber)) (obsolete ??= new()).Add(b.BlobFileNumber);
            }
            if (obsolete is not null) foreach (ulong n in obsolete) cf.BlobFiles.Remove(n);
        }
        return _state;
    }

    private static bool IsBlobFileReferenced(ColumnFamilyVersion cf, ulong blobFileNumber)
    {
        foreach ((_, FileMetaData f) in cf.AllFiles())
        {
            if (f.OldestBlobFileNumber == blobFileNumber) return true;
        }
        return false;
    }

    private void ApplyVersionEdit(VersionEdit edit)
    {
        ColumnFamilyVersion? cf = null;
        if (edit.IsColumnFamilyAdd)
        {
            if (_state.ColumnFamilies.ContainsKey(edit.ColumnFamily))
            {
                ThrowHelper.Corruption("MANIFEST adding the same column family twice: " + edit.ColumnFamilyAddName);
            }
            cf = new ColumnFamilyVersion { Id = edit.ColumnFamily, Name = edit.ColumnFamilyAddName! };
            _state.ColumnFamilies[edit.ColumnFamily] = cf;
        }
        else if (edit.IsColumnFamilyDrop)
        {
            if (!_state.ColumnFamilies.Remove(edit.ColumnFamily)) ThrowHelper.Corruption("MANIFEST - dropping non-existing column family");
        }
        else if (edit.IsWalAddition)
        {
            foreach (WalAddition w in edit.WalAdditions) AddWal(w);
        }
        else if (edit.IsWalDeletion)
        {
            DeleteWalsBefore(edit.WalDeletionBefore);
        }
        else
        {
            if (!_state.ColumnFamilies.TryGetValue(edit.ColumnFamily, out cf))
            {
                ThrowHelper.Corruption("MANIFEST record referencing unknown column family");
            }
            ApplyFileChanges(cf, edit, RecordedTimestampSize(cf));
        }
        ExtractInfo(cf, edit);
        EditsApplied++;
    }

    private void ExtractInfo(ColumnFamilyVersion? cf, VersionEdit edit)
    {
        if (edit.DbId is not null) _state.DbId = edit.DbId;
        if (cf is not null)
        {
            if (edit.LogNumber is ulong ln)
            {
                // Log numbers must be monotonically increasing; a smaller one is ignored (with a warning in C++).
                if (cf.LogNumber <= ln)
                {
                    cf.LogNumber = ln;
                    _state.LogNumber = ln;
                    _hasLogNumber = true;
                }
            }
            if (edit.ComparatorName is not null)
            {
                cf.ComparatorName = edit.ComparatorName;
                cf.PersistUserDefinedTimestamps = edit.PersistUserDefinedTimestamps ?? true;
            }
            if (edit.FullHistoryTsLow is { Length: > 0 } ts) cf.FullHistoryTsLow = ts;
        }
        if (edit.PrevLogNumber is ulong prev) _state.PrevLogNumber = prev;
        if (edit.NextFileNumber is ulong next)
        {
            _state.NextFileNumber = next;
            _hasNextFile = true;
        }
        if (edit.MaxColumnFamily is uint maxCf) _state.MaxColumnFamily = maxCf;
        if (edit.MinLogNumberToKeep is ulong minLog) _state.MinLogNumberToKeep = Math.Max(_state.MinLogNumberToKeep, minLog);
        if (edit.LastSequence is ulong seq)
        {
            _state.LastSequence = seq;
            _hasLastSequence = true;
        }
        if (edit.LastCompactedManifestFileSize is ulong lcm) _state.LastCompactedManifestFileSize = lcm;
        if (edit.CommittedOptionsFileNumber is ulong opts) _state.CommittedOptionsFileNumber = opts;
    }

    // WalSet::AddWal
    private void AddWal(WalAddition wal)
    {
        if (wal.LogNumber < _state.MinWalNumberToKeep) return; // already obsolete
        if (!_state.Wals.TryGetValue(wal.LogNumber, out ulong? existing))
        {
            _state.Wals[wal.LogNumber] = wal.SyncedSizeInBytes;
            return;
        }
        if (wal.SyncedSizeInBytes is not ulong size)
        {
            ThrowHelper.Corruption("WalSet::AddWal: WAL " + wal.LogNumber.ToString(CultureInfo.InvariantCulture) + " is created more than once");
            return;
        }
        if (existing is ulong old && size <= old) return; // out-of-order sync records
        _state.Wals[wal.LogNumber] = size;
    }

    // WalSet::DeleteWalsBefore
    private void DeleteWalsBefore(ulong number)
    {
        if (number <= _state.MinWalNumberToKeep) return;
        _state.MinWalNumberToKeep = number;
        List<ulong>? remove = null;
        foreach (ulong n in _state.Wals.Keys)
        {
            if (n >= number) break;
            (remove ??= new()).Add(n);
        }
        if (remove is not null) foreach (ulong n in remove) _state.Wals.Remove(n);
    }

    /// <summary>Timestamp size of the comparator recorded for <paramref name="cf"/> (0 when unknown or without timestamps).</summary>
    private int RecordedTimestampSize(ColumnFamilyVersion cf) =>
        cf.ComparatorName is not null && _comparatorResolver?.Invoke(cf.ComparatorName) is { } c ? c.TimestampSize : 0;

    /// <summary>
    /// Port of VersionEditHandler::MaybeHandleFileBoundariesForNewFiles: boundaries of files written without persisted
    /// timestamps are stored without them; they get the minimum timestamp back (the maximum one for a range tombstone
    /// sentinel largest key). All new files of one edit must agree on user_defined_timestamps_persisted.
    /// </summary>
    internal static void PadFileBoundaries(FileMetaData meta, int tsSize)
    {
        meta.Smallest = PadInternalKey(meta.Smallest, tsSize, 0);
        ulong largestFooter = meta.Largest.Length >= DbFormat.InternalKeyFooterSize ? DbFormat.ExtractFooter(meta.Largest) : 0;
        bool sentinel = largestFooter == DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, EntryType.RangeDeletion);
        meta.Largest = PadInternalKey(meta.Largest, tsSize, sentinel ? (byte)0xff : (byte)0);
    }

    /// <summary>Reverse of <see cref="PadFileBoundaries"/> (UDT disabled on a column family whose files were written without timestamps).</summary>
    internal static void StripFileBoundaries(FileMetaData meta, int tsSize)
    {
        meta.Smallest = StripInternalKey(meta.Smallest, tsSize);
        meta.Largest = StripInternalKey(meta.Largest, tsSize);
    }

    /// <summary>PadInternalKeyWithMinTimestamp / PadInternalKeyWithMaxTimestamp.</summary>
    private static byte[] PadInternalKey(byte[] key, int tsSize, byte fill)
    {
        if (key.Length < DbFormat.InternalKeyFooterSize) ThrowHelper.Corruption("bad file boundary key in MANIFEST");
        int userLen = key.Length - DbFormat.InternalKeyFooterSize;
        byte[] r = new byte[key.Length + tsSize];
        key.AsSpan(0, userLen).CopyTo(r);
        r.AsSpan(userLen, tsSize).Fill(fill);
        key.AsSpan(userLen).CopyTo(r.AsSpan(userLen + tsSize));
        return r;
    }

    private static byte[] StripInternalKey(byte[] key, int tsSize)
    {
        if (key.Length < DbFormat.InternalKeyFooterSize + tsSize) ThrowHelper.Corruption("bad file boundary key in MANIFEST");
        int userLen = key.Length - DbFormat.InternalKeyFooterSize - tsSize;
        byte[] r = new byte[key.Length - tsSize];
        key.AsSpan(0, userLen).CopyTo(r);
        key.AsSpan(key.Length - DbFormat.InternalKeyFooterSize).CopyTo(r.AsSpan(userLen));
        return r;
    }

    private static void ApplyFileChanges(ColumnFamilyVersion cf, VersionEdit edit, int recordedTimestampSize)
    {
        if (recordedTimestampSize > 0 && edit.NewFiles.Count > 0)
        {
            bool needsHandling = false;
            foreach (NewFileEntry nf in edit.NewFiles)
            {
                if (nf.Meta.UserDefinedTimestampsPersisted)
                {
                    if (needsHandling) ThrowHelper.Corruption("New files in one VersionEdit has different user_defined_timestamps_persisted value.");
                    break;
                }
                needsHandling = true;
                PadFileBoundaries(nf.Meta, recordedTimestampSize);
            }
        }
        foreach (BlobFileAddition a in edit.BlobFileAdditions)
        {
            if (cf.BlobFiles.ContainsKey(a.BlobFileNumber))
            {
                ThrowHelper.Corruption("VersionBuilder: Blob file #" + a.BlobFileNumber.ToString(CultureInfo.InvariantCulture) + " already added");
            }
            cf.BlobFiles[a.BlobFileNumber] = new BlobFileState { Addition = a };
        }
        foreach (BlobFileGarbage g in edit.BlobFileGarbages)
        {
            if (!cf.BlobFiles.TryGetValue(g.BlobFileNumber, out BlobFileState? b))
            {
                ThrowHelper.Corruption("VersionBuilder: Blob file #" + g.BlobFileNumber.ToString(CultureInfo.InvariantCulture) + " not found");
                return;
            }
            b.GarbageBlobCount += g.GarbageBlobCount;
            b.GarbageBlobBytes += g.GarbageBlobBytes;
            if (b.GarbageBlobCount > b.Addition.TotalBlobCount || b.GarbageBlobBytes > b.Addition.TotalBlobBytes)
            {
                ThrowHelper.Corruption("VersionBuilder: Garbage overflow for blob file #" + g.BlobFileNumber.ToString(CultureInfo.InvariantCulture));
            }
        }
        foreach ((int level, ulong number) in edit.DeletedFiles)
        {
            if (!cf.FileLevels.TryGetValue(number, out int current) || current != level)
            {
                string where = cf.FileLevels.TryGetValue(number, out current)
                    ? "on level " + current.ToString(CultureInfo.InvariantCulture)
                    : "not in the LSM tree";
                ThrowHelper.Corruption("VersionBuilder: Cannot delete table file #" + number.ToString(CultureInfo.InvariantCulture) + " from level " +
                                       level.ToString(CultureInfo.InvariantCulture) + " since it is " + where);
            }
            List<FileMetaData> files = cf.MutableLevel(level);
            for (int i = 0; i < files.Count; i++)
            {
                if (files[i].FileNumber == number)
                {
                    files.RemoveAt(i);
                    break;
                }
            }
            cf.FileLevels.Remove(number);
        }
        foreach (NewFileEntry nf in edit.NewFiles)
        {
            ulong number = nf.Meta.FileNumber;
            if (cf.FileLevels.TryGetValue(number, out int current))
            {
                ThrowHelper.Corruption("VersionBuilder: Cannot add table file #" + number.ToString(CultureInfo.InvariantCulture) + " to level " +
                                       nf.Level.ToString(CultureInfo.InvariantCulture) + " since it is already in the LSM tree on level " +
                                       current.ToString(CultureInfo.InvariantCulture));
            }
            if (nf.Level < 0) ThrowHelper.Corruption("VersionBuilder: invalid level for table file #" + number.ToString(CultureInfo.InvariantCulture));
            cf.MutableLevel(nf.Level).Add(nf.Meta);
            cf.FileLevels[number] = nf.Level;
        }
        foreach (CompactCursor c in edit.CompactCursors) cf.CompactCursors[c.Level] = c.InternalKey;
    }

    private void SortLevels(ColumnFamilyVersion cf)
    {
        Comparator ucmp = (cf.ComparatorName is not null ? _comparatorResolver?.Invoke(cf.ComparatorName) : null) ?? Comparator.Bytewise;
        for (int level = 0; level < cf.NumLevels; level++)
        {
            List<FileMetaData> files = cf.MutableLevel(level);
            if (level == 0)
            {
                files.Sort(static (a, b) => CompareNewestFirst(a, b)); // NewestFirstByEpochNumber
            }
            else
            {
                files.Sort((a, b) =>
                {
                    int r = CompareInternalKeys(ucmp, a.Smallest, b.Smallest);
                    return r != 0 ? r : a.FileNumber.CompareTo(b.FileNumber);
                });
            }
        }
    }

    private static int CompareNewestFirst(FileMetaData a, FileMetaData b)
    {
        if (a.EpochNumber != b.EpochNumber) return b.EpochNumber.CompareTo(a.EpochNumber);
        if (a.LargestSeqno != b.LargestSeqno) return b.LargestSeqno.CompareTo(a.LargestSeqno);
        if (a.SmallestSeqno != b.SmallestSeqno) return b.SmallestSeqno.CompareTo(a.SmallestSeqno);
        return b.FileNumber.CompareTo(a.FileNumber);
    }

    /// <summary>InternalKeyComparator::Compare: user key ascending, then packed (seq, type) descending.</summary>
    private static int CompareInternalKeys(Comparator ucmp, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int r = ucmp.Compare(DbFormat.ExtractUserKey(a), DbFormat.ExtractUserKey(b));
        if (r != 0) return r;
        return DbFormat.ExtractFooter(b).CompareTo(DbFormat.ExtractFooter(a));
    }
}

/// <summary>
/// Reads a MANIFEST: CURRENT → MANIFEST-N → decoded <see cref="VersionEdit"/>s → <see cref="RecoveredVersionState"/>
/// (port of the VersionSet::Recover flow in db/version_set.cc).
/// </summary>
public static class ManifestReader
{
    /// <summary>Decodes and replays every record yielded by <paramref name="records"/>.</summary>
    public static RecoveredVersionState Recover(IManifestRecordReader records, Func<string, Comparator?>? comparatorResolver)
    {
        var replayer = new ManifestReplayer(comparatorResolver);
        while (records.TryReadRecord(out ReadOnlySpan<byte> record))
        {
            replayer.Apply(VersionEdit.DecodeFrom(record));
        }
        return replayer.Finish();
    }

    /// <summary>
    /// Reads CURRENT in <paramref name="dbname"/>, opens the MANIFEST it names and recovers the state, reading the log
    /// records with <see cref="ManifestLogReader"/> (the WAL <c>LogReader</c>).
    /// </summary>
    public static RecoveredVersionState RecoverFromDirectory(IFileSystem fs, string dbname, Func<string, Comparator?>? comparatorResolver,
        out ulong manifestFileNumber)
    {
        string path = FileNames.GetCurrentManifestPath(fs, dbname, out manifestFileNumber);
        using ManifestLogReader reader = ManifestLogReader.Open(fs, path);
        return Recover(reader, comparatorResolver);
    }

    /// <summary>Decodes all edits of a MANIFEST without replaying them (diagnostics, ldb manifest_dump).</summary>
    public static List<VersionEdit> ReadAllEdits(IManifestRecordReader records)
    {
        var edits = new List<VersionEdit>();
        while (records.TryReadRecord(out ReadOnlySpan<byte> record)) edits.Add(VersionEdit.DecodeFrom(record));
        return edits;
    }
}

/// <summary>
/// Writes VersionEdits to a MANIFEST through an <see cref="IManifestRecordWriter"/>. Ports the record layout of
/// VersionSet::WriteCurrentStateToManifest and the per-edit append of VersionSet::LogAndApply (db/version_set.cc).
/// Not thread-safe: the caller (version set) serializes manifest writes like the C++ manifest writer queue.
/// </summary>
public sealed class ManifestWriter
{
    private readonly IManifestRecordWriter _sink;
    private readonly PooledBufferWriter _buffer = new(1024);

    public ManifestWriter(IManifestRecordWriter sink) => _sink = sink;

    /// <summary>Encodes and appends one edit (optionally syncing).</summary>
    public async ValueTask AddEditAsync(VersionEdit edit, int timestampSize, bool sync, CancellationToken cancellationToken)
    {
        await AddRecordAsync(edit, timestampSize, cancellationToken).ConfigureAwait(false);
        if (sync) await _sink.SyncAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Appends edits as one atomic group (kInAtomicGroup with descending remaining-entry counts), then syncs.</summary>
    public async ValueTask AddAtomicGroupAsync(IReadOnlyList<VersionEdit> edits, int timestampSize, CancellationToken cancellationToken)
    {
        for (int i = 0; i < edits.Count; i++)
        {
            edits[i].MarkAtomicGroup((uint)(edits.Count - 1 - i));
            await AddRecordAsync(edits[i], timestampSize, cancellationToken).ConfigureAwait(false);
        }
        await _sink.SyncAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a full snapshot of <paramref name="state"/> as the first records of a new MANIFEST, in the same order as
    /// WriteCurrentStateToManifest (DB id, WAL additions, WAL deletion, then per CF a descriptor edit and a files edit,
    /// then the committed OPTIONS file number), followed by the LogAndApply-style edit carrying next_file_number and
    /// max_column_family. Syncs at the end.
    /// </summary>
    public async ValueTask WriteSnapshotAsync(RecoveredVersionState state, Func<ColumnFamilyVersion, int> timestampSize, CancellationToken cancellationToken)
    {
        if (state.DbId is not null)
        {
            await AddRecordAsync(new VersionEdit { DbId = state.DbId }, 0, cancellationToken).ConfigureAwait(false);
        }
        if (state.Wals.Count > 0)
        {
            var wals = new VersionEdit();
            foreach (KeyValuePair<ulong, ulong?> w in state.Wals) wals.WalAdditions.Add(new WalAddition(w.Key, w.Value));
            await AddRecordAsync(wals, 0, cancellationToken).ConfigureAwait(false);
        }
        // Always written (possibly empty when min_log_number_to_keep is 0), like the C++ code.
        await AddRecordAsync(new VersionEdit { WalDeletionBefore = state.MinLogNumberToKeep }, 0, cancellationToken).ConfigureAwait(false);

        foreach (ColumnFamilyVersion cf in state.ColumnFamilies.Values)
        {
            var desc = new VersionEdit();
            if (cf.Id != 0)
            {
                desc.AddColumnFamily(cf.Name);
                desc.ColumnFamily = cf.Id;
            }
            desc.ComparatorName = cf.ComparatorName ?? Comparator.Bytewise.Name;
            desc.PersistUserDefinedTimestamps = cf.PersistUserDefinedTimestamps;
            await AddRecordAsync(desc, 0, cancellationToken).ConfigureAwait(false);

            var files = new VersionEdit { ColumnFamily = cf.Id };
            foreach ((int level, FileMetaData f) in cf.AllFiles()) files.NewFiles.Add(new NewFileEntry(level, f));
            foreach (KeyValuePair<int, byte[]> c in cf.CompactCursors) files.CompactCursors.Add(new CompactCursor(c.Key, c.Value));
            foreach (BlobFileState b in cf.BlobFiles.Values)
            {
                files.BlobFileAdditions.Add(b.Addition);
                if (b.GarbageBlobCount > 0) files.BlobFileGarbages.Add(new BlobFileGarbage(b.BlobFileNumber, b.GarbageBlobCount, b.GarbageBlobBytes));
            }
            files.LogNumber = cf.LogNumber;
            if (cf.Id == 0 && state.MinLogNumberToKeep != 0) files.MinLogNumberToKeep = state.MinLogNumberToKeep;
            if (cf.FullHistoryTsLow is { Length: > 0 }) files.FullHistoryTsLow = cf.FullHistoryTsLow;
            files.LastSequence = state.LastSequence;
            await AddRecordAsync(files, timestampSize(cf), cancellationToken).ConfigureAwait(false);
        }
        if (state.CommittedOptionsFileNumber is ulong opts)
        {
            await AddRecordAsync(new VersionEdit { CommittedOptionsFileNumber = opts }, 0, cancellationToken).ConfigureAwait(false);
        }
        var tail = new VersionEdit { NextFileNumber = state.NextFileNumber, LastSequence = state.LastSequence, MaxColumnFamily = state.MaxColumnFamily };
        if (state.PrevLogNumber != 0) tail.PrevLogNumber = state.PrevLogNumber;
        await AddRecordAsync(tail, 0, cancellationToken).ConfigureAwait(false);
        await _sink.SyncAsync(cancellationToken).ConfigureAwait(false);
    }

    private ValueTask AddRecordAsync(VersionEdit edit, int timestampSize, CancellationToken cancellationToken)
    {
        _buffer.Clear();
        if (!edit.TryEncodeTo(_buffer, timestampSize)) ThrowHelper.Corruption("Unable to Encode VersionEdit:" + edit);
        return _sink.AddRecordAsync(_buffer.WrittenMemory, cancellationToken);
    }
}
