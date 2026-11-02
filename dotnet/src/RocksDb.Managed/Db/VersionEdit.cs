using System.Buffers;
using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// Tag numbers for serialized VersionEdit (db/version_edit.h <c>enum Tag</c>). These are written to disk and
/// must never change. Tags above <see cref="TagSafeIgnoreMask"/> are forward compatible: an unknown one is
/// followed by a varint32 length and can be skipped.
/// </summary>
public enum VersionEditTag : uint
{
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactCursor = 5,
    DeletedFile = 6,
    NewFile = 7,
    // 8 was used for large value refs
    PrevLogNumber = 9,
    MinLogNumberToKeep = 10,

    NewFile2 = 100,
    NewFile3 = 102,
    NewFile4 = 103,
    ColumnFamily = 200,
    ColumnFamilyAdd = 201,
    ColumnFamilyDrop = 202,
    MaxColumnFamily = 203,

    InAtomicGroup = 300,

    BlobFileAddition = 400,
    BlobFileGarbage = 401,

    /// <summary>kTagSafeIgnoreMask = 1 &lt;&lt; 13.</summary>
    TagSafeIgnoreMask = 1 << 13,

    // Forward compatible (aka ignorable) records: consecutive values after the mask.
    DbId = TagSafeIgnoreMask + 1,
    BlobFileAdditionDeprecated = TagSafeIgnoreMask + 2,
    BlobFileGarbageDeprecated = TagSafeIgnoreMask + 3,
    WalAddition = TagSafeIgnoreMask + 4,
    WalDeletion = TagSafeIgnoreMask + 5,
    FullHistoryTsLow = TagSafeIgnoreMask + 6,
    WalAddition2 = TagSafeIgnoreMask + 7,
    WalDeletion2 = TagSafeIgnoreMask + 8,
    PersistUserDefinedTimestamps = TagSafeIgnoreMask + 9,
    SubcompactionProgress = TagSafeIgnoreMask + 10,
    LastCompactedManifestFileSize = TagSafeIgnoreMask + 11,
    CommittedOptionsFileNumber = TagSafeIgnoreMask + 12,
}

/// <summary>Custom field tags inside a kNewFile4 record (db/version_edit.h <c>enum NewFileCustomTag</c>).</summary>
public enum NewFileCustomTag : uint
{
    Terminate = 1,
    NeedCompaction = 2,
    /// <summary>Hack to carry min_log_number_to_keep in a forward-compatible way (fixed64 payload).</summary>
    MinLogNumberToKeepHack = 3,
    OldestBlobFileNumber = 4,
    OldestAncestorTime = 5,
    FileCreationTime = 6,
    FileChecksum = 7,
    FileChecksumFuncName = 8,
    Temperature = 9,
    MinTimestamp = 10,
    MaxTimestamp = 11,
    UniqueId = 12,
    EpochNumber = 13,
    CompensatedRangeDeletionSize = 14,
    TailSize = 15,
    UserDefinedTimestampsPersisted = 16,
    FileOpenMetadata = 17,

    /// <summary>If this bit is set on an unknown custom tag, opening the DB must fail.</summary>
    CustomTagNonSafeIgnoreMask = 1 << 6,
    /// <summary>Forward incompatible: path id (1 byte).</summary>
    PathId = CustomTagNonSafeIgnoreMask + 1,
}

/// <summary>A file added to a level by a <see cref="VersionEdit"/>.</summary>
public readonly record struct NewFileEntry(int Level, FileMetaData Meta);

/// <summary>A compaction cursor (round-robin compaction priority) for a level; key is an internal key.</summary>
public readonly record struct CompactCursor(int Level, byte[] InternalKey);

/// <summary>
/// Port of VersionEdit (db/version_edit.{h,cc}): one MANIFEST record. <see cref="TryEncodeTo"/> produces
/// output byte-identical to the C++ <c>EncodeTo</c> for the same logical edit, and <see cref="DecodeFrom"/>
/// accepts everything native RocksDB writes (including legacy kNewFile/kNewFile2/kNewFile3 records and
/// forward-compatible unknown tags, which are skipped).
/// </summary>
/// <remarks>
/// kSubcompactionProgress payloads are kept opaque (<see cref="SubcompactionProgress"/>): they are only used
/// by compaction resumption and are re-encoded verbatim.
/// </remarks>
public sealed class VersionEdit
{
    private readonly SortedSet<(int Level, ulong FileNumber)> _deletedFiles = new();

    public string? DbId { get; set; }
    /// <summary>User comparator name; native always writes <see cref="PersistUserDefinedTimestamps"/> alongside.</summary>
    public string? ComparatorName { get; set; }
    public bool? PersistUserDefinedTimestamps { get; set; }
    public ulong? LogNumber { get; set; }
    public ulong? PrevLogNumber { get; set; }
    public ulong? NextFileNumber { get; set; }
    public uint? MaxColumnFamily { get; set; }
    public ulong? MinLogNumberToKeep { get; set; }
    public ulong? LastSequence { get; set; }
    public ulong? LastCompactedManifestFileSize { get; set; }
    public ulong? CommittedOptionsFileNumber { get; set; }

    public List<CompactCursor> CompactCursors { get; } = new();
    /// <summary>Deleted files ordered by (level, file number), like the C++ std::set.</summary>
    public IReadOnlyCollection<(int Level, ulong FileNumber)> DeletedFiles => _deletedFiles;
    public List<NewFileEntry> NewFiles { get; } = new();
    public List<BlobFileAddition> BlobFileAdditions { get; } = new();
    public List<BlobFileGarbage> BlobFileGarbages { get; } = new();
    public List<WalAddition> WalAdditions { get; } = new();
    /// <summary>DeleteWalsBefore number; 0 means no WAL deletion (WalDeletion::kEmpty).</summary>
    public ulong WalDeletionBefore { get; set; }

    /// <summary>Column family id the edit applies to (0 = default, not written).</summary>
    public uint ColumnFamily { get; set; }
    /// <summary>Non-null when this edit adds a column family with this name.</summary>
    public string? ColumnFamilyAddName { get; set; }
    public bool IsColumnFamilyDrop { get; set; }
    public bool IsInAtomicGroup { get; set; }
    public uint RemainingEntries { get; set; }
    /// <summary>full_history_ts_low; null/empty means not set.</summary>
    public byte[]? FullHistoryTsLow { get; set; }
    /// <summary>Opaque kSubcompactionProgress payload (the length-prefixed body).</summary>
    public byte[]? SubcompactionProgress { get; set; }

    public bool IsColumnFamilyAdd => ColumnFamilyAddName is not null;
    public bool IsColumnFamilyManipulation => IsColumnFamilyAdd || IsColumnFamilyDrop;
    public bool IsWalAddition => WalAdditions.Count > 0;
    public bool IsWalDeletion => WalDeletionBefore != 0;

    public int NumEntries =>
        NewFiles.Count + _deletedFiles.Count + BlobFileAdditions.Count + BlobFileGarbages.Count + WalAdditions.Count + (IsWalDeletion ? 1 : 0);

    /// <summary>IsWalManipulation: the edit only adds WALs or only deletes WALs.</summary>
    public bool IsWalManipulation
    {
        get
        {
            int entries = NumEntries;
            return entries > 0 && (entries == WalAdditions.Count || entries == (IsWalDeletion ? 1 : 0));
        }
    }

    public void DeleteFile(int level, ulong fileNumber) => _deletedFiles.Add((level, fileNumber));

    /// <summary>AddFile: also bumps <see cref="LastSequence"/> to the file's largest seqno, like C++.</summary>
    public void AddFile(int level, FileMetaData meta)
    {
        NewFiles.Add(new NewFileEntry(level, meta));
        if (LastSequence is not ulong last || meta.LargestSeqno > last) LastSequence = meta.LargestSeqno;
    }

    public void AddColumnFamily(string name) => ColumnFamilyAddName = name;
    public void DropColumnFamily() => IsColumnFamilyDrop = true;

    public void MarkAtomicGroup(uint remainingEntries)
    {
        IsInAtomicGroup = true;
        RemainingEntries = remainingEntries;
    }

    // ---- encoding ---------------------------------------------------------------------------

    /// <summary>
    /// Port of VersionEdit::EncodeTo. <paramref name="timestampSize"/> is the user comparator timestamp size
    /// (used to strip timestamps from file boundaries when user-defined timestamps are not persisted). Returns
    /// false (writing a partial record) when a new file lacks valid boundaries or an epoch number.
    /// </summary>
    public bool TryEncodeTo(IBufferWriter<byte> dst, int timestampSize)
    {
        if (DbId is not null)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.DbId);
            PutLengthPrefixedString(dst, DbId);
        }
        if (ComparatorName is not null)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.Comparator);
            PutLengthPrefixedString(dst, ComparatorName);
        }
        if (LogNumber is ulong logNumber) PutTagVarint64(dst, VersionEditTag.LogNumber, logNumber);
        if (PrevLogNumber is ulong prevLog) PutTagVarint64(dst, VersionEditTag.PrevLogNumber, prevLog);
        if (NextFileNumber is ulong next) PutTagVarint64(dst, VersionEditTag.NextFileNumber, next);
        if (MaxColumnFamily is uint maxCf) Coding.PutVarint32Varint32(dst, (uint)VersionEditTag.MaxColumnFamily, maxCf);
        if (MinLogNumberToKeep is ulong minLog) PutTagVarint64(dst, VersionEditTag.MinLogNumberToKeep, minLog);
        if (LastSequence is ulong lastSeq) PutTagVarint64(dst, VersionEditTag.LastSequence, lastSeq);
        foreach (CompactCursor cursor in CompactCursors)
        {
            if (IsValidInternalKey(cursor.InternalKey))
            {
                Coding.PutVarint32(dst, (uint)VersionEditTag.CompactCursor);
                Coding.PutVarint32(dst, (uint)cursor.Level);
                Coding.PutLengthPrefixedSlice(dst, cursor.InternalKey);
            }
        }
        foreach ((int level, ulong number) in _deletedFiles)
        {
            Coding.PutVarint32Varint32(dst, (uint)VersionEditTag.DeletedFile, (uint)level);
            Coding.PutVarint64(dst, number);
        }

        bool minLogNumWritten = false;
        foreach (NewFileEntry nf in NewFiles)
        {
            FileMetaData f = nf.Meta;
            if (!IsValidInternalKey(f.Smallest) || !IsValidInternalKey(f.Largest) || f.EpochNumber == FileMetaData.UnknownEpochNumber)
            {
                return false;
            }
            EncodeToNewFile4(f, nf.Level, timestampSize, MinLogNumberToKeep.HasValue, MinLogNumberToKeep.GetValueOrDefault(), ref minLogNumWritten, dst);
        }

        foreach (BlobFileAddition a in BlobFileAdditions)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.BlobFileAddition);
            a.EncodeTo(dst);
        }
        foreach (BlobFileGarbage g in BlobFileGarbages)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.BlobFileGarbage);
            g.EncodeTo(dst);
        }

        if (WalAdditions.Count > 0)
        {
            using var tmp = new PooledBufferWriter(64);
            foreach (WalAddition w in WalAdditions)
            {
                Coding.PutVarint32(dst, (uint)VersionEditTag.WalAddition2);
                tmp.Clear();
                w.EncodeTo(tmp);
                Coding.PutLengthPrefixedSlice(dst, tmp.WrittenSpan);
            }
        }
        if (IsWalDeletion)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.WalDeletion2);
            PutLengthPrefixedVarint64(dst, WalDeletionBefore);
        }

        if (ColumnFamily != 0) Coding.PutVarint32Varint32(dst, (uint)VersionEditTag.ColumnFamily, ColumnFamily);
        if (ColumnFamilyAddName is not null)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.ColumnFamilyAdd);
            PutLengthPrefixedString(dst, ColumnFamilyAddName);
        }
        if (IsColumnFamilyDrop) Coding.PutVarint32(dst, (uint)VersionEditTag.ColumnFamilyDrop);
        if (IsInAtomicGroup) Coding.PutVarint32Varint32(dst, (uint)VersionEditTag.InAtomicGroup, RemainingEntries);
        if (FullHistoryTsLow is { Length: > 0 } tsLow)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.FullHistoryTsLow);
            Coding.PutLengthPrefixedSlice(dst, tsLow);
        }
        if (PersistUserDefinedTimestamps is bool persist)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.PersistUserDefinedTimestamps);
            Span<byte> p = [persist ? (byte)1 : (byte)0];
            Coding.PutLengthPrefixedSlice(dst, p);
        }
        if (SubcompactionProgress is not null)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.SubcompactionProgress);
            Coding.PutLengthPrefixedSlice(dst, SubcompactionProgress);
        }
        if (LastCompactedManifestFileSize is ulong lcm)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.LastCompactedManifestFileSize);
            PutLengthPrefixedVarint64(dst, lcm);
        }
        if (CommittedOptionsFileNumber is ulong opts)
        {
            Coding.PutVarint32(dst, (uint)VersionEditTag.CommittedOptionsFileNumber);
            PutLengthPrefixedVarint64(dst, opts);
        }
        return true;
    }

    /// <summary>Encodes into a new array; throws InvalidArgument if the edit cannot be encoded.</summary>
    public byte[] Encode(int timestampSize)
    {
        using var w = new PooledBufferWriter(256);
        if (!TryEncodeTo(w, timestampSize)) ThrowHelper.InvalidArgument("Unable to encode VersionEdit (invalid new file boundaries or epoch number)");
        return w.WrittenSpan.ToArray();
    }

    /// <summary>Port of VersionEdit::EncodeToNewFile4.</summary>
    public static void EncodeToNewFile4(FileMetaData f, int level, int timestampSize, bool hasMinLogNumberToKeep, ulong minLogNumberToKeep,
        ref bool minLogNumWritten, IBufferWriter<byte> dst)
    {
        Coding.PutVarint32(dst, (uint)VersionEditTag.NewFile4);
        Coding.PutVarint32(dst, (uint)level);
        Coding.PutVarint64(dst, f.FileNumber);
        Coding.PutVarint64(dst, f.FileSize);
        EncodeFileBoundaries(dst, f, timestampSize);
        Coding.PutVarint64(dst, f.SmallestSeqno);
        Coding.PutVarint64(dst, f.LargestSeqno);

        // Customized fields: tag (varint32) + length-prefixed payload, terminated by kTerminate.
        PutCustomVarint64(dst, NewFileCustomTag.OldestAncestorTime, f.OldestAncestorTime);
        PutCustomVarint64(dst, NewFileCustomTag.FileCreationTime, f.FileCreationTime);
        PutCustomVarint64(dst, NewFileCustomTag.EpochNumber, f.EpochNumber);
        if (f.FileChecksumFuncName != FileMetaData.UnknownFileChecksumFuncName)
        {
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.FileChecksum);
            Coding.PutLengthPrefixedSlice(dst, f.FileChecksum);
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.FileChecksumFuncName);
            PutLengthPrefixedString(dst, f.FileChecksumFuncName);
        }
        if (f.PathId != 0) PutCustomByte(dst, NewFileCustomTag.PathId, (byte)f.PathId);
        if (f.Temperature != Temperature.Unknown) PutCustomByte(dst, NewFileCustomTag.Temperature, (byte)f.Temperature);
        if (f.MarkedForCompaction) PutCustomByte(dst, NewFileCustomTag.NeedCompaction, 1);
        if (hasMinLogNumberToKeep && !minLogNumWritten)
        {
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.MinLogNumberToKeepHack);
            Span<byte> fixed64 = stackalloc byte[8];
            Coding.EncodeFixed64(fixed64, minLogNumberToKeep);
            Coding.PutLengthPrefixedSlice(dst, fixed64);
            minLogNumWritten = true;
        }
        if (f.OldestBlobFileNumber != FileMetaData.InvalidBlobFileNumber)
        {
            PutCustomVarint64(dst, NewFileCustomTag.OldestBlobFileNumber, f.OldestBlobFileNumber);
        }
        if (!f.UniqueId.IsNull)
        {
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.UniqueId);
            Span<byte> id = stackalloc byte[16];
            Coding.EncodeFixed64(id, f.UniqueId.Lo);
            Coding.EncodeFixed64(id[8..], f.UniqueId.Hi);
            Coding.PutLengthPrefixedSlice(dst, id);
        }
        if (f.CompensatedRangeDeletionSize != 0)
        {
            PutCustomVarint64(dst, NewFileCustomTag.CompensatedRangeDeletionSize, f.CompensatedRangeDeletionSize);
        }
        if (f.TailSize != 0) PutCustomVarint64(dst, NewFileCustomTag.TailSize, f.TailSize);
        if (!f.UserDefinedTimestampsPersisted) PutCustomByte(dst, NewFileCustomTag.UserDefinedTimestampsPersisted, 0);
        if (f.MinTimestamp.Length > 0)
        {
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.MinTimestamp);
            Coding.PutLengthPrefixedSlice(dst, f.MinTimestamp);
        }
        if (f.MaxTimestamp.Length > 0)
        {
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.MaxTimestamp);
            Coding.PutLengthPrefixedSlice(dst, f.MaxTimestamp);
        }
        if (f.FileOpenMetadata.Length > 0)
        {
            Coding.PutVarint32(dst, (uint)NewFileCustomTag.FileOpenMetadata);
            Coding.PutLengthPrefixedSlice(dst, f.FileOpenMetadata);
        }
        Coding.PutVarint32(dst, (uint)NewFileCustomTag.Terminate);
    }

    /// <summary>Port of EncodeFileBoundaries: strips the timestamp when UDTs are enabled but not persisted.</summary>
    private static void EncodeFileBoundaries(IBufferWriter<byte> dst, FileMetaData meta, int timestampSize)
    {
        if (timestampSize == 0 || meta.UserDefinedTimestampsPersisted)
        {
            Coding.PutLengthPrefixedSlice(dst, meta.Smallest);
            Coding.PutLengthPrefixedSlice(dst, meta.Largest);
            return;
        }
        PutStrippedInternalKey(dst, meta.Smallest, timestampSize);
        PutStrippedInternalKey(dst, meta.Largest, timestampSize);
    }

    /// <summary>StripTimestampFromInternalKey + PutLengthPrefixedSlice.</summary>
    private static void PutStrippedInternalKey(IBufferWriter<byte> dst, ReadOnlySpan<byte> key, int tsSize)
    {
        int userLen = key.Length - DbFormat.InternalKeyFooterSize - tsSize;
        Coding.PutVarint32(dst, (uint)(key.Length - tsSize));
        dst.Write(key[..userLen]);
        dst.Write(key[^DbFormat.InternalKeyFooterSize..]);
    }

    private static void PutTagVarint64(IBufferWriter<byte> dst, VersionEditTag tag, ulong v)
    {
        Coding.PutVarint32(dst, (uint)tag);
        Coding.PutVarint64(dst, v);
    }

    private static void PutCustomVarint64(IBufferWriter<byte> dst, NewFileCustomTag tag, ulong v)
    {
        Coding.PutVarint32(dst, (uint)tag);
        PutLengthPrefixedVarint64(dst, v);
    }

    private static void PutCustomByte(IBufferWriter<byte> dst, NewFileCustomTag tag, byte b)
    {
        Coding.PutVarint32(dst, (uint)tag);
        Span<byte> p = [b];
        Coding.PutLengthPrefixedSlice(dst, p);
    }

    private static void PutLengthPrefixedVarint64(IBufferWriter<byte> dst, ulong v)
    {
        Span<byte> tmp = stackalloc byte[Coding.MaxVarint64Length];
        int n = Coding.EncodeVarint64(tmp, v);
        Coding.PutLengthPrefixedSlice(dst, tmp[..n]);
    }

    private static void PutLengthPrefixedString(IBufferWriter<byte> dst, string s) =>
        Coding.PutLengthPrefixedSlice(dst, Encoding.UTF8.GetBytes(s));

    // ---- decoding ---------------------------------------------------------------------------

    /// <summary>
    /// Port of VersionEdit::DecodeFrom. Throws <see cref="RocksDbException"/> with
    /// <see cref="StatusCode.Corruption"/> on malformed input.
    /// </summary>
    public static VersionEdit DecodeFrom(ReadOnlySpan<byte> src)
    {
        var edit = new VersionEdit();
        string? msg = edit.DecodeCore(src);
        if (msg is not null) ThrowHelper.Corruption(msg);
        return edit;
    }

    /// <summary>Non-throwing variant; <paramref name="error"/> carries the corruption message.</summary>
    public static bool TryDecodeFrom(ReadOnlySpan<byte> src, out VersionEdit edit, out string? error)
    {
        edit = new VersionEdit();
        error = edit.DecodeCore(src);
        return error is null;
    }

    private string? DecodeCore(ReadOnlySpan<byte> input)
    {
        const string P = "VersionEdit: ";
        while (Coding.GetVarint32(ref input, out uint tag))
        {
            switch ((VersionEditTag)tag)
            {
                case VersionEditTag.DbId:
                    if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> s)) return P + "db id";
                    DbId = Encoding.UTF8.GetString(s);
                    break;
                case VersionEditTag.Comparator:
                    if (!Coding.GetLengthPrefixedSlice(ref input, out s)) return P + "comparator name";
                    ComparatorName = Encoding.UTF8.GetString(s);
                    break;
                case VersionEditTag.LogNumber:
                    if (!Coding.GetVarint64(ref input, out ulong v)) return P + "log number";
                    LogNumber = v;
                    break;
                case VersionEditTag.PrevLogNumber:
                    if (!Coding.GetVarint64(ref input, out v)) return P + "previous log number";
                    PrevLogNumber = v;
                    break;
                case VersionEditTag.NextFileNumber:
                    if (!Coding.GetVarint64(ref input, out v)) return P + "next file number";
                    NextFileNumber = v;
                    break;
                case VersionEditTag.MaxColumnFamily:
                    if (!Coding.GetVarint32(ref input, out uint u)) return P + "max column family";
                    MaxColumnFamily = u;
                    break;
                case VersionEditTag.MinLogNumberToKeep:
                    if (!Coding.GetVarint64(ref input, out v)) return P + "min log number to kee";
                    MinLogNumberToKeep = v;
                    break;
                case VersionEditTag.LastSequence:
                    if (!Coding.GetVarint64(ref input, out v)) return P + "last sequence number";
                    LastSequence = v;
                    break;
                case VersionEditTag.CompactCursor:
                {
                    if (!GetLevel(ref input, out int level) || !GetInternalKey(ref input, out byte[] key)) return P + "compaction cursor";
                    CompactCursors.Add(new CompactCursor(level, key));
                    break;
                }
                case VersionEditTag.DeletedFile:
                {
                    if (!GetLevel(ref input, out int level) || !Coding.GetVarint64(ref input, out ulong number)) return P + "deleted file";
                    _deletedFiles.Add((level, number));
                    break;
                }
                case VersionEditTag.NewFile:
                {
                    var f = new FileMetaData();
                    if (!GetLevel(ref input, out int level) || !Coding.GetVarint64(ref input, out ulong number) ||
                        !Coding.GetVarint64(ref input, out ulong size) || !GetInternalKey(ref input, out byte[] smallest) ||
                        !GetInternalKey(ref input, out byte[] largest))
                    {
                        return P + "new-file entry";
                    }
                    f.FileNumber = number; f.FileSize = size; f.Smallest = smallest; f.Largest = largest;
                    f.SmallestSeqno = DbFormat.MaxSequenceNumber; f.LargestSeqno = 0; // FileDescriptor(number, 0, size)
                    NewFiles.Add(new NewFileEntry(level, f));
                    break;
                }
                case VersionEditTag.NewFile2:
                case VersionEditTag.NewFile3:
                {
                    bool v3 = (VersionEditTag)tag == VersionEditTag.NewFile3;
                    var f = new FileMetaData();
                    uint pathId = 0;
                    if (!GetLevel(ref input, out int level) || !Coding.GetVarint64(ref input, out ulong number) ||
                        (v3 && !Coding.GetVarint32(ref input, out pathId)) ||
                        !Coding.GetVarint64(ref input, out ulong size) || !GetInternalKey(ref input, out byte[] smallest) ||
                        !GetInternalKey(ref input, out byte[] largest) || !Coding.GetVarint64(ref input, out ulong sseq) ||
                        !Coding.GetVarint64(ref input, out ulong lseq))
                    {
                        return P + (v3 ? "new-file3 entry" : "new-file2 entry");
                    }
                    f.FileNumber = number; f.PathId = pathId; f.FileSize = size; f.Smallest = smallest; f.Largest = largest;
                    f.SmallestSeqno = sseq; f.LargestSeqno = lseq;
                    NewFiles.Add(new NewFileEntry(level, f));
                    break;
                }
                case VersionEditTag.NewFile4:
                {
                    string? err = DecodeNewFile4From(ref input, out int level, out FileMetaData f, out bool hasMinLog, out ulong minLog);
                    if (err is not null) return P + err;
                    if (hasMinLog) MinLogNumberToKeep = minLog;
                    NewFiles.Add(new NewFileEntry(level, f));
                    break;
                }
                case VersionEditTag.BlobFileAddition:
                case VersionEditTag.BlobFileAdditionDeprecated:
                {
                    var a = new BlobFileAddition();
                    string? err = a.DecodeFrom(ref input);
                    if (err is not null) return "BlobFileAddition: " + err;
                    BlobFileAdditions.Add(a);
                    break;
                }
                case VersionEditTag.BlobFileGarbage:
                case VersionEditTag.BlobFileGarbageDeprecated:
                {
                    var g = new BlobFileGarbage();
                    string? err = g.DecodeFrom(ref input);
                    if (err is not null) return "BlobFileGarbage: " + err;
                    BlobFileGarbages.Add(g);
                    break;
                }
                case VersionEditTag.WalAddition:
                {
                    var w = new WalAddition();
                    string? err = w.DecodeFrom(ref input);
                    if (err is not null) return err;
                    WalAdditions.Add(w);
                    break;
                }
                case VersionEditTag.WalAddition2:
                {
                    if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> encoded)) return P + "WalAddition not prefixed by length";
                    var w = new WalAddition();
                    string? err = w.DecodeFrom(ref encoded);
                    if (err is not null) return err;
                    WalAdditions.Add(w);
                    break;
                }
                case VersionEditTag.WalDeletion:
                    if (!Coding.GetVarint64(ref input, out v)) return "WalDeletion: Error decoding WAL log number";
                    WalDeletionBefore = v;
                    break;
                case VersionEditTag.WalDeletion2:
                {
                    if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> encoded)) return P + "WalDeletion not prefixed by length";
                    if (!Coding.GetVarint64(ref encoded, out v)) return "WalDeletion: Error decoding WAL log number";
                    WalDeletionBefore = v;
                    break;
                }
                case VersionEditTag.ColumnFamily:
                    if (!Coding.GetVarint32(ref input, out u)) return P + "set column family id";
                    ColumnFamily = u;
                    break;
                case VersionEditTag.ColumnFamilyAdd:
                    if (!Coding.GetLengthPrefixedSlice(ref input, out s)) return P + "column family add";
                    ColumnFamilyAddName = Encoding.UTF8.GetString(s);
                    break;
                case VersionEditTag.ColumnFamilyDrop:
                    IsColumnFamilyDrop = true;
                    break;
                case VersionEditTag.InAtomicGroup:
                    IsInAtomicGroup = true;
                    if (!Coding.GetVarint32(ref input, out u)) return P + "remaining entries";
                    RemainingEntries = u;
                    break;
                case VersionEditTag.FullHistoryTsLow:
                    if (!Coding.GetLengthPrefixedSlice(ref input, out s)) return P + "full_history_ts_low";
                    if (s.IsEmpty) return P + "full_history_ts_low: empty";
                    FullHistoryTsLow = s.ToArray();
                    break;
                case VersionEditTag.PersistUserDefinedTimestamps:
                    if (!Coding.GetLengthPrefixedSlice(ref input, out s)) return P + "persist_user_defined_timestamps";
                    if (s.Length != 1) return P + "persist_user_defined_timestamps field wrong size";
                    PersistUserDefinedTimestamps = s[0] == 1;
                    break;
                case VersionEditTag.SubcompactionProgress:
                    if (!Coding.GetLengthPrefixedSlice(ref input, out s)) return P + "SubcompactionProgress not prefixed by length";
                    SubcompactionProgress = s.ToArray();
                    break;
                case VersionEditTag.LastCompactedManifestFileSize:
                {
                    if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> encoded) || !Coding.GetVarint64(ref encoded, out v))
                    {
                        return P + "last compacted manifest file size";
                    }
                    LastCompactedManifestFileSize = v;
                    break;
                }
                case VersionEditTag.CommittedOptionsFileNumber:
                {
                    if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> encoded) || !Coding.GetVarint64(ref encoded, out v) ||
                        !encoded.IsEmpty)
                    {
                        return P + "committed options file number";
                    }
                    CommittedOptionsFileNumber = v;
                    break;
                }
                default:
                    if ((tag & (uint)VersionEditTag.TagSafeIgnoreMask) != 0)
                    {
                        // Tag from the future which can be safely ignored: next is the entry length.
                        if (!Coding.GetVarint32(ref input, out uint len) || len > (uint)input.Length)
                        {
                            return P + "safely ignoreable tag length error";
                        }
                        input = input[(int)len..];
                    }
                    else
                    {
                        return P + "unknown tag";
                    }
                    break;
            }
        }
        if (!input.IsEmpty) return P + "invalid tag";
        return null;
    }

    /// <summary>
    /// Port of VersionEdit::DecodeNewFile4From (the part after the kNewFile4 tag). Returns null on success or an
    /// error message (without the "VersionEdit: " prefix).
    /// </summary>
    public static string? DecodeNewFile4From(ref ReadOnlySpan<byte> input, out int level, out FileMetaData f, out bool hasMinLogNumberToKeep,
        out ulong minLogNumberToKeep)
    {
        f = new FileMetaData();
        hasMinLogNumberToKeep = false;
        minLogNumberToKeep = 0;
        uint pathId = 0;
        if (!GetLevel(ref input, out level) || !Coding.GetVarint64(ref input, out ulong number) || !Coding.GetVarint64(ref input, out ulong fileSize) ||
            !GetInternalKey(ref input, out byte[] smallest) || !GetInternalKey(ref input, out byte[] largest) ||
            !Coding.GetVarint64(ref input, out ulong smallestSeqno) || !Coding.GetVarint64(ref input, out ulong largestSeqno))
        {
            return "new-file4 entry";
        }
        f.Smallest = smallest;
        f.Largest = largest;
        while (true)
        {
            if (!Coding.GetVarint32(ref input, out uint customTag)) return "new-file4 custom field";
            if (customTag == (uint)NewFileCustomTag.Terminate) break;
            if (!Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> field)) return "new-file4 custom field length prefixed slice error";
            switch ((NewFileCustomTag)customTag)
            {
                case NewFileCustomTag.PathId:
                    if (field.Length != 1) return "path_id field wrong size";
                    pathId = (uint)(int)(sbyte)field[0]; // C++ reads a (signed) char
                    if (pathId > 3) return "path_id wrong vaue";
                    break;
                case NewFileCustomTag.OldestAncestorTime:
                    if (!Coding.GetVarint64(ref field, out ulong t)) return "invalid oldest ancestor time";
                    f.OldestAncestorTime = t;
                    break;
                case NewFileCustomTag.FileCreationTime:
                    if (!Coding.GetVarint64(ref field, out t)) return "invalid file creation time";
                    f.FileCreationTime = t;
                    break;
                case NewFileCustomTag.EpochNumber:
                    if (!Coding.GetVarint64(ref field, out t)) return "invalid epoch number";
                    f.EpochNumber = t;
                    break;
                case NewFileCustomTag.FileChecksum:
                    f.FileChecksum = field.ToArray();
                    break;
                case NewFileCustomTag.FileChecksumFuncName:
                    f.FileChecksumFuncName = Encoding.UTF8.GetString(field);
                    break;
                case NewFileCustomTag.NeedCompaction:
                    if (field.Length != 1) return "need_compaction field wrong size";
                    f.MarkedForCompaction = field[0] == 1;
                    break;
                case NewFileCustomTag.MinLogNumberToKeepHack:
                    if (!Coding.GetFixed64(ref field, out minLogNumberToKeep)) return "deleted log number malformatted";
                    hasMinLogNumberToKeep = true;
                    break;
                case NewFileCustomTag.OldestBlobFileNumber:
                    if (!Coding.GetVarint64(ref field, out t)) return "invalid oldest blob file number";
                    f.OldestBlobFileNumber = t;
                    break;
                case NewFileCustomTag.Temperature:
                    if (field.Length != 1) return "temperature field wrong size";
                    if (field[0] < (byte)Temperature.LastTemperature) f.Temperature = (Temperature)field[0];
                    break;
                case NewFileCustomTag.UniqueId:
                    if (field.Length != 16) return "invalid unique id";
                    f.UniqueId = new UniqueId64x2(Coding.DecodeFixed64(field), Coding.DecodeFixed64(field[8..]));
                    break;
                case NewFileCustomTag.CompensatedRangeDeletionSize:
                    if (!Coding.GetVarint64(ref field, out t)) return "Invalid compensated range deletion size";
                    f.CompensatedRangeDeletionSize = t;
                    break;
                case NewFileCustomTag.TailSize:
                    if (!Coding.GetVarint64(ref field, out t)) return "invalid tail start offset";
                    f.TailSize = t;
                    break;
                case NewFileCustomTag.UserDefinedTimestampsPersisted:
                    if (field.Length != 1) return "user-defined timestamps persisted field wrong size";
                    f.UserDefinedTimestampsPersisted = field[0] == 1;
                    break;
                case NewFileCustomTag.MinTimestamp:
                    f.MinTimestamp = field.ToArray();
                    break;
                case NewFileCustomTag.MaxTimestamp:
                    f.MaxTimestamp = field.ToArray();
                    break;
                case NewFileCustomTag.FileOpenMetadata:
                    f.FileOpenMetadata = field.ToArray();
                    break;
                default:
                    if ((customTag & (uint)NewFileCustomTag.CustomTagNonSafeIgnoreMask) != 0)
                    {
                        return "new-file4 custom field not supported";
                    }
                    break;
            }
        }
        f.FileNumber = number;
        f.PathId = pathId;
        f.FileSize = fileSize;
        f.SmallestSeqno = smallestSeqno;
        f.LargestSeqno = largestSeqno;
        return null;
    }

    private static bool GetLevel(ref ReadOnlySpan<byte> input, out int level)
    {
        bool ok = Coding.GetVarint32(ref input, out uint v);
        level = (int)v;
        return ok;
    }

    private static bool GetInternalKey(ref ReadOnlySpan<byte> input, out byte[] key)
    {
        if (Coding.GetLengthPrefixedSlice(ref input, out ReadOnlySpan<byte> s))
        {
            key = s.ToArray();
            return IsValidInternalKey(s);
        }
        key = [];
        return false;
    }

    /// <summary>InternalKey::Valid(): at least 8 bytes and an extended value type in the footer.</summary>
    public static bool IsValidInternalKey(ReadOnlySpan<byte> internalKey)
    {
        if (internalKey.Length < DbFormat.InternalKeyFooterSize) return false;
        var t = (EntryType)internalKey[^DbFormat.InternalKeyFooterSize];
        return t <= EntryType.Merge || t == EntryType.SingleDeletion || t == EntryType.BlobIndex || t == EntryType.DeletionWithTimestamp ||
               t == EntryType.WideColumnEntity || t == EntryType.ValuePreferredSeqno || t == EntryType.RangeDeletion || t == EntryType.MaxValid;
    }

    public override string ToString()
    {
        var r = new StringBuilder("VersionEdit {");
        if (DbId is not null) r.Append("\n  DB ID: ").Append(DbId);
        if (ComparatorName is not null) r.Append("\n  Comparator: ").Append(ComparatorName);
        if (PersistUserDefinedTimestamps is bool p) r.Append("\n  PersistUserDefinedTimestamps: ").Append(p ? "true" : "false");
        if (LogNumber is ulong ln) r.Append("\n  LogNumber: ").Append(ln);
        if (PrevLogNumber is ulong pln) r.Append("\n  PrevLogNumber: ").Append(pln);
        if (NextFileNumber is ulong nf) r.Append("\n  NextFileNumber: ").Append(nf);
        if (MaxColumnFamily is uint mcf) r.Append("\n  MaxColumnFamily: ").Append(mcf);
        if (MinLogNumberToKeep is ulong ml) r.Append("\n  MinLogNumberToKeep: ").Append(ml);
        if (LastSequence is ulong ls) r.Append("\n  LastSeq: ").Append(ls);
        foreach ((int level, ulong number) in _deletedFiles) r.Append("\n  DeleteFile: ").Append(level).Append(' ').Append(number);
        foreach (NewFileEntry e in NewFiles) r.Append("\n  AddFile: ").Append(e.Level).Append(' ').Append(e.Meta);
        foreach (BlobFileAddition a in BlobFileAdditions) r.Append("\n  BlobFileAddition: ").Append(a);
        foreach (WalAddition w in WalAdditions) r.Append("\n  WalAddition: ").Append(w);
        if (IsWalDeletion) r.Append("\n  WalDeletion: log_number: ").Append(WalDeletionBefore);
        r.Append("\n  ColumnFamily: ").Append(ColumnFamily);
        if (ColumnFamilyAddName is not null) r.Append("\n  ColumnFamilyAdd: ").Append(ColumnFamilyAddName);
        if (IsColumnFamilyDrop) r.Append("\n  ColumnFamilyDrop");
        if (IsInAtomicGroup) r.Append("\n  AtomicGroup: ").Append(RemainingEntries);
        r.Append("\n}\n");
        return r.ToString();
    }
}
