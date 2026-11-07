using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// One record of a WriteBatch rep, as decoded by <see cref="WriteBatchReader"/>. The spans point into the rep.
/// <list type="bullet">
/// <item>Put / Merge / BlobIndex / WideColumnEntity / ValuePreferredSeqno (TimedPut): <see cref="Key"/>, <see cref="Value"/>.</item>
/// <item>Deletion / SingleDeletion: <see cref="Key"/>.</item>
/// <item>RangeDeletion: <see cref="Key"/> = begin key, <see cref="Value"/> = end key (exclusive).</item>
/// <item>LogData: <see cref="Value"/> (also <see cref="Blob"/>) = blob.</item>
/// <item>EndPrepareXID / CommitXID / RollbackXID: <see cref="Value"/> (also <see cref="Xid"/>) = xid.</item>
/// <item>CommitXIDAndTimestamp: <see cref="Key"/> (also <see cref="CommitTimestamp"/>) = commit timestamp, <see cref="Value"/> = xid.</item>
/// <item>Noop / BeginPrepareXID / BeginPersistedPrepareXID / BeginUnprepareXID: no payload.</item>
/// </list>
/// </summary>
public readonly ref struct WriteBatchEntry
{
    /// <summary>The tag byte as stored in the rep (e.g. <see cref="EntryType.ColumnFamilyValue"/>).</summary>
    public readonly EntryType Tag;
    /// <summary>The operation type with column-family variants mapped to their default-CF tag (e.g. <see cref="EntryType.Value"/>).</summary>
    public readonly EntryType Type;
    /// <summary>Column family id (0 = default).</summary>
    public readonly uint ColumnFamilyId;
    public readonly ReadOnlySpan<byte> Key;
    public readonly ReadOnlySpan<byte> Value;
    /// <summary>For TimedPut (<see cref="EntryType.ValuePreferredSeqno"/>): the write time unpacked from the value.</summary>
    public readonly ulong WriteUnixTime;

    internal WriteBatchEntry(EntryType tag, EntryType type, uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime)
    {
        Tag = tag;
        Type = type;
        ColumnFamilyId = cf;
        Key = key;
        Value = value;
        WriteUnixTime = writeUnixTime;
    }

    public ReadOnlySpan<byte> Blob => Value;
    public ReadOnlySpan<byte> Xid => Value;
    public ReadOnlySpan<byte> CommitTimestamp => Key;

    /// <summary>Whether this record counts towards the batch's <c>Count</c> (key-value operations only).</summary>
    public bool IsCounted => WriteBatchReader.IsCountedType(Type);
}

/// <summary>
/// Allocation-free forward reader over a WriteBatch rep (from <see cref="WriteBatch.Data"/>, a WAL record, or
/// <c>GetUpdatesSince</c>). Port of <c>ReadRecordFromWriteBatch</c> and the validation in
/// <c>WriteBatchInternal::Iterate</c> (db/write_batch.cc).
/// <para>
/// Usable with <c>foreach</c>. Malformed input throws a Corruption <see cref="RocksDbException"/>; after the last
/// record, the number of key-value records is checked against the header count ("WriteBatch has wrong count").
/// </para>
/// </summary>
public ref struct WriteBatchReader
{
    private readonly ReadOnlySpan<byte> _input;
    private int _pos;
    private readonly uint _count;
    private uint _found;
    private readonly bool _wholeBatch;
    private WriteBatchEntry _current;

    /// <summary>Creates a reader over a complete rep (12-byte header followed by records).</summary>
    public WriteBatchReader(ReadOnlySpan<byte> rep)
    {
        if (rep.Length < WriteBatch.HeaderSize) ThrowHelper.Corruption("malformed WriteBatch (too small)");
        Sequence = Coding.DecodeFixed64(rep);
        _count = Coding.DecodeFixed32(rep[8..]);
        _input = rep[WriteBatch.HeaderSize..];
        _pos = 0;
        _wholeBatch = true;
        _found = 0;
        _current = default;
    }

    /// <summary>Creates a reader over a range of records (no header, no count validation).</summary>
    internal WriteBatchReader(ReadOnlySpan<byte> records, bool wholeBatch, uint count)
    {
        _input = records;
        _pos = 0;
        _wholeBatch = wholeBatch;
        _count = count;
        Sequence = 0;
        _found = 0;
        _current = default;
    }

    /// <summary>Sequence number from the header.</summary>
    public ulong Sequence { get; }
    /// <summary>Record count from the header.</summary>
    public uint Count => _count;
    /// <summary>Remaining undecoded bytes.</summary>
    public readonly int RemainingBytes => _input.Length - _pos;

    /// <summary>Reads the next record; returns false at the end (after validating the count).</summary>
    public bool TryReadNext(out WriteBatchEntry entry)
    {
        if (_pos >= _input.Length)
        {
            if (_wholeBatch && _found != _count) ThrowHelper.Corruption("WriteBatch has wrong count");
            entry = default;
            return false;
        }
        string? error = TryReadRecord(_input, ref _pos, out entry);
        if (error is not null) ThrowHelper.Corruption(error);
        if (IsCountedType(entry.Type)) _found++;
        return true;
    }

    public readonly WriteBatchReader GetEnumerator() => this;
    public readonly WriteBatchEntry Current => _current;
    public bool MoveNext() => TryReadNext(out _current);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsCountedType(EntryType type) => type switch
    {
        EntryType.Value or EntryType.Deletion or EntryType.SingleDeletion or EntryType.RangeDeletion or
        EntryType.Merge or EntryType.BlobIndex or EntryType.WideColumnEntity or EntryType.ValuePreferredSeqno => true,
        _ => false,
    };

    /// <summary>
    /// Decodes one record and advances <paramref name="input"/>. Returns null on success or the corruption reason.
    /// Port of <c>ReadRecordFromWriteBatch</c>. <paramref name="input"/> must not be empty.
    /// </summary>
    internal static string? TryReadRecord(ReadOnlySpan<byte> input, scoped ref int pos, out WriteBatchEntry entry)
    {
        var tag = (EntryType)input[pos++];
        uint cf = 0; // default
        ReadOnlySpan<byte> key = default, value = default;
        ulong writeUnixTime = 0;
        EntryType type;
        switch (tag)
        {
            case EntryType.ColumnFamilyValue:
            case EntryType.Value:
                type = EntryType.Value;
                if ((tag == EntryType.ColumnFamilyValue && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key) || !GetSlice(input, ref pos, out value))
                    goto BadPut;
                break;
            case EntryType.ColumnFamilyDeletion:
            case EntryType.ColumnFamilySingleDeletion:
            case EntryType.Deletion:
            case EntryType.SingleDeletion:
                type = tag is EntryType.ColumnFamilyDeletion or EntryType.Deletion ? EntryType.Deletion : EntryType.SingleDeletion;
                if ((tag is EntryType.ColumnFamilyDeletion or EntryType.ColumnFamilySingleDeletion && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key))
                {
                    entry = default;
                    return "bad WriteBatch Delete";
                }
                break;
            case EntryType.ColumnFamilyRangeDeletion:
            case EntryType.RangeDeletion:
                // for range delete, "key" is begin_key, "value" is end_key
                type = EntryType.RangeDeletion;
                if ((tag == EntryType.ColumnFamilyRangeDeletion && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key) || !GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad WriteBatch DeleteRange";
                }
                break;
            case EntryType.ColumnFamilyMerge:
            case EntryType.Merge:
                type = EntryType.Merge;
                if ((tag == EntryType.ColumnFamilyMerge && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key) || !GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad WriteBatch Merge";
                }
                break;
            case EntryType.ColumnFamilyBlobIndex:
            case EntryType.BlobIndex:
                type = EntryType.BlobIndex;
                if ((tag == EntryType.ColumnFamilyBlobIndex && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key) || !GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad WriteBatch BlobIndex";
                }
                break;
            case EntryType.LogData:
                type = tag;
                if (!GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad WriteBatch Blob";
                }
                break;
            case EntryType.Noop:
            case EntryType.BeginPrepareXID:
            // This indicates that the prepared batch is also persisted in the db. This is used in WritePreparedTxn
            case EntryType.BeginPersistedPrepareXID:
            // This is used in WriteUnpreparedTxn
            case EntryType.BeginUnprepareXID:
                type = tag;
                break;
            case EntryType.EndPrepareXID:
                type = tag;
                if (!GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad EndPrepare XID";
                }
                break;
            case EntryType.CommitXIDAndTimestamp:
                type = tag;
                if (!GetSlice(input, ref pos, out key))
                {
                    entry = default;
                    return "bad commit timestamp";
                }
                if (!GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad Commit XID";
                }
                break;
            case EntryType.CommitXID:
                type = tag;
                if (!GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad Commit XID";
                }
                break;
            case EntryType.RollbackXID:
                type = tag;
                if (!GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad Rollback XID";
                }
                break;
            case EntryType.ColumnFamilyWideColumnEntity:
            case EntryType.WideColumnEntity:
                type = EntryType.WideColumnEntity;
                if ((tag == EntryType.ColumnFamilyWideColumnEntity && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key) || !GetSlice(input, ref pos, out value))
                {
                    entry = default;
                    return "bad WriteBatch PutEntity";
                }
                break;
            case EntryType.ColumnFamilyValuePreferredSeqno:
            case EntryType.ValuePreferredSeqno:
                type = EntryType.ValuePreferredSeqno;
                if ((tag == EntryType.ColumnFamilyValuePreferredSeqno && !GetVarint32(input, ref pos, out cf)) ||
                    !GetSlice(input, ref pos, out key) || !GetSlice(input, ref pos, out value) ||
                    value.Length < 8)
                {
                    entry = default;
                    return "bad WriteBatch TimedPut";
                }
                // ParsePackedValueWithWriteTime: value | fixed64 write_unix_time (db/seqno_to_time_mapping.h)
                writeUnixTime = Coding.DecodeFixed64(value[^8..]);
                value = value[..^8];
                break;
            default:
                entry = default;
                return "unknown WriteBatch tag: " + ((byte)tag).ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        entry = new WriteBatchEntry(tag, type, cf, key, value, writeUnixTime);
        return null;

    BadPut:
        entry = default;
        return "bad WriteBatch Put";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool GetVarint32(ReadOnlySpan<byte> input, scoped ref int pos, out uint value)
    {
        int n = Coding.DecodeVarint32(input[pos..], out value);
        pos += n;
        return n != 0;
    }

    /// <summary>GetLengthPrefixedSlice over (input, pos); the result has the same lifetime as <paramref name="input"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool GetSlice(ReadOnlySpan<byte> input, scoped ref int pos, out ReadOnlySpan<byte> result)
    {
        int n = Coding.DecodeVarint32(input[pos..], out uint len);
        if (n != 0 && (uint)(input.Length - pos - n) >= len)
        {
            result = input.Slice(pos + n, (int)len);
            pos += n + (int)len;
            return true;
        }
        result = default;
        return false;
    }
}
