using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// Orders internal keys (<c>user_key | fixed64(seq &lt;&lt; 8 | type)</c>) by increasing user key (user comparator),
/// then decreasing sequence number, then decreasing type. Port of rocksdb::InternalKeyComparator
/// (db/dbformat.h, db/dbformat.cc) plus the internal-key separator helpers from
/// <c>ShortenedIndexBuilder::FindShortestInternalKeySeparator</c> / <c>FindShortInternalKeySuccessor</c>
/// (table/block_based/index_builder.cc).
/// </summary>
/// <remarks>
/// Hot path: the built-in bytewise / reverse-bytewise user comparators are detected once (via the non-virtual
/// <see cref="Comparator.Kind"/>) and compared with an inlined vectorized <c>SequenceCompareTo</c> instead of a
/// virtual call. Thread-safe and immutable.
/// </remarks>
public sealed class InternalKeyComparator
{
    /// <summary>kDisableGlobalSequenceNumber (db/dbformat.h): disables the global-seqno override in <see cref="Compare(ReadOnlySpan{byte}, ulong, ReadOnlySpan{byte}, ulong)"/>.</summary>
    public const ulong DisableGlobalSequenceNumber = ulong.MaxValue;

    private readonly Comparator _user;
    private readonly ComparatorKind _kind;
    private string? _name;

    public InternalKeyComparator(Comparator userComparator)
    {
        ArgumentNullException.ThrowIfNull(userComparator);
        _user = userComparator;
        _kind = userComparator.Kind;
    }

    /// <summary>Shared instance over <see cref="Comparator.Bytewise"/>.</summary>
    public static InternalKeyComparator Bytewise { get; } = new(BytewiseComparator.Instance);

    public Comparator UserComparator => _user;

    /// <summary>True when the user comparator is the built-in bytewise comparator.</summary>
    public bool IsBytewise => _kind == ComparatorKind.Bytewise;

    /// <summary>"rocksdb.InternalKeyComparator:" + user comparator name (the historical upstream naming).</summary>
    public string Name => _name ??= "rocksdb.InternalKeyComparator:" + _user.Name;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareUserKey(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (_kind == ComparatorKind.Bytewise) return a.SequenceCompareTo(b);
        if (_kind == ComparatorKind.ReverseBytewise) return b.SequenceCompareTo(a);
        return _user.Compare(a, b);
    }

    /// <summary>User-key equality (port of Comparator::Equal on the user comparator); memcmp for the built-ins.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool EqualUserKey(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        _kind is ComparatorKind.Bytewise or ComparatorKind.ReverseBytewise ? a.SequenceEqual(b) : _user.Equal(a, b);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Footer(ReadOnlySpan<byte> internalKey) =>
        BinaryPrimitives.ReadUInt64LittleEndian(internalKey[^DbFormat.InternalKeyFooterSize..]);

    /// <summary>Compares two encoded internal keys (port of InternalKeyComparator::Compare(Slice, Slice)).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int r = CompareUserKey(a[..^DbFormat.InternalKeyFooterSize], b[..^DbFormat.InternalKeyFooterSize]);
        if (r == 0)
        {
            ulong anum = Footer(a), bnum = Footer(b);
            r = anum > bnum ? -1 : (anum < bnum ? 1 : 0);
        }
        return r;
    }

    /// <summary>Like <see cref="Compare(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> but ignores the value type (port of CompareKeySeq). Used by the memtable.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareKeySeq(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int r = CompareUserKey(a[..^DbFormat.InternalKeyFooterSize], b[..^DbFormat.InternalKeyFooterSize]);
        if (r == 0)
        {
            // Shift the number to exclude the last byte which contains the value type.
            ulong anum = Footer(a) >> 8, bnum = Footer(b) >> 8;
            r = anum > bnum ? -1 : (anum < bnum ? 1 : 0);
        }
        return r;
    }

    /// <summary>Compares an encoded internal key with a parsed one (port of Compare(Slice, ParsedInternalKey)).</summary>
    public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> bUserKey, ulong bSequence, EntryType bType)
    {
        int r = CompareUserKey(a[..^DbFormat.InternalKeyFooterSize], bUserKey);
        if (r == 0)
        {
            ulong anum = Footer(a), bnum = DbFormat.PackSequenceAndType(bSequence, bType);
            r = anum > bnum ? -1 : (anum < bnum ? 1 : 0);
        }
        return r;
    }

    /// <summary>
    /// Compare with sequence numbers overridden by <paramref name="aGlobalSeqno"/> / <paramref name="bGlobalSeqno"/>
    /// unless they equal <see cref="DisableGlobalSequenceNumber"/> (ingested files).
    /// </summary>
    public int Compare(ReadOnlySpan<byte> a, ulong aGlobalSeqno, ReadOnlySpan<byte> b, ulong bGlobalSeqno)
    {
        int r = CompareUserKey(a[..^DbFormat.InternalKeyFooterSize], b[..^DbFormat.InternalKeyFooterSize]);
        if (r == 0)
        {
            ulong af = Footer(a), bf = Footer(b);
            if (aGlobalSeqno != DisableGlobalSequenceNumber) af = DbFormat.PackSequenceAndType(aGlobalSeqno, (EntryType)(byte)af);
            if (bGlobalSeqno != DisableGlobalSequenceNumber) bf = DbFormat.PackSequenceAndType(bGlobalSeqno, (EntryType)(byte)bf);
            r = af > bf ? -1 : (af < bf ? 1 : 0);
        }
        return r;
    }

    public bool Equal(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Compare(a, b) == 0;

    /// <summary>
    /// Port of ShortenedIndexBuilder::FindShortestInternalKeySeparator. Writes to <paramref name="result"/> a short
    /// internal key k with start &lt;= k &lt; limit: either the shortened user key + (kMaxSequenceNumber,
    /// kValueTypeForSeek), or <paramref name="start"/> unchanged. Result bytes are persisted in index blocks.
    /// </summary>
    /// <returns>True if a shortened key was written, false if <paramref name="start"/> was written unchanged.</returns>
    public bool FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, IBufferWriter<byte> result) =>
        ShortenInternalKey(start, limit, isSuccessor: false, result);

    /// <summary>Port of ShortenedIndexBuilder::FindShortInternalKeySuccessor (used for the last index entry).</summary>
    /// <returns>True if a shortened key was written, false if <paramref name="key"/> was written unchanged.</returns>
    public bool FindShortSuccessor(ReadOnlySpan<byte> key, IBufferWriter<byte> result) =>
        ShortenInternalKey(key, default, isSuccessor: true, result);

    private bool ShortenInternalKey(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, bool isSuccessor, IBufferWriter<byte> result)
    {
        ReadOnlySpan<byte> userStart = DbFormat.ExtractUserKey(start);
        // Fast paths for the built-in comparators avoid the scratch buffer entirely.
        if (_kind == ComparatorKind.Bytewise)
        {
            int len;
            byte last;
            if (isSuccessor)
            {
                len = userStart.IndexOfAnyExcept((byte)0xff);
                if (len >= 0)
                {
                    last = (byte)(userStart[len] + 1);
                    len++;
                }
                else last = 0;
            }
            else
            {
                len = BytewiseComparator.ShortestSeparatorLength(userStart, DbFormat.ExtractUserKey(limit), out last);
            }
            // A bytewise-shortened key is always logically larger than userStart (a byte was incremented) and
            // physically no longer.
            if (len < 0)
            {
                result.Write(start);
                return false;
            }
            Span<byte> dst = result.GetSpan(len + DbFormat.InternalKeyFooterSize);
            userStart[..(len - 1)].CopyTo(dst);
            dst[len - 1] = last;
            Coding.EncodeFixed64(dst[len..], DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
            result.Advance(len + DbFormat.InternalKeyFooterSize);
            return true;
        }

        PooledBufferWriter scratch = ObjectPools.RentWriter();
        try
        {
            if (isSuccessor) _user.FindShortSuccessor(userStart, scratch);
            else _user.FindShortestSeparator(userStart, DbFormat.ExtractUserKey(limit), scratch);
            ReadOnlySpan<byte> shortened = scratch.WrittenSpan;
            if (shortened.Length <= userStart.Length && _user.Compare(userStart, shortened) < 0)
            {
                // User key has become shorter physically, but larger logically. Tack on the earliest possible
                // number to the shortened user key.
                Span<byte> dst = result.GetSpan(shortened.Length + DbFormat.InternalKeyFooterSize);
                DbFormat.AppendInternalKey(dst, shortened, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                result.Advance(shortened.Length + DbFormat.InternalKeyFooterSize);
                return true;
            }
            result.Write(start);
            return false;
        }
        finally
        {
            ObjectPools.ReturnWriter(scratch);
        }
    }

    public override string ToString() => Name;
}
