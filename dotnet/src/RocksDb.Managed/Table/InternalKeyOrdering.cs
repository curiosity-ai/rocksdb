using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Minimal internal-key ordering used by the table module: user key ascending (by the user comparator), then
/// packed (sequence, type) footer descending. Port of InternalKeyComparator::Compare (db/dbformat.cc) and the
/// separator helpers of ShortenedIndexBuilder (table/block_based/index_builder.cc).
/// Intentionally private to Table/; stream E owns the general InternalKeyComparator and this helper can be
/// replaced by it without changing the table format code.
/// </summary>
internal static class InternalKeyOrdering
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Compare(Comparator ucmp, bool bytewise, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        ReadOnlySpan<byte> ua = a[..^DbFormat.InternalKeyFooterSize];
        ReadOnlySpan<byte> ub = b[..^DbFormat.InternalKeyFooterSize];
        int r = bytewise ? ua.SequenceCompareTo(ub) : ucmp.Compare(ua, ub);
        if (r == 0)
        {
            ulong anum = Coding.DecodeFixed64(a[^DbFormat.InternalKeyFooterSize..]);
            ulong bnum = Coding.DecodeFixed64(b[^DbFormat.InternalKeyFooterSize..]);
            if (anum > bnum) r = -1;
            else if (anum < bnum) r = 1;
        }
        return r;
    }

    /// <summary>
    /// Port of ShortenedIndexBuilder::FindShortestInternalKeySeparator. Writes the separator into
    /// <paramref name="scratch"/> and returns true when it is shorter than <paramref name="start"/>; otherwise
    /// returns false and the caller uses <paramref name="start"/>.
    /// </summary>
    public static bool TryFindShortestSeparator(Comparator ucmp, ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, PooledBufferWriter scratch)
    {
        ReadOnlySpan<byte> userStart = DbFormat.ExtractUserKey(start);
        ReadOnlySpan<byte> userLimit = DbFormat.ExtractUserKey(limit);
        scratch.Clear();
        if (ReferenceEquals(ucmp, Comparator.Bytewise)) BytewiseFindShortestSeparator(userStart, userLimit, scratch);
        else ucmp.FindShortestSeparator(userStart, userLimit, scratch);
        return FinishShortened(ucmp, userStart, scratch);
    }

    /// <summary>Port of ShortenedIndexBuilder::FindShortInternalKeySuccessor.</summary>
    public static bool TryFindShortSuccessor(Comparator ucmp, ReadOnlySpan<byte> key, PooledBufferWriter scratch)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(key);
        scratch.Clear();
        if (ReferenceEquals(ucmp, Comparator.Bytewise)) BytewiseFindShortSuccessor(userKey, scratch);
        else ucmp.FindShortSuccessor(userKey, scratch);
        return FinishShortened(ucmp, userKey, scratch);
    }

    // Ports of BytewiseComparatorImpl::FindShortestSeparator / FindShortSuccessor (util/comparator.cc). They
    // determine index separators and so the exact bytes of index blocks; kept here so the table module matches
    // native output before stream E ports these overrides onto BytewiseComparator (then these can be removed).
    internal static void BytewiseFindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, PooledBufferWriter result)
    {
        int minLength = Math.Min(start.Length, limit.Length);
        int diffIndex = start.CommonPrefixLength(limit);
        if (diffIndex >= minLength)
        {
            // Do not shorten if one string is a prefix of the other
            result.Write(start);
            return;
        }
        byte startByte = start[diffIndex];
        byte limitByte = limit[diffIndex];
        if (startByte >= limitByte)
        {
            result.Write(start);
            return;
        }
        if (diffIndex < limit.Length - 1 || startByte + 1 < limitByte)
        {
            result.Write(start[..(diffIndex + 1)]);
            result.WrittenSpanMutable[diffIndex]++;
            return;
        }
        // Incrementing the current byte would make start >= limit; skip it and increment the first non-0xff byte after.
        diffIndex++;
        while (diffIndex < start.Length)
        {
            if (start[diffIndex] < 0xff)
            {
                result.Write(start[..(diffIndex + 1)]);
                result.WrittenSpanMutable[diffIndex]++;
                return;
            }
            diffIndex++;
        }
        result.Write(start);
    }

    internal static void BytewiseFindShortSuccessor(ReadOnlySpan<byte> key, PooledBufferWriter result)
    {
        for (int i = 0; i < key.Length; i++)
        {
            if (key[i] != 0xff)
            {
                result.Write(key[..(i + 1)]);
                result.WrittenSpanMutable[i]++;
                return;
            }
        }
        result.Write(key); // a run of 0xffs: leave it alone
    }

    private static bool FinishShortened(Comparator ucmp, ReadOnlySpan<byte> userKey, PooledBufferWriter scratch)
    {
        if (scratch.WrittenCount <= userKey.Length && ucmp.Compare(userKey, scratch.WrittenSpan) < 0)
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            Coding.PutFixed64(scratch, DbFormat.PackSequenceAndType(DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
            return true;
        }
        return false;
    }
}
