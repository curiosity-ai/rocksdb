using System.Buffers.Binary;
using System.Runtime.InteropServices;
using N = RocksDbSharp;

namespace RocksDb.Managed.RoundTrip.Engines;

/// <summary>
/// Timestamps for column families with user-defined timestamps (<see cref="Protocol.CfSpec.UserTimestamps"/>): 8-byte
/// little-endian values that increase across write batches, processes and engine switches (UTC ticks, bumped past
/// the last one handed out).
/// </summary>
public static class UdtClock
{
    private static long s_last;

    /// <summary>The maximum timestamp (all 0xff), used for reads of the latest state.</summary>
    public static byte[] MaxTimestamp { get; } = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

    public static byte[] Next()
    {
        long now = DateTime.UtcNow.Ticks;
        long last, next;
        do
        {
            last = Volatile.Read(ref s_last);
            next = Math.Max(now, last + 1);
        }
        while (Interlocked.CompareExchange(ref s_last, next, last) != last);
        byte[] ts = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(ts, (ulong)next);
        return ts;
    }
}

/// <summary>
/// Native comparator named "leveldb.BytewiseComparator.u64ts" (rocksdb_comparator_with_ts_create) with the order of
/// <see cref="Comparator.BytewiseWithU64Ts"/>; created once and kept for the process.
/// </summary>
internal static unsafe class NativeU64TsComparator
{
    private static readonly N.DestructorDelegate s_destructor = static _ => { };
    private static readonly N.CompareDelegate s_compare = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.Compare(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly N.CompareTsDelegate s_compareTs = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.CompareTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly N.CompareWithoutTsDelegate s_compareWithoutTs = static (_, a, alen, aHasTs, b, blen, bHasTs) =>
        Comparator.BytewiseWithU64Ts.CompareWithoutTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), aHasTs != 0,
            new ReadOnlySpan<byte>((void*)b, (int)blen), bHasTs != 0);
    private static readonly IntPtr s_name = Marshal.StringToHGlobalAnsi("leveldb.BytewiseComparator.u64ts");
    private static readonly N.NameDelegate s_nameDelegate = static _ => s_name;

    public static IntPtr Handle { get; } =
        N.Native.Instance.rocksdb_comparator_with_ts_create(IntPtr.Zero, s_destructor, s_compare, s_compareTs, s_compareWithoutTs, s_nameDelegate, 8);
}
