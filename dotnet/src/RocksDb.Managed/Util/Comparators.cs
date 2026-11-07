using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed;

/// <summary>
/// "rocksdb.ReverseBytewiseComparator" — reverse memcmp ordering. Port of ReverseBytewiseComparatorImpl
/// (util/comparator.cc).
/// </summary>
public sealed class ReverseBytewiseComparator : Comparator
{
    internal const string ClassName = "rocksdb.ReverseBytewiseComparator";
    public static readonly ReverseBytewiseComparator Instance = new();
    private ReverseBytewiseComparator() : base(ComparatorKind.ReverseBytewise) { }
    public override string Name => ClassName;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => b.SequenceCompareTo(a);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equal(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceEqual(b);

    public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, IBufferWriter<byte> result)
    {
        int len = ShortestSeparatorLength(start, limit);
        result.Write(len < 0 ? start : start[..len]);
    }

    /// <summary>Returns the separator length (a prefix of start), or -1 when start stays unchanged.</summary>
    internal static int ShortestSeparatorLength(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit)
    {
        int minLength = Math.Min(start.Length, limit.Length);
        int diffIndex = start.CommonPrefixLength(limit);
        if (diffIndex == minLength)
        {
            // Do not shorten if one string is a prefix of the other (upstream keeps it simple here too).
            return -1;
        }
        byte startByte = start[diffIndex];
        byte limitByte = limit[diffIndex];
        if (startByte > limitByte && diffIndex < start.Length - 1)
        {
            // e.g. start "AA3AA", limit "AA1BB" -> "AA3".
            return diffIndex + 1;
        }
        return -1;
    }

    /// <summary>Upstream does nothing here "for simplicity"; must stay that way (persisted in index blocks).</summary>
    public override void FindShortSuccessor(ReadOnlySpan<byte> key, IBufferWriter<byte> result) => result.Write(key);

    /// <summary>Always false upstream, to avoid surfacing design flaws in auto_prefix_mode.</summary>
    public override bool IsSameLengthImmediateSuccessor(ReadOnlySpan<byte> s, ReadOnlySpan<byte> t) => false;

    public override bool CanKeysWithDifferentByteContentsBeEqual => false;
    public override int CompareWithoutTimestamp(ReadOnlySpan<byte> a, bool aHasTs, ReadOnlySpan<byte> b, bool bHasTs) => b.SequenceCompareTo(a);
    public override bool EqualWithoutTimestamp(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceEqual(b);
}

/// <summary>
/// Comparator with a 64-bit little-endian integer timestamp suffix on every user key. Port of
/// <c>ComparatorWithU64TsImpl&lt;TComparator&gt;</c> (util/comparator.cc). Ordering: the root comparator over the
/// key without timestamp, then larger (newer) timestamp first. Names are <c>root.Name + ".u64ts"</c>.
/// </summary>
public sealed class ComparatorWithU64Ts : Comparator
{
    internal const string Suffix = ".u64ts";
    internal static readonly ComparatorWithU64Ts BytewiseInstance = new(BytewiseComparator.Instance);
    internal static readonly ComparatorWithU64Ts ReverseInstance = new(ReverseBytewiseComparator.Instance);

    private static ReadOnlySpan<byte> MaxTs => [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    private static ReadOnlySpan<byte> MinTs => [0, 0, 0, 0, 0, 0, 0, 0];

    private readonly Comparator _root;
    private readonly string _name;

    private ComparatorWithU64Ts(Comparator root)
    {
        _root = root;
        _name = root.Name + Suffix;
    }

    public override string Name => _name;
    public override int TimestampSize => sizeof(ulong);
    public override Comparator RootComparator => _root;
    public override ReadOnlySpan<byte> MaxTimestamp => MaxTs;
    public override ReadOnlySpan<byte> MinTimestamp => MinTs;

    // Upstream: no shortening for timestamped keys.
    public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, IBufferWriter<byte> result) => result.Write(start);
    public override void FindShortSuccessor(ReadOnlySpan<byte> key, IBufferWriter<byte> result) => result.Write(key);

    public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int ret = _root.Compare(a[..^sizeof(ulong)], b[..^sizeof(ulong)]);
        if (ret != 0) return ret;
        // For the same user key with different timestamps, larger (newer) timestamp comes first.
        return -CompareTimestamp(a[^sizeof(ulong)..], b[^sizeof(ulong)..]);
    }

    public override int CompareWithoutTimestamp(ReadOnlySpan<byte> a, bool aHasTs, ReadOnlySpan<byte> b, bool bHasTs)
    {
        ReadOnlySpan<byte> lhs = aHasTs ? a[..^sizeof(ulong)] : a;
        ReadOnlySpan<byte> rhs = bHasTs ? b[..^sizeof(ulong)] : b;
        return _root.Compare(lhs, rhs);
    }

    public override int CompareTimestamp(ReadOnlySpan<byte> ts1, ReadOnlySpan<byte> ts2)
    {
        ulong l = BinaryPrimitives.ReadUInt64LittleEndian(ts1);
        ulong r = BinaryPrimitives.ReadUInt64LittleEndian(ts2);
        return l.CompareTo(r);
    }

    public override string TimestampToString(ReadOnlySpan<byte> timestamp) =>
        BinaryPrimitives.ReadUInt64LittleEndian(timestamp).ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Port of EncodeU64Ts: writes the 8-byte little-endian timestamp.</summary>
    public static void EncodeU64Ts(ulong ts, Span<byte> dst) => BinaryPrimitives.WriteUInt64LittleEndian(dst, ts);

    /// <summary>Port of DecodeU64Ts. Returns false when <paramref name="ts"/> is not exactly 8 bytes.</summary>
    public static bool TryDecodeU64Ts(ReadOnlySpan<byte> ts, out ulong value)
    {
        if (ts.Length != sizeof(ulong))
        {
            value = 0;
            return false;
        }
        value = BinaryPrimitives.ReadUInt64LittleEndian(ts);
        return true;
    }
}
