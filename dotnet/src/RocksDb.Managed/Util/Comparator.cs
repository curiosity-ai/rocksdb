using System.Buffers;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed;

/// <summary>
/// Identifies the built-in comparators so hot paths (InternalKeyComparator, memtable, block search) can
/// bypass the virtual call and compare with a direct <see cref="MemoryExtensions.SequenceCompareTo{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/>.
/// </summary>
internal enum ComparatorKind : byte
{
    Custom = 0,
    /// <summary>"leveldb.BytewiseComparator": memcmp order, no timestamp.</summary>
    Bytewise = 1,
    /// <summary>"rocksdb.ReverseBytewiseComparator": reverse memcmp order, no timestamp.</summary>
    ReverseBytewise = 2,
}

/// <summary>
/// User key comparator. Port of rocksdb::Comparator (include/rocksdb/comparator.h). The Name is persisted
/// (MANIFEST, table properties) and must match across opens, e.g. "leveldb.BytewiseComparator".
/// CONTRACT (owned by stream E for the built-in implementations and InternalKeyComparator).
/// </summary>
/// <remarks>
/// Implementations must be thread-safe (they are shared by every reader and writer) and must define a
/// total order. <see cref="FindShortestSeparator"/> / <see cref="FindShortSuccessor"/> results are persisted
/// in SST index blocks, so a comparator's implementation of those must never change once data was written.
/// </remarks>
public abstract class Comparator
{
    protected Comparator() { }

    private protected Comparator(ComparatorKind kind) => Kind = kind;

    /// <summary>Non-virtual tag checked by hot paths to devirtualize the built-in comparators.</summary>
    internal ComparatorKind Kind { get; }

    public abstract string Name { get; }
    public abstract int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);
    public virtual bool Equal(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Compare(a, b) == 0;

    /// <summary>Port of FindShortestSeparator: writes a short key k with start &lt;= k &lt; limit into <paramref name="result"/>. Default: start unchanged.</summary>
    public virtual void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, System.Buffers.IBufferWriter<byte> result) => result.Write(start);
    /// <summary>Port of FindShortSuccessor. Default: key unchanged.</summary>
    public virtual void FindShortSuccessor(ReadOnlySpan<byte> key, System.Buffers.IBufferWriter<byte> result) => result.Write(key);
    /// <summary>Size in bytes of the user-defined timestamp suffix of every user key (0 = no timestamps).</summary>
    public virtual int TimestampSize => 0;

    /// <summary>
    /// True iff <paramref name="t"/> is the immediate successor of <paramref name="s"/> among keys of the same length
    /// (no key of that length sorts strictly between them). Default: false (always safe).
    /// </summary>
    public virtual bool IsSameLengthImmediateSuccessor(ReadOnlySpan<byte> s, ReadOnlySpan<byte> t) => false;

    /// <summary>False if two keys with different bytes can never compare equal (allows hash-based shortcuts). Default: true.</summary>
    public virtual bool CanKeysWithDifferentByteContentsBeEqual => true;

    /// <summary>For timestamp-aware comparators, the comparator over user keys without timestamp. Default: this.</summary>
    public virtual Comparator RootComparator => this;

    /// <summary>Compares two timestamps (each <see cref="TimestampSize"/> bytes). Default: 0.</summary>
    public virtual int CompareTimestamp(ReadOnlySpan<byte> ts1, ReadOnlySpan<byte> ts2) => 0;

    /// <summary>Compares user keys ignoring the timestamp suffix; <paramref name="aHasTs"/>/<paramref name="bHasTs"/> say whether each key carries one.</summary>
    public virtual int CompareWithoutTimestamp(ReadOnlySpan<byte> a, bool aHasTs, ReadOnlySpan<byte> b, bool bHasTs) => Compare(a, b);

    /// <summary>Compares two user keys that both carry a timestamp, ignoring it.</summary>
    public int CompareWithoutTimestamp(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => CompareWithoutTimestamp(a, true, b, true);

    public virtual bool EqualWithoutTimestamp(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => CompareWithoutTimestamp(a, true, b, true) == 0;

    /// <summary>Largest timestamp value (empty when <see cref="TimestampSize"/> is 0).</summary>
    public virtual ReadOnlySpan<byte> MaxTimestamp => default;
    /// <summary>Smallest timestamp value (empty when <see cref="TimestampSize"/> is 0).</summary>
    public virtual ReadOnlySpan<byte> MinTimestamp => default;
    public virtual string TimestampToString(ReadOnlySpan<byte> timestamp) => "";

    public override string ToString() => Name;

    /// <summary>"leveldb.BytewiseComparator".</summary>
    public static Comparator Bytewise => BytewiseComparator.Instance;
    /// <summary>"rocksdb.ReverseBytewiseComparator".</summary>
    public static Comparator ReverseBytewise => ReverseBytewiseComparator.Instance;
    /// <summary>"leveldb.BytewiseComparator.u64ts": bytewise over the key prefix, then newer (larger) 8-byte LE timestamp first.</summary>
    public static Comparator BytewiseWithU64Ts => ComparatorWithU64Ts.BytewiseInstance;
    /// <summary>"rocksdb.ReverseBytewiseComparator.u64ts".</summary>
    public static Comparator ReverseBytewiseWithU64Ts => ComparatorWithU64Ts.ReverseInstance;

    /// <summary>
    /// Resolves a built-in comparator by its persisted name (port of the factories registered in
    /// <c>RegisterBuiltinComparators</c>, util/comparator.cc). Returns false for unknown (custom) names.
    /// </summary>
    public static bool TryGetBuiltin(string name, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Comparator? comparator)
    {
        comparator = name switch
        {
            BytewiseComparator.ClassName => BytewiseComparator.Instance,
            ReverseBytewiseComparator.ClassName => ReverseBytewiseComparator.Instance,
            BytewiseComparator.ClassName + ComparatorWithU64Ts.Suffix => ComparatorWithU64Ts.BytewiseInstance,
            ReverseBytewiseComparator.ClassName + ComparatorWithU64Ts.Suffix => ComparatorWithU64Ts.ReverseInstance,
            _ => null,
        };
        return comparator is not null;
    }
}

/// <summary>"leveldb.BytewiseComparator" — memcmp ordering. Port of BytewiseComparatorImpl (util/comparator.cc).</summary>
public sealed class BytewiseComparator : Comparator
{
    internal const string ClassName = "leveldb.BytewiseComparator";
    public static readonly BytewiseComparator Instance = new();
    private BytewiseComparator() : base(ComparatorKind.Bytewise) { }
    public override string Name => ClassName;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceCompareTo(b);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equal(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceEqual(b);

    public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, IBufferWriter<byte> result)
    {
        int len = ShortestSeparatorLength(start, limit, out byte lastByte);
        if (len < 0)
        {
            result.Write(start);
            return;
        }
        // start[..len-1] followed by the incremented byte.
        Span<byte> dst = result.GetSpan(len);
        start[..(len - 1)].CopyTo(dst);
        dst[len - 1] = lastByte;
        result.Advance(len);
    }

    /// <summary>
    /// Core of BytewiseComparatorImpl::FindShortestSeparator. Returns -1 when start must stay unchanged; otherwise
    /// the separator is <c>start[..(len-1)] + lastByte</c>.
    /// </summary>
    internal static int ShortestSeparatorLength(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, out byte lastByte)
    {
        lastByte = 0;
        int minLength = Math.Min(start.Length, limit.Length);
        int diffIndex = start.CommonPrefixLength(limit);
        if (diffIndex >= minLength)
        {
            // Do not shorten if one string is a prefix of the other.
            return -1;
        }
        byte startByte = start[diffIndex];
        byte limitByte = limit[diffIndex];
        if (startByte >= limitByte)
        {
            // Cannot shorten since limit is smaller than start or start is already the shortest possible.
            return -1;
        }
        if (diffIndex < limit.Length - 1 || startByte + 1 < limitByte)
        {
            lastByte = (byte)(startByte + 1);
            return diffIndex + 1;
        }
        //     v
        // A A 1 A A A
        // A A 2
        // Incrementing the current byte would make start bigger than limit: skip this byte and increment the
        // first non-0xff byte after it.
        diffIndex++;
        while (diffIndex < start.Length)
        {
            if (start[diffIndex] < 0xff)
            {
                lastByte = (byte)(start[diffIndex] + 1);
                return diffIndex + 1;
            }
            diffIndex++;
        }
        return -1;
    }

    public override void FindShortSuccessor(ReadOnlySpan<byte> key, IBufferWriter<byte> result)
    {
        // Find first character that can be incremented.
        int i = key.IndexOfAnyExcept((byte)0xff);
        if (i < 0)
        {
            // key is a run of 0xffs. Leave it alone.
            result.Write(key);
            return;
        }
        Span<byte> dst = result.GetSpan(i + 1);
        key[..i].CopyTo(dst);
        dst[i] = (byte)(key[i] + 1);
        result.Advance(i + 1);
    }

    public override bool IsSameLengthImmediateSuccessor(ReadOnlySpan<byte> s, ReadOnlySpan<byte> t) => IsSameLengthImmediateSuccessorImpl(s, t);

    internal static bool IsSameLengthImmediateSuccessorImpl(ReadOnlySpan<byte> s, ReadOnlySpan<byte> t)
    {
        if (s.Length != t.Length || s.Length == 0) return false;
        int diff = s.CommonPrefixLength(t);
        if (diff >= s.Length) return false; // same slice
        byte bs = s[diff], bt = t[diff];
        // First different byte must be consecutive, and remaining bytes must be 0xff for s and 0x00 for t.
        if (bs == 0xff || bs + 1 != bt) return false;
        return s[(diff + 1)..].IndexOfAnyExcept((byte)0xff) < 0 && t[(diff + 1)..].IndexOfAnyExcept((byte)0) < 0;
    }

    public override bool CanKeysWithDifferentByteContentsBeEqual => false;
    public override int CompareWithoutTimestamp(ReadOnlySpan<byte> a, bool aHasTs, ReadOnlySpan<byte> b, bool bHasTs) => a.SequenceCompareTo(b);
    public override bool EqualWithoutTimestamp(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceEqual(b);
}
