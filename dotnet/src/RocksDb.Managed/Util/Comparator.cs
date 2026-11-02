using System.Buffers;

namespace RocksDb.Managed;

/// <summary>
/// User key comparator. Port of rocksdb::Comparator (include/rocksdb/comparator.h). The Name is persisted
/// (MANIFEST, table properties) and must match across opens, e.g. "leveldb.BytewiseComparator".
/// CONTRACT (owned by stream E for the built-in implementations and InternalKeyComparator).
/// </summary>
public abstract class Comparator
{
    public abstract string Name { get; }
    public abstract int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);
    public virtual bool Equal(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Compare(a, b) == 0;

    /// <summary>Port of FindShortestSeparator: writes a short key k with start &lt;= k &lt; limit into <paramref name="result"/>. Default: start unchanged.</summary>
    public virtual void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, System.Buffers.IBufferWriter<byte> result) => result.Write(start);
    /// <summary>Port of FindShortSuccessor. Default: key unchanged.</summary>
    public virtual void FindShortSuccessor(ReadOnlySpan<byte> key, System.Buffers.IBufferWriter<byte> result) => result.Write(key);
    public virtual int TimestampSize => 0;

    public static Comparator Bytewise => BytewiseComparator.Instance;
}

/// <summary>"leveldb.BytewiseComparator" — memcmp ordering.</summary>
public sealed class BytewiseComparator : Comparator
{
    public static readonly BytewiseComparator Instance = new();
    private BytewiseComparator() { }
    public override string Name => "leveldb.BytewiseComparator";
    public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceCompareTo(b);
    public override bool Equal(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceEqual(b);
    // FindShortestSeparator / FindShortSuccessor: to be ported by stream E (util/comparator.cc).
}
