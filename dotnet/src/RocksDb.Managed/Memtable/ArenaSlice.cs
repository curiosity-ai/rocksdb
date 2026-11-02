using RocksDb.Managed.Util;

namespace RocksDb.Managed.Memtable;

/// <summary>
/// A byte range in arena memory compared by content: the key type of the memtable's insert hint map (upstream
/// <c>std::unordered_map&lt;Slice, void*&gt;</c> with Slice keys pointing into the memtable arena). The memory must outlive
/// the map (it does: the arena is freed with the memtable).
/// </summary>
internal readonly unsafe struct ArenaSlice(byte* ptr, int length) : IEquatable<ArenaSlice>
{
    private readonly byte* _ptr = ptr;
    private readonly int _length = length;

    public ReadOnlySpan<byte> Span => new(_ptr, _length);

    public bool Equals(ArenaSlice other) => Span.SequenceEqual(other.Span);

    public override bool Equals(object? obj) => obj is ArenaSlice s && Equals(s);

    public override int GetHashCode() => (int)Hash.Hash32(Span, 0);
}
