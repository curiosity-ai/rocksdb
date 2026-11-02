namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Byte-wise equality for <c>byte[]</c> dictionary keys with allocation-free lookups by <see cref="ReadOnlySpan{T}"/>
/// (<see cref="Dictionary{TKey, TValue}.GetAlternateLookup{TAlternateKey}"/>). Used for tracked keys and lock tables,
/// where only equality matters (ordering is the column family comparator's job).
/// </summary>
internal sealed class ByteKeyComparer : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
    public static readonly ByteKeyComparer Instance = new();

    public bool Equals(byte[]? x, byte[]? y) => ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));
    public int GetHashCode(byte[] obj) => Hash(obj);
    public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
    public int GetHashCode(ReadOnlySpan<byte> alternate) => Hash(alternate);
    public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();

    /// <summary>64-bit hash of the key (GetSliceNPHash64 role), folded to 32 bits.</summary>
    public static int Hash(ReadOnlySpan<byte> key)
    {
        ulong h = Util.Hash.GetSliceNPHash64(key);
        return (int)(h ^ (h >> 32));
    }

    /// <summary>Stripe / bucket index for <paramref name="key"/> (fastrange64 of the 64-bit hash).</summary>
    public static int Bucket(ReadOnlySpan<byte> key, ulong seed, int count)
    {
        ulong h = Util.Hash.GetSliceNPHash64(key, seed);
        return (int)Math.BigMul(h, (ulong)count, out _);
    }
}

/// <summary>
/// A dictionary key over the first <see cref="Length"/> bytes of <see cref="Buffer"/>, a buffer owned and recycled by
/// the dictionary's owner (lock table entries, tracked keys): keys can be added and removed in steady state without
/// allocating an array per key.
/// </summary>
internal readonly struct PooledKey(byte[] buffer, int length)
{
    public readonly byte[] Buffer = buffer;
    public readonly int Length = length;

    public ReadOnlySpan<byte> Span => new(Buffer, 0, Length);
}

/// <summary>Byte-wise equality of <see cref="PooledKey"/>s, with allocation-free lookups by <see cref="ReadOnlySpan{T}"/>.</summary>
internal sealed class PooledKeyComparer : IEqualityComparer<PooledKey>, IAlternateEqualityComparer<ReadOnlySpan<byte>, PooledKey>
{
    public static readonly PooledKeyComparer Instance = new();

    public bool Equals(PooledKey x, PooledKey y) => x.Span.SequenceEqual(y.Span);
    public int GetHashCode(PooledKey obj) => ByteKeyComparer.Hash(obj.Span);
    public bool Equals(ReadOnlySpan<byte> alternate, PooledKey other) => alternate.SequenceEqual(other.Span);
    public int GetHashCode(ReadOnlySpan<byte> alternate) => ByteKeyComparer.Hash(alternate);

    /// <summary>Only used by alternate-key inserts; owners insert keys with their own (recycled) buffers.</summary>
    public PooledKey Create(ReadOnlySpan<byte> alternate) => new(alternate.ToArray(), alternate.Length);
}

/// <summary>
/// Recycled key buffers of one owner (not thread-safe; a mutable struct: keep it in a field and call it in place).
/// <see cref="Rent"/> copies a key into a buffer of at least its length; <see cref="Return"/> keeps up to
/// <see cref="MaxRetained"/> buffers.
/// </summary>
internal struct KeyBufferPool
{
    private const int MaxRetained = 1024;
    private byte[]?[]? _free;
    private int _count;

    public PooledKey Rent(ReadOnlySpan<byte> key)
    {
        byte[] b;
        if (_count > 0 && _free![_count - 1]!.Length >= key.Length)
        {
            b = _free[--_count]!;
            _free[_count] = null;
        }
        else
        {
            // Rounded up so that keys of slightly different lengths can share buffers.
            b = new byte[Math.Max(16, (key.Length + 15) & ~15)];
        }
        key.CopyTo(b);
        return new PooledKey(b, key.Length);
    }

    public void Return(PooledKey key)
    {
        if (_count == MaxRetained) return;
        _free ??= new byte[]?[16];
        if (_count == _free.Length) Array.Resize(ref _free, _count * 2);
        _free[_count++] = key.Buffer;
    }
}
