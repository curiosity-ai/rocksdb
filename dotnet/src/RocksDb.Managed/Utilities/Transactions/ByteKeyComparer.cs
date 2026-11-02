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
