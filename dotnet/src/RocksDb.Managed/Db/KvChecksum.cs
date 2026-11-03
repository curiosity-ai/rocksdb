using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// Per key-value protection info. Port of db/kv_checksum.h (ProtectionInfo64, ProtectionInfoKVO64, ProtectionInfoKVOC64,
/// ProtectionInfoKVOS64): a 64-bit value that is the XOR of independently seeded hashes (NPHash64 = Hash64, the XXH3
/// preview) of the key, the value, the operation type and the column family id ("C") or sequence number ("S"). Fields
/// are added ("Protect") and removed ("Strip") by XORing their hash, so after stripping every field of an intact entry
/// the value is 0 (<see cref="GetStatus"/>). The value is never persisted; truncated to 1, 2, 4 or 8 bytes it is stored
/// next to memtable entries (memtable_protection_bytes_per_key).
/// </summary>
/// <remarks>
/// The distinct C++ wrapper types (which fields are covered) are not modelled as distinct C# types: callers keep track of
/// the covered fields like the C++ code's variable types do. All operations are pure functions over the value.
/// </remarks>
internal static class KvChecksum
{
    // Each field is hashed with an independent value so we can catch fields being swapped (kv_checksum.h).
    private const ulong SeedK = 0;
    private const ulong SeedV = 0xD28AAD72F49BD50B;
    private const ulong SeedO = 0xA5155AE5E937AA16;
    private const ulong SeedS = 0x77A00858DDD37F21;
    private const ulong SeedC = 0x4A2AB5CBD26F542C;

    /// <summary>The upstream message of a non-zero protection info (ProtectionInfo::GetStatus).</summary>
    public const string MismatchMessage = "ProtectionInfo mismatch";

    /// <summary>The upstream message of a memtable entry failing its checksum (MemTable::VerifyEntryChecksum).</summary>
    public const string MemtableEntryCorruptedMessage = "Corrupted memtable entry, per key-value checksum verification failed.";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashK(ReadOnlySpan<byte> key) => Hash.Hash64(key, SeedK);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashV(ReadOnlySpan<byte> value) => Hash.Hash64(value, SeedV);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashO(EntryType type)
    {
        byte t = (byte)type;
        return Hash.Hash64(new ReadOnlySpan<byte>(in t), SeedO);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashS(ulong sequence)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, sequence);
        return Hash.Hash64(b, SeedS);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashC(uint columnFamilyId)
    {
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(b, columnFamilyId);
        return Hash.Hash64(b, SeedC);
    }

    /// <summary>ProtectionInfo64().ProtectKVO(key, value, op_type) (also StripKVO, the same XOR).</summary>
    public static ulong ProtectKvo(ulong info, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, EntryType type) =>
        info ^ HashK(key) ^ HashV(value) ^ HashO(type);

    /// <summary>ProtectionInfo64().ProtectKVO(key, value, op_type).</summary>
    public static ulong ProtectKvo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, EntryType type) => ProtectKvo(0, key, value, type);

    /// <summary>ProtectionInfo64().ProtectKV(key, value) (block per key-value checksums).</summary>
    public static ulong ProtectKv(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => HashK(key) ^ HashV(value);

    /// <summary>ProtectionInfoKVOC::StripKVO.</summary>
    public static ulong StripKvo(ulong info, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, EntryType type) => ProtectKvo(info, key, value, type);

    /// <summary>ProtectionInfoKVO::ProtectC / ProtectionInfoKVOC::StripC.</summary>
    public static ulong ProtectC(ulong info, uint columnFamilyId) => info ^ HashC(columnFamilyId);

    /// <summary>ProtectionInfoKVOC::StripC.</summary>
    public static ulong StripC(ulong info, uint columnFamilyId) => info ^ HashC(columnFamilyId);

    /// <summary>ProtectionInfoKVO::ProtectS.</summary>
    public static ulong ProtectS(ulong info, ulong sequence) => info ^ HashS(sequence);

    /// <summary>ProtectionInfoKVOS::StripS.</summary>
    public static ulong StripS(ulong info, ulong sequence) => info ^ HashS(sequence);

    /// <summary>
    /// One half of ProtectionInfoKVO::UpdateK (and, for a range deletion's end key, UpdateV): XORs out (or in) the hash
    /// of <paramref name="key"/> and of <paramref name="rangeEnd"/> when not empty. Call before and after changing the
    /// bytes in place.
    /// </summary>
    public static ulong UpdateK(ulong info, ReadOnlySpan<byte> key, ReadOnlySpan<byte> rangeEnd) =>
        rangeEnd.IsEmpty ? info ^ HashK(key) : info ^ HashK(key) ^ HashV(rangeEnd);

    /// <summary>ProtectionInfoKVO::UpdateO.</summary>
    public static ulong UpdateO(ulong info, EntryType oldType, EntryType newType) => info ^ HashO(oldType) ^ HashO(newType);

    /// <summary>ProtectionInfo::Encode: the low <paramref name="length"/> bytes (1, 2, 4 or 8) of the value, little endian.</summary>
    public static void Encode(ulong info, int length, Span<byte> dst)
    {
        switch (length)
        {
            case 1: dst[0] = (byte)info; break;
            case 2: BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)info); break;
            case 4: BinaryPrimitives.WriteUInt32LittleEndian(dst, (uint)info); break;
            case 8: BinaryPrimitives.WriteUInt64LittleEndian(dst, info); break;
            default: ThrowHelper.InvalidArgument("protection bytes per key must be 1, 2, 4 or 8"); break;
        }
    }

    /// <summary>ProtectionInfo::Verify: whether <paramref name="checksum"/> holds the encoding of <paramref name="info"/>.</summary>
    public static bool Verify(ulong info, int length, ReadOnlySpan<byte> checksum) => length switch
    {
        1 => checksum[0] == (byte)info,
        2 => BinaryPrimitives.ReadUInt16LittleEndian(checksum) == (ushort)info,
        4 => BinaryPrimitives.ReadUInt32LittleEndian(checksum) == (uint)info,
        8 => BinaryPrimitives.ReadUInt64LittleEndian(checksum) == info,
        _ => false,
    };

    /// <summary>ProtectionInfo::GetStatus: throws Corruption ("ProtectionInfo mismatch") unless every field was stripped.</summary>
    public static void ThrowIfMismatch(ulong info)
    {
        if (info != 0) ThrowMismatch();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowMismatch() => ThrowHelper.Corruption(MismatchMessage);

    /// <summary>Supported memtable / block protection sizes: 0, 1, 2, 4, 8.</summary>
    public static bool IsSupportedLength(uint length) => length is 0 or 1 or 2 or 4 or 8;
}
