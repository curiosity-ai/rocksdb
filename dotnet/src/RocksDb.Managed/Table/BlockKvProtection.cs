namespace RocksDb.Managed.Table;

/// <summary>
/// Per key-value checksums of an in-memory block (block_protection_bytes_per_key). Port of the kv_checksum_ array of
/// Block (Block::InitializeDataBlockProtectionInfo / InitializeIndexBlockProtectionInfo /
/// InitializeMetaIndexBlockProtectionInfo, table/block_based/block.cc): for each entry, the low
/// <see cref="BytesPerKey"/> bytes of ProtectionInfo64().ProtectKV(key, value) (<see cref="Db.KvChecksum"/>).
/// </summary>
/// <remarks>
/// Upstream indexes the array by the entry index the iterator tracks; the port keeps the entry offsets (sorted) and
/// finds the index by binary search when verifying, so the parsing hot paths stay untouched. Keys and values are
/// taken as the iterator exposes them (with a global sequence number applied / a minimum timestamp padded); every
/// iterator over a block uses the same settings, so verification is unaffected.
/// </remarks>
internal sealed class BlockKvProtection(int bytesPerKey, int[] offsets, byte[] checksums)
{
    public int BytesPerKey { get; } = bytesPerKey;
    /// <summary>Offset of each entry in the block, ascending.</summary>
    public int[] Offsets { get; } = offsets;
    /// <summary><see cref="BytesPerKey"/> bytes per entry, in entry order.</summary>
    public byte[] Checksums { get; } = checksums;
}
