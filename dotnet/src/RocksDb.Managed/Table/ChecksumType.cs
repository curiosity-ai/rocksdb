namespace RocksDb.Managed.Table;

/// <summary>Mirrors rocksdb::ChecksumType (include/rocksdb/table.h). Persisted in the footer.</summary>
public enum ChecksumType : byte
{
    NoChecksum = 0x0,
    Crc32c = 0x1,
    XxHash = 0x2,
    XxHash64 = 0x3,
    Xxh3 = 0x4,
}
