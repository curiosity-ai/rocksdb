namespace RocksDb.Managed.Compression;

/// <summary>Mirrors rocksdb::CompressionType (include/rocksdb/compression_type.h). Persisted in block trailers.</summary>
public enum CompressionType : byte
{
    None = 0x00,
    Snappy = 0x01,
    Zlib = 0x02,
    BZip2 = 0x03,
    LZ4 = 0x04,
    LZ4HC = 0x05,
    Xpress = 0x06,
    Zstd = 0x07,
    DisableCompressionOption = 0xff,
}
