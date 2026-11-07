using RocksDb.Managed.Options;

namespace RocksDb.Managed;

/// <summary>
/// How plain table keys are encoded (EncodingType, include/rocksdb/table.h). Persisted as <c>kPlain</c> /
/// <c>kPrefix</c> (encoding_type_string_map, options/options_helper.cc) and in each file's
/// "rocksdb.plain.table.encoding.type" property.
/// </summary>
public enum PlainTableEncodingType : byte
{
    /// <summary>kPlain: every key is written in full (with a varint length unless <see cref="PlainTableOptions.UserKeyLength"/> is fixed).</summary>
    Plain = 0,
    /// <summary>kPrefix: keys sharing a prefix with the previous key store only the suffix. Requires a prefix extractor.</summary>
    Prefix = 1,
}

/// <summary>
/// Port of rocksdb::PlainTableOptions (include/rocksdb/table.h) with upstream defaults; the OPTIONS-file section
/// <c>[TableOptions/PlainTable "cf"]</c> uses the names of plain_table_type_info (table/plain/plain_table_factory.cc),
/// written in native's order.
/// </summary>
/// <remarks>
/// PlainTable is designed for in-memory / tmpfs databases: no blocks, no compression, no checksums, no range deletions.
/// Without a prefix extractor the table is in total order mode (one hash bucket, binary search); with one, iterators
/// only support prefix seeks unless the table was built in total order mode (see the format notes in
/// table/plain/plain_table_factory.h).
/// </remarks>
public sealed class PlainTableOptions : PersistedOptions<PlainTableOptions>
{
    /// <summary>kPlainTableVariableLength: keys have variable lengths.</summary>
    public const uint VariableLength = 0;

    /// <summary>user_key_len: fixed user key length, or <see cref="VariableLength"/>.</summary>
    public uint UserKeyLength { get; set; } = VariableLength;
    /// <summary>bloom_bits_per_key: bits per prefix (or per key in total order mode) of the bloom filter; 0 disables it.</summary>
    public int BloomBitsPerKey { get; set; } = 10;
    /// <summary>hash_table_ratio: number of prefixes / number of hash buckets; 0 means binary search only.</summary>
    public double HashTableRatio { get; set; } = 0.75;
    /// <summary>index_sparseness: one index record (and, with kPrefix, one full key) per this many keys of a prefix.</summary>
    public ulong IndexSparseness { get; set; } = 16;
    /// <summary>huge_page_tlb_size: accepted and persisted; the managed port allocates indexes from the GC heap.</summary>
    public ulong HugePageTlbSize { get; set; }
    /// <summary>encoding_type: the key encoding of new files (each file records its own).</summary>
    public PlainTableEncodingType EncodingType { get; set; } = PlainTableEncodingType.Plain;
    /// <summary>full_scan_mode: read files record by record without building an index (Get and Seek are not allowed).</summary>
    public bool FullScanMode { get; set; }
    /// <summary>store_index_in_file: build the index and bloom filter at write time and store them in the file.</summary>
    public bool StoreIndexInFile { get; set; }

    private static readonly EnumMap<PlainTableEncodingType> s_encoding = new(
    [
        ("kPlain", PlainTableEncodingType.Plain),
        ("kPrefix", PlainTableEncodingType.Prefix),
    ]);

    // Order as written by native RocksDB 11.12 (ConfigurableHelper::SerializeOptions over plain_table_type_info).
    private static readonly OptionTable<PlainTableOptions> s_table = new OptionTable<PlainTableOptions>()
        .Bool("store_index_in_file", static o => o.StoreIndexInFile, static (o, v) => o.StoreIndexInFile = v)
        .Enum("encoding_type", s_encoding, static o => o.EncodingType, static (o, v) => o.EncodingType = v)
        .UInt64("huge_page_tlb_size", static o => o.HugePageTlbSize, static (o, v) => o.HugePageTlbSize = v)
        .Double("hash_table_ratio", static o => o.HashTableRatio, static (o, v) => o.HashTableRatio = v)
        .Bool("full_scan_mode", static o => o.FullScanMode, static (o, v) => o.FullScanMode = v)
        .Int32("bloom_bits_per_key", static o => o.BloomBitsPerKey, static (o, v) => o.BloomBitsPerKey = v)
        .UInt64("index_sparseness", static o => o.IndexSparseness, static (o, v) => o.IndexSparseness = v)
        .UInt32("user_key_len", static o => o.UserKeyLength, static (o, v) => o.UserKeyLength = v);

    internal override OptionTable<PlainTableOptions> OptionTable => s_table;

    public PlainTableOptions Clone()
    {
        var c = new PlainTableOptions();
        c.CopyFrom(this);
        return c;
    }
}

/// <summary>
/// Port of rocksdb::CuckooTableOptions (include/rocksdb/table.h) with upstream defaults; the OPTIONS-file section
/// <c>[TableOptions/CuckooTable "cf"]</c> uses the names of cuckoo_table_type_info (table/cuckoo/cuckoo_table_factory.cc),
/// written in native's order.
/// </summary>
/// <remarks>
/// Cuckoo tables hold fixed-size keys and values, at most one entry per user key, only Put and Delete entries, and are
/// read memory-mapped: the database needs <see cref="DbOptions.AllowMmapReads"/> (otherwise opening a table fails with
/// InvalidArgument "File is not mmaped", like upstream). Iterators do not support SeekForPrev.
/// </remarks>
public sealed class CuckooTableOptions : PersistedOptions<CuckooTableOptions>
{
    /// <summary>hash_table_ratio: utilization of the hash table.</summary>
    public double HashTableRatio { get; set; } = 0.9;
    /// <summary>max_search_depth: depth of the BFS for displacing elements on collision.</summary>
    public uint MaxSearchDepth { get; set; } = 100;
    /// <summary>cuckoo_block_size: consecutive buckets probed per hash function.</summary>
    public uint CuckooBlockSize { get; set; } = 5;
    /// <summary>identity_as_first_hash: the first hash is the key's first 8 bytes (keys must be 8 bytes).</summary>
    public bool IdentityAsFirstHash { get; set; }
    /// <summary>use_module_hash: hash values are taken modulo the table size (else the table size is a power of two).</summary>
    public bool UseModuleHash { get; set; } = true;

    // Order as written by native RocksDB 11.12 (ConfigurableHelper::SerializeOptions over cuckoo_table_type_info).
    private static readonly OptionTable<CuckooTableOptions> s_table = new OptionTable<CuckooTableOptions>()
        .Bool("use_module_hash", static o => o.UseModuleHash, static (o, v) => o.UseModuleHash = v)
        .Bool("identity_as_first_hash", static o => o.IdentityAsFirstHash, static (o, v) => o.IdentityAsFirstHash = v)
        .UInt32("max_search_depth", static o => o.MaxSearchDepth, static (o, v) => o.MaxSearchDepth = v)
        .UInt32("cuckoo_block_size", static o => o.CuckooBlockSize, static (o, v) => o.CuckooBlockSize = v)
        .Double("hash_table_ratio", static o => o.HashTableRatio, static (o, v) => o.HashTableRatio = v);

    internal override OptionTable<CuckooTableOptions> OptionTable => s_table;

    public CuckooTableOptions Clone()
    {
        var c = new CuckooTableOptions();
        c.CopyFrom(this);
        return c;
    }
}
