namespace RocksDb.Managed.Cache;

/// <summary>
/// Classification of cache entries for accounting and policy (rocksdb::CacheEntryRole, include/rocksdb/cache.h). Values and
/// order are upstream's.
/// </summary>
public enum CacheEntryRole : byte
{
    /// <summary>Block-based table data block.</summary>
    DataBlock = 0,
    /// <summary>Block-based table filter block (full or partitioned).</summary>
    FilterBlock = 1,
    /// <summary>Block-based table metadata block for partitioned filters.</summary>
    FilterMetaBlock = 2,
    /// <summary>OBSOLETE / DEPRECATED: old/removed block-based filter.</summary>
    DeprecatedFilterBlock = 3,
    /// <summary>Block-based table index block.</summary>
    IndexBlock = 4,
    /// <summary>Other kinds of block-based table block.</summary>
    OtherBlock = 5,
    /// <summary>WriteBufferManager's charge to account for its memtable usage.</summary>
    WriteBuffer = 6,
    /// <summary>Compression dictionary building buffer's charge.</summary>
    CompressionDictionaryBuildingBuffer = 7,
    /// <summary>Filter construction memory (Bloom / Ribbon building).</summary>
    FilterConstruction = 8,
    /// <summary>BlockBasedTableReader's charge to account for its memory usage.</summary>
    BlockBasedTableReader = 9,
    /// <summary>FileMetadata's charge to account for its memory usage.</summary>
    FileMetadata = 10,
    /// <summary>Blob value (when using the same cache as block cache and blob cache).</summary>
    BlobValue = 11,
    /// <summary>Blob cache's charge to account its memory usage (when using a separate block cache and blob cache).</summary>
    BlobCache = 12,
    /// <summary>Default bucket, for miscellaneous cache entries. Do not use for entries that could potentially add up to large usage.</summary>
    Misc = 13,
}

/// <summary>A set of <see cref="CacheEntryRole"/>s (rocksdb::CacheEntryRoleSet, a SmallEnumSet). Immutable.</summary>
public readonly struct CacheEntryRoleSet : IEquatable<CacheEntryRoleSet>
{
    private readonly ushort _bits;

    private CacheEntryRoleSet(ushort bits) => _bits = bits;

    /// <summary>The empty set.</summary>
    public static CacheEntryRoleSet Empty => default;

    /// <summary>A set of the given roles.</summary>
    public static CacheEntryRoleSet Of(params ReadOnlySpan<CacheEntryRole> roles)
    {
        ushort bits = 0;
        foreach (CacheEntryRole r in roles) bits |= Bit(r);
        return new CacheEntryRoleSet(bits);
    }

    private static ushort Bit(CacheEntryRole role) =>
        (uint)role <= (uint)CacheEntryRole.Misc ? (ushort)(1 << (int)role) : throw new ArgumentOutOfRangeException(nameof(role));

    public bool Contains(CacheEntryRole role) => (uint)role <= (uint)CacheEntryRole.Misc && (_bits & (1 << (int)role)) != 0;

    public CacheEntryRoleSet With(CacheEntryRole role) => new((ushort)(_bits | Bit(role)));

    public CacheEntryRoleSet Without(CacheEntryRole role) => new((ushort)(_bits & ~Bit(role)));

    public bool IsEmpty => _bits == 0;

    public bool Equals(CacheEntryRoleSet other) => _bits == other._bits;

    public override bool Equals(object? obj) => obj is CacheEntryRoleSet o && Equals(o);

    public override int GetHashCode() => _bits;

    public static bool operator ==(CacheEntryRoleSet a, CacheEntryRoleSet b) => a._bits == b._bits;

    public static bool operator !=(CacheEntryRoleSet a, CacheEntryRoleSet b) => a._bits != b._bits;
}

/// <summary>Helpers for <see cref="CacheEntryRole"/> (cache/cache_entry_roles.cc).</summary>
public static class CacheEntryRoles
{
    private static readonly string[] s_hyphen =
    [
        "data-block", "filter-block", "filter-meta-block", "deprecated-filter-block", "index-block", "other-block",
        "write-buffer", "compression-dictionary-building-buffer", "filter-construction", "block-based-table-reader",
        "file-metadata", "blob-value", "blob-cache", "misc",
    ];

    private static readonly string[] s_camel =
    [
        "DataBlock", "FilterBlock", "FilterMetaBlock", "DeprecatedFilterBlock", "IndexBlock", "OtherBlock", "WriteBuffer",
        "CompressionDictionaryBuildingBuffer", "FilterConstruction", "BlockBasedTableReader", "FileMetadata", "BlobValue",
        "BlobCache", "Misc",
    ];

    /// <summary>kNumCacheEntryRoles.</summary>
    public const int Count = (int)CacheEntryRole.Misc + 1;

    /// <summary>GetCacheEntryRoleName: the hyphenated name ("data-block").</summary>
    public static string GetName(CacheEntryRole role) => s_hyphen[(int)role];

    /// <summary>kCacheEntryRoleToCamelString ("DataBlock").</summary>
    public static string GetCamelName(CacheEntryRole role) => s_camel[(int)role];
}

/// <summary>
/// Tier of a cache (rocksdb::CacheTier, include/rocksdb/advanced_options.h); values are upstream's and persisted in the
/// OPTIONS file as <c>lowest_used_cache_tier</c> (kVolatileTier / kVolatileCompressedTier / kNonVolatileBlockTier).
/// </summary>
public enum CacheTier : byte
{
    /// <summary>The primary (uncompressed, in-memory) block cache only.</summary>
    VolatileTier = 0,
    /// <summary>Also an in-memory compressed secondary cache.</summary>
    VolatileCompressedTier = 0x01,
    /// <summary>Also a non-volatile (flash / NVM) secondary cache (the default: every configured tier is used).</summary>
    NonVolatileBlockTier = 0x02,
}

/// <summary>Primary cache type of a tiered cache (rocksdb::PrimaryCacheType).</summary>
public enum PrimaryCacheType : byte
{
    /// <summary>kCacheTypeLRU.</summary>
    Lru = 0,
    /// <summary>kCacheTypeHCC (HyperClockCache).</summary>
    HyperClock = 1,
}

/// <summary>
/// How entries move between the primary block cache and a secondary cache (rocksdb::TieredAdmissionPolicy,
/// include/rocksdb/cache.h).
/// </summary>
public enum TieredAdmissionPolicy : byte
{
    /// <summary>Select automatically: <see cref="ThreeQueue"/> with a non-volatile tier, else <see cref="Placeholder"/>.</summary>
    Auto = 0,
    /// <summary>
    /// During promotion/demotion, first time insert a placeholder entry, second time insert the full entry if the
    /// placeholder is found, i.e. insert on second hit.
    /// </summary>
    Placeholder = 1,
    /// <summary>Same as <see cref="Placeholder"/>, but entries evicted from the primary cache that were hit there are admitted directly.</summary>
    AllowCacheHits = 2,
    /// <summary>
    /// Three cache tiers (primary uncompressed, compressed secondary, compressed non-volatile), each managed as an
    /// independent queue: nothing is demoted on primary eviction; compressed blocks read from files warm the secondary
    /// tiers.
    /// </summary>
    ThreeQueue = 3,
    /// <summary>Admit every block evicted from the primary cache into the secondary cache.</summary>
    AllowAll = 4,
    /// <summary>kAdmPolicyMax: "no change" for <see cref="BlockCache.UpdateTieredCache"/>.</summary>
    Max = 5,
}
