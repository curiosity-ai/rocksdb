namespace RocksDb.Managed.Table.Filters;

/// <summary>Allocator whose <c>malloc_usable_size</c> behaviour optimize_filters_for_memory rounds filter sizes to.</summary>
public enum FilterAllocatorKind
{
    /// <summary>
    /// No rounding: like native builds without ROCKSDB_MALLOC_USABLE_SIZE (Windows/MSVC, macOS), where
    /// optimize_filters_for_memory has no effect on filter sizes.
    /// </summary>
    None,

    /// <summary>
    /// glibc ptmalloc (the allocator of the Linux librocksdb.so shipped in the RocksDB NuGet package): a request of n
    /// bytes gets a chunk of max(32, align16(n + 8)) bytes and n' = chunk - 8 usable bytes; requests of 128 KiB and more
    /// (the default M_MMAP_THRESHOLD) are modelled as mmapped chunks of align4096(chunk + 8) bytes with chunk - 16 usable.
    /// </summary>
    Glibc,
}

/// <summary>
/// Model of the allocator's usable size used by optimize_filters_for_memory (port of the <c>malloc_usable_size</c> call
/// in XXPH3FilterBitsBuilder::AllocateMaybeRounding, table/block_based/filter_policy.cc).
/// <para>
/// Upstream sizes filters to what the allocator actually hands out, so the bytes native RocksDB writes depend on its
/// allocator and, for filters of 128 KiB and more, on the state of the native heap (glibc serves large requests from
/// the heap top when it has room and from mmap otherwise; free chunks may also be returned up to 24 bytes larger than
/// requested). This model reproduces the deterministic common case of glibc: every filter whose size does not depend
/// on heap history comes out byte-identical to native. Readers are unaffected either way: filter sizes are recorded in
/// the filter itself, and any size is valid.
/// </para>
/// </summary>
public static class FilterAllocationModel
{
    private const ulong GlibcSizeSz = 8;
    private const ulong GlibcAlignMask = 15;
    private const ulong GlibcMinSize = 32;
    private const ulong GlibcMmapThreshold = 128 * 1024; // DEFAULT_MMAP_THRESHOLD_MIN
    private const ulong PageSize = 4096;

    /// <summary>The modelled allocator. Default: <see cref="FilterAllocatorKind.Glibc"/> on Linux, else <see cref="FilterAllocatorKind.None"/>.</summary>
    public static FilterAllocatorKind Allocator { get; set; } = OperatingSystem.IsLinux() ? FilterAllocatorKind.Glibc : FilterAllocatorKind.None;

    /// <summary>Whether optimize_filters_for_memory changes filter sizes (ROCKSDB_MALLOC_USABLE_SIZE).</summary>
    public static bool IsActive => Allocator != FilterAllocatorKind.None;

    /// <summary>malloc_usable_size of an allocation of <paramref name="requested"/> bytes.</summary>
    public static ulong UsableSize(ulong requested) => Allocator switch
    {
        FilterAllocatorKind.Glibc => GlibcUsableSize(requested),
        _ => requested,
    };

    /// <summary>glibc: request2size + musable (malloc/malloc.c).</summary>
    public static ulong GlibcUsableSize(ulong requested)
    {
        ulong nb = requested + GlibcSizeSz + GlibcAlignMask < GlibcMinSize ? GlibcMinSize : (requested + GlibcSizeSz + GlibcAlignMask) & ~GlibcAlignMask;
        if (nb >= GlibcMmapThreshold)
        {
            // sysmalloc_mmap: size = ALIGN_UP(nb + SIZE_SZ, pagesize); usable = chunksize - CHUNK_HDR_SZ.
            ulong size = (nb + GlibcSizeSz + PageSize - 1) & ~(PageSize - 1);
            return size - 2 * GlibcSizeSz;
        }
        // In-use heap chunk: usable = chunksize - SIZE_SZ.
        return nb - GlibcSizeSz;
    }
}
