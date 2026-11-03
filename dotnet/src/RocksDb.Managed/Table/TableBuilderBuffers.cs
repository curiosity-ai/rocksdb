using System.Buffers;

namespace RocksDb.Managed.Table;

/// <summary>
/// Array pools for the growable buffers of table builders (block contents and restart arrays, the compressed-block
/// buffer, the full filter, the output staging buffer). The index block, its restart array and the filter of a 64 MiB
/// flush grow to 100 KiB - 1 MiB. <see cref="ArrayPool{T}.Shared"/> keeps a returned array in the returning thread's
/// local slot first, and flushes / compactions run on whichever thread-pool or background thread is free, so the next
/// builder often did not find them and allocated them again (100 - 300 KB per flush). These pools are lock-based and
/// shared by all threads, and are not trimmed.
/// </summary>
internal static class TableBuilderBuffers
{
    /// <summary>Byte arrays up to 4 MiB, at most 16 retained per size class.</summary>
    public static readonly ArrayPool<byte> Bytes = ArrayPool<byte>.Create(4 << 20, 16);

    /// <summary>Restart-point arrays up to 1M entries, at most 16 retained per size class.</summary>
    public static readonly ArrayPool<uint> UInts = ArrayPool<uint>.Create(1 << 20, 16);
}
