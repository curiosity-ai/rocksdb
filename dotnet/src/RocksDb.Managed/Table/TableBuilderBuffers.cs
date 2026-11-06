using System.Buffers;
using RocksDb.Managed.Memory;

namespace RocksDb.Managed.Table;

/// <summary>Where the buffer of a <see cref="Block"/> (or a raw meta block payload) was rented from.</summary>
internal enum BlockBufferSource : byte
{
    /// <summary>Not rented: left to the GC.</summary>
    NotRented,
    /// <summary><see cref="Memory.BlockBufferPool"/>, or <see cref="ArrayPool{T}.Shared"/> for its power-of-two sizes (routed by length).</summary>
    BlockBufferPool,
    /// <summary><see cref="ArrayPool{T}.Shared"/> (raw payloads handed to code that returns them there, e.g. cached filters).</summary>
    Shared,
    /// <summary><see cref="TableBuilderBuffers.Pinned"/> (blocks a table reader pins for its lifetime).</summary>
    TableBuffers,
}

/// <summary>
/// Array pools for the large, table-sized buffers of table builders and of the blocks a table reader keeps for its
/// lifetime.
/// <para>
/// Builders: the growable block contents and restart arrays, the compressed-block buffer, the full filter and the output
/// staging buffer. The index block, its restart array and the filter of a 64 MiB flush grow to 100 KiB - 1 MiB.
/// <see cref="ArrayPool{T}.Shared"/> keeps a returned array in the returning thread's local slot first, and flushes /
/// compactions run on whichever thread-pool or background thread is free, so the next builder often did not find them
/// and allocated them again (100 - 300 KB per flush).
/// </para>
/// <para>
/// Readers: the index block and the full filter a reader pins (when they are not held by the block cache) come from a
/// separate pool, <see cref="Pinned"/>, and go back to it when the reader is closed, so the blocks of the tables a
/// compaction deletes serve the readers of its output files. A reader keeps its arrays for the table's lifetime;
/// sharing one pool with the builders let every new reader take the arrays its builder had just returned (or, since
/// the configurable pool also serves a request from the next larger size classes, a larger one), so while the number
/// of live tables grew every builder allocated its index, filter and output buffers again.
/// </para>
/// These pools are lock-based and shared by all threads, and are not trimmed.
/// </summary>
internal static class TableBuilderBuffers
{
    /// <summary>Byte arrays up to 4 MiB, at most 32 retained per size class.</summary>
    public static readonly ArrayPool<byte> Bytes = ArrayPool<byte>.Create(4 << 20, 32);

    /// <summary>Blocks pinned by table readers (index, full filter), up to 4 MiB, at most 32 retained per size class.</summary>
    public static readonly ArrayPool<byte> Pinned = ArrayPool<byte>.Create(4 << 20, 32);

    /// <summary>Restart-point arrays up to 1M entries, at most 16 retained per size class.</summary>
    public static readonly ArrayPool<uint> UInts = ArrayPool<uint>.Create(1 << 20, 16);

    /// <summary>Rents a buffer of at least <paramref name="minimumLength"/> bytes from <paramref name="source"/>.</summary>
    public static byte[] Rent(BlockBufferSource source, int minimumLength) => source switch
    {
        BlockBufferSource.TableBuffers => Pinned.Rent(minimumLength),
        BlockBufferSource.Shared => ArrayPool<byte>.Shared.Rent(minimumLength),
        BlockBufferSource.BlockBufferPool => BlockBufferPool.Rent(minimumLength),
        _ => GC.AllocateUninitializedArray<byte>(minimumLength),
    };

    /// <summary>Returns <paramref name="array"/> (rented by <see cref="Rent"/> with the same source).</summary>
    public static void Return(BlockBufferSource source, byte[] array)
    {
        switch (source)
        {
            case BlockBufferSource.TableBuffers:
                Pinned.Return(array);
                break;
            case BlockBufferSource.Shared:
                ArrayPool<byte>.Shared.Return(array);
                break;
            case BlockBufferSource.BlockBufferPool:
                BlockBufferPool.Return(array);
                break;
        }
    }
}
