using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Memory;

namespace RocksDb.Managed.Table;

/// <summary>Key of a block in a block cache: a per-table cache id plus the block offset.</summary>
public readonly record struct BlockCacheKey(ulong TableId, ulong Offset);

/// <summary>
/// Seam for a block cache (stream F provides the real sharded LRU / HyperClock cache). Port of the role of
/// table/block_based/block_cache.h + the Cache interface as used by BlockBasedTable::RetrieveBlock.
/// <para>
/// Ownership: <see cref="TryGet"/> returns a block with a reference already added for the caller (the caller must
/// <see cref="Block.Release"/> it). <see cref="Insert"/> must take its own reference (<see cref="Block.AddRef"/>) if it
/// keeps the block; the caller keeps (and later releases) its own reference. Implementations must be thread-safe.
/// </para>
/// </summary>
public interface IBlockCache
{
    bool TryGet(in BlockCacheKey key, [NotNullWhen(true)] out Block? block);
    void Insert(in BlockCacheKey key, Block block);
}

/// <summary>No-op cache: every lookup misses.</summary>
public sealed class NoBlockCache : IBlockCache
{
    public static readonly NoBlockCache Instance = new();
    private NoBlockCache() { }
    public bool TryGet(in BlockCacheKey key, [NotNullWhen(true)] out Block? block)
    {
        block = null;
        return false;
    }
    public void Insert(in BlockCacheKey key, Block block) { }
}

/// <summary>
/// Reads serialized blocks (payload + 5-byte trailer), verifies the checksum and decompresses through the
/// <see cref="BlockCompression"/> facade. Port of BlockFetcher (table/block_fetcher.cc), ReadBlockContents and
/// DecompressSerializedBlock (table/format.cc).
/// </summary>
internal static class BlockFetcher
{
    // Block buffers come from BlockBufferPool (fine size classes): an uncompressed block keeps its read buffer for its
    // whole life in the block cache, and its length is what the cache is charged.
    public static Block ReadBlock(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums) =>
        ReadBlock(file, footer, handle, verifyChecksums, null);

    /// <summary>Reads a block, decompressing with <paramref name="dictionary"/> (data blocks of dictionary-compressed tables).</summary>
    public static Block ReadBlock(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums, CompressionDictionary? dictionary)
    {
        int n = CheckedLength(handle);
        byte[] buf = BlockBufferPool.Rent(n);
        try
        {
            int read = ReadFully(file, (long)handle.Offset, buf.AsSpan(0, n));
            if (read != n) ThrowTruncated(handle, read);
        }
        catch
        {
            BlockBufferPool.Return(buf);
            throw;
        }
        return ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary);
    }

    public static ValueTask<Block> ReadBlockAsync(IRandomAccessFile file, Footer footer, BlockHandle handle, bool verifyChecksums, CancellationToken ct) =>
        ReadBlockAsync(file, footer, handle, verifyChecksums, null, ct);

    public static ValueTask<Block> ReadBlockAsync(IRandomAccessFile file, Footer footer, BlockHandle handle, bool verifyChecksums,
        CompressionDictionary? dictionary, CancellationToken ct)
    {
        int n = CheckedLength(handle);
        byte[] buf = BlockBufferPool.Rent(n);
        ValueTask<int> first;
        try
        {
            first = file.ReadAsync((long)handle.Offset, buf.AsMemory(0, n), ct);
        }
        catch (Exception e)
        {
            BlockBufferPool.Return(buf);
            return ValueTask.FromException<Block>(e);
        }
        int already = -1;
        if (first.IsCompletedSuccessfully)
        {
            // Synchronous completion (page cache hit): no state machine. A ValueTask may be consumed only once, so the
            // slow path continues from the known count.
            try
            {
                already = first.Result;
                if (already == n) return new ValueTask<Block>(ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary));
            }
            catch (Exception e)
            {
                // ProcessSerialized returns the buffer itself when it fails.
                if (already != n) BlockBufferPool.Return(buf);
                return ValueTask.FromException<Block>(e);
            }
            first = default;
        }
        return ReadBlockSlowAsync(file, footer, handle, verifyChecksums, dictionary, buf, n, first, already, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<Block> ReadBlockSlowAsync(IRandomAccessFile file, Footer footer, BlockHandle handle, bool verifyChecksums,
        CompressionDictionary? dictionary, byte[] buf, int n, ValueTask<int> first, int alreadyRead, CancellationToken ct)
    {
        try
        {
            int total = alreadyRead >= 0 ? alreadyRead : await first.ConfigureAwait(false);
            if (total > 0)
            {
                while (total < n)
                {
                    int r = await file.ReadAsync((long)handle.Offset + total, buf.AsMemory(total, n - total), ct).ConfigureAwait(false);
                    if (r <= 0) break;
                    total += r;
                }
            }
            if (total != n) ThrowTruncated(handle, Math.Max(total, 0));
        }
        catch
        {
            BlockBufferPool.Return(buf);
            throw;
        }
        return ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary);
    }

    private static int CheckedLength(BlockHandle handle)
    {
        ulong n = handle.Size + BlockBasedTableConstants.BlockTrailerSize;
        if (n > int.MaxValue) ThrowHelper.Corruption("block too large: " + handle);
        return (int)n;
    }

    internal static int ReadFully(IRandomAccessFile file, long offset, Span<byte> dst)
    {
        int total = 0;
        while (total < dst.Length)
        {
            int r = file.Read(offset + total, dst[total..]);
            if (r <= 0) break;
            total += r;
        }
        return total;
    }

    [DoesNotReturn]
    private static void ThrowTruncated(BlockHandle handle, int read) =>
        ThrowHelper.Corruption("truncated block read from file at offset " + handle.Offset + ", expected " + (handle.Size + BlockBasedTableConstants.BlockTrailerSize) + " bytes, got " + read);

    /// <summary>
    /// Reads a block through a readahead buffer (BlockFetcher::TryGetFromPrefetchBuffer): the serialized block is
    /// verified in the buffer, uncompressed blocks are copied into their own buffer, compressed ones decompressed
    /// straight from it.
    /// </summary>
    public static Block ReadBlock(AsyncPrefetchBuffer prefetch, in Footer footer, BlockHandle handle, bool verifyChecksums, CompressionDictionary? dictionary)
    {
        int n = CheckedLength(handle);
        if (!prefetch.TryRead((long)handle.Offset, n, out ReadOnlySpan<byte> serialized)) ThrowTruncated(handle, 0);
        int size = (int)handle.Size;
        if (verifyChecksums && !BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, serialized, handle.Offset))
            ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + size);
        var type = (CompressionType)serialized[size];
        if (type != CompressionType.None) return Decompress(type, serialized[..size], footer.FormatVersion, dictionary);
        byte[] buf = BlockBufferPool.Rent(Math.Max(size, 1));
        serialized[..size].CopyTo(buf);
        return Block.Create(buf, size, rentedFromSharedPool: true);
    }

    /// <summary>Takes ownership of <paramref name="buf"/> (rented) holding payload + trailer.</summary>
    private static Block ProcessSerialized(byte[] buf, in Footer footer, BlockHandle handle, bool verifyChecksums, CompressionDictionary? dictionary)
    {
        int size = (int)handle.Size;
        try
        {
            if (verifyChecksums && !BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, buf.AsSpan(0, size + BlockBasedTableConstants.BlockTrailerSize), handle.Offset))
                ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + size);
        }
        catch
        {
            BlockBufferPool.Return(buf);
            throw;
        }
        var type = (CompressionType)buf[size];
        if (type == CompressionType.None) return Block.Create(buf, size, rentedFromSharedPool: true);
        try
        {
            return Decompress(type, buf.AsSpan(0, size), footer.FormatVersion, dictionary);
        }
        finally
        {
            BlockBufferPool.Return(buf);
        }
    }

    [ThreadStatic] private static RentedBufferWriter? t_decompressWriter;

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static Block Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion) => Decompress(type, compressed, formatVersion, null);

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static Block Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, CompressionDictionary? dictionary)
    {
        // No initial buffer: the decompressors request the exact uncompressed size (stored in the block or the snappy
        // header) with a single GetSpan, so the block gets one right-sized array and no grow-and-copy.
        // Per-thread writer (it holds no buffer between calls: Detach/Dispose leave it empty), so a block read does not
        // allocate the writer object.
        RentedBufferWriter w = t_decompressWriter ?? new RentedBufferWriter(0, blockPool: true);
        t_decompressWriter = null;
        try
        {
            BlockCompression.Decompress(type, compressed, formatVersion, w, dictionary);

            byte[] arr = w.Detach(out int len);
            return Block.Create(arr, len, rentedFromSharedPool: true);
        }
        finally
        {
            w.Dispose();
            t_decompressWriter = w;
        }
    }

    /// <summary>Unpacks a serialized block's payload into a fresh rented array (used for filters, which aren't parsed as blocks).</summary>
    public static byte[] ReadRawPayload(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums, out int length)
    {
        int n = CheckedLength(handle);
        byte[] buf = ArrayPool<byte>.Shared.Rent(n);
        try
        {
            int read = ReadFully(file, (long)handle.Offset, buf.AsSpan(0, n));
            if (read != n) ThrowTruncated(handle, read);
            int size = (int)handle.Size;
            if (verifyChecksums && !BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, buf.AsSpan(0, n), handle.Offset))
                ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + size);
            var type = (CompressionType)buf[size];
            if (type == CompressionType.None)
            {
                length = size;
                return buf;
            }
            var w = new RentedBufferWriter(size * 2, blockPool: false);
            try
            {
                BlockCompression.Decompress(type, buf.AsSpan(0, size), footer.FormatVersion, w);
                ArrayPool<byte>.Shared.Return(buf);
                buf = [];
                return w.Detach(out length);
            }
            finally
            {
                w.Dispose();
            }
        }
        catch
        {
            if (buf.Length > 0) ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
    }
}

/// <summary>
/// Minimal growable writer over a rented array whose buffer can be detached (ownership transfer). Arrays come from
/// <see cref="BlockBufferPool"/> (<paramref name="blockPool"/>, for buffers that become <see cref="Block"/>s) or from
/// <see cref="ArrayPool{T}.Shared"/> (buffers handed to code that returns them there, e.g. cached filters).
/// </summary>
internal sealed class RentedBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;
    private readonly bool _blockPool;

    public RentedBufferWriter(int initialCapacity, bool blockPool)
    {
        _blockPool = blockPool;
        _buffer = initialCapacity <= 0 ? [] : RentArray(Math.Max(initialCapacity, 16));
    }

    private byte[] RentArray(int n) => _blockPool ? BlockBufferPool.Rent(n) : ArrayPool<byte>.Shared.Rent(n);

    private void ReturnArray(byte[] a)
    {
        if (a.Length == 0) return;
        if (_blockPool) BlockBufferPool.Return(a);
        else ArrayPool<byte>.Shared.Return(a);
    }

    public void Advance(int count)
    {
        if ((uint)count > (uint)(_buffer.Length - _written)) throw new InvalidOperationException("Cannot advance past the end of the buffer.");
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_written);
    }

    private void Ensure(int sizeHint)
    {
        if (_buffer.Length - _written >= Math.Max(sizeHint, 1)) return;
        byte[] n = RentArray(Math.Max(_written + Math.Max(sizeHint, 1), _buffer.Length * 2));
        _buffer.AsSpan(0, _written).CopyTo(n);
        ReturnArray(_buffer);
        _buffer = n;
    }

    public byte[] Detach(out int length)
    {
        byte[] b = _buffer;
        length = _written;
        _buffer = [];
        _written = 0;
        return b;
    }

    public void Dispose()
    {
        ReturnArray(_buffer);
        _buffer = [];
        _written = 0;
    }
}
