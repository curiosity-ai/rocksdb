using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;

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
    public static Block ReadBlock(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums)
    {
        int n = CheckedLength(handle);
        byte[] buf = ArrayPool<byte>.Shared.Rent(n);
        try
        {
            int read = ReadFully(file, (long)handle.Offset, buf.AsSpan(0, n));
            if (read != n) ThrowTruncated(handle, read);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        return ProcessSerialized(buf, footer, handle, verifyChecksums);
    }

    public static async ValueTask<Block> ReadBlockAsync(IRandomAccessFile file, Footer footer, BlockHandle handle, bool verifyChecksums, CancellationToken ct)
    {
        int n = CheckedLength(handle);
        byte[] buf = ArrayPool<byte>.Shared.Rent(n);
        try
        {
            int total = 0;
            while (total < n)
            {
                int r = await file.ReadAsync((long)handle.Offset + total, buf.AsMemory(total, n - total), ct).ConfigureAwait(false);
                if (r <= 0) break;
                total += r;
            }
            if (total != n) ThrowTruncated(handle, total);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        return ProcessSerialized(buf, footer, handle, verifyChecksums);
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

    /// <summary>Takes ownership of <paramref name="buf"/> (rented) holding payload + trailer.</summary>
    private static Block ProcessSerialized(byte[] buf, in Footer footer, BlockHandle handle, bool verifyChecksums)
    {
        int size = (int)handle.Size;
        try
        {
            if (verifyChecksums && !BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, buf.AsSpan(0, size + BlockBasedTableConstants.BlockTrailerSize), handle.Offset))
                ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + size);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        var type = (CompressionType)buf[size];
        if (type == CompressionType.None) return Block.Create(buf, size, rentedFromSharedPool: true);
        try
        {
            return Decompress(type, buf.AsSpan(0, size), footer.FormatVersion);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static Block Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion)
    {
        var w = new RentedBufferWriter(Math.Max(compressed.Length * 2, 256));
        try
        {
            BlockCompression.Decompress(type, compressed, formatVersion, w);
            byte[] arr = w.Detach(out int len);
            return Block.Create(arr, len, rentedFromSharedPool: true);
        }
        finally
        {
            w.Dispose();
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
            var w = new RentedBufferWriter(size * 2);
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

/// <summary>Minimal growable writer over a rented array whose buffer can be detached (ownership transfer).</summary>
internal sealed class RentedBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;

    public RentedBufferWriter(int initialCapacity) => _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 16));

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
        byte[] n = ArrayPool<byte>.Shared.Rent(Math.Max(_written + Math.Max(sizeHint, 1), _buffer.Length * 2));
        _buffer.AsSpan(0, _written).CopyTo(n);
        ArrayPool<byte>.Shared.Return(_buffer);
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
        if (_buffer.Length > 0) ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = [];
    }
}
