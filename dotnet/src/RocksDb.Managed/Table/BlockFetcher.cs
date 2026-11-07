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
    public static Block ReadBlock(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums, CompressionDictionary? dictionary) =>
        ReadBlock(file, footer, handle, verifyChecksums, dictionary, BlockBufferSource.BlockBufferPool);

    /// <summary>
    /// Reads a block into a buffer rented from <paramref name="source"/> (the block returns it there when released):
    /// <see cref="BlockBufferSource.TableBuffers"/> for the blocks a table reader pins for its lifetime.
    /// </summary>
    public static Block ReadBlock(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums, CompressionDictionary? dictionary,
        BlockBufferSource source)
    {
        int n = CheckedLength(handle);
        byte[] buf = TableBuilderBuffers.Rent(source, n);
        try
        {
            long perfRead = PerfContext.StartTimer(); // PERF_TIMER_GUARD(block_read_time)
            int read = ReadFully(file, (long)handle.Offset, buf.AsSpan(0, n));
            PerfContext.RecordBlockRead(read, perfRead);
            if (read != n) ThrowTruncated(handle, read);
        }
        catch
        {
            TableBuilderBuffers.Return(source, buf);
            throw;
        }
        return ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary, source);
    }

    public static ValueTask<Block> ReadBlockAsync(IRandomAccessFile file, Footer footer, BlockHandle handle, bool verifyChecksums, CancellationToken ct) =>
        ReadBlockAsync(file, footer, handle, verifyChecksums, null, ct);

    public static ValueTask<Block> ReadBlockAsync(IRandomAccessFile file, Footer footer, BlockHandle handle, bool verifyChecksums,
        CompressionDictionary? dictionary, CancellationToken ct)
    {
        int n = CheckedLength(handle);
        byte[] buf = BlockBufferPool.Rent(n);
        PerfContext.RecordBlockRead(n, 0); // async: counted, not timed
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
                if (already == n) return new ValueTask<Block>(ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary, BlockBufferSource.BlockBufferPool));
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
        return ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary, BlockBufferSource.BlockBufferPool);
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
        if (type != CompressionType.None) return Decompress(type, serialized[..size], footer.FormatVersion, dictionary, BlockBufferSource.BlockBufferPool);
        byte[] buf = BlockBufferPool.Rent(Math.Max(size, 1));
        serialized[..size].CopyTo(buf);
        return Block.Create(buf, size, rentedFromSharedPool: true);
    }

    /// <summary>
    /// Reads a block like <see cref="ReadBlock(IRandomAccessFile, in Footer, BlockHandle, bool, CompressionDictionary?)"/>
    /// and, when it is stored compressed, also returns the verified serialized block (payload + trailer, compression type
    /// at <c>handle.Size</c>) in <paramref name="serialized"/>, rented from <see cref="BlockBufferPool"/>: the caller
    /// returns it. This is BlockFetcher::GetCompressedBlock for MaybeReadBlockAndLoadToCache, which offers the compressed
    /// block to the secondary cache tiers. Uncompressed blocks keep their read buffer and set it to null.
    /// </summary>
    public static Block ReadBlockKeepingCompressed(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums,
        CompressionDictionary? dictionary, out byte[]? serialized)
    {
        int n = CheckedLength(handle);
        byte[] buf = BlockBufferPool.Rent(n);
        try
        {
            long perfRead = PerfContext.StartTimer(); // PERF_TIMER_GUARD(block_read_time)
            int read = ReadFully(file, (long)handle.Offset, buf.AsSpan(0, n));
            PerfContext.RecordBlockRead(read, perfRead);
            if (read != n) ThrowTruncated(handle, read);
        }
        catch
        {
            BlockBufferPool.Return(buf);
            throw;
        }
        return ProcessSerializedKeepingCompressed(buf, footer, handle, verifyChecksums, dictionary, out serialized);
    }

    /// <summary>Async <see cref="ReadBlockKeepingCompressed"/>; the serialized block (if compressed) is <see cref="CompressedRead.Serialized"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public static async ValueTask<CompressedRead> ReadBlockKeepingCompressedAsync(IRandomAccessFile file, Footer footer, BlockHandle handle,
        bool verifyChecksums, CompressionDictionary? dictionary, CancellationToken ct)
    {
        int n = CheckedLength(handle);
        byte[] buf = BlockBufferPool.Rent(n);
        PerfContext.RecordBlockRead(n, 0); // async: counted, not timed
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
            BlockBufferPool.Return(buf);
            throw;
        }
        Block b = ProcessSerializedKeepingCompressed(buf, footer, handle, verifyChecksums, dictionary, out byte[]? serialized);
        return new CompressedRead(b, serialized);
    }

    /// <summary>A block read by <see cref="ReadBlockKeepingCompressedAsync"/> and its serialized form (null: not compressed).</summary>
    public readonly record struct CompressedRead(Block Block, byte[]? Serialized);

    // Takes ownership of buf (from BlockBufferPool); hands it back through `serialized` when the block is compressed.
    private static Block ProcessSerializedKeepingCompressed(byte[] buf, in Footer footer, BlockHandle handle, bool verifyChecksums,
        CompressionDictionary? dictionary, out byte[]? serialized)
    {
        int size = (int)handle.Size;
        var type = (CompressionType)buf[size];
        if (type == CompressionType.None)
        {
            serialized = null;
            return ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary, BlockBufferSource.BlockBufferPool);
        }
        try
        {
            if (verifyChecksums && !BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, buf.AsSpan(0, size + BlockBasedTableConstants.BlockTrailerSize), handle.Offset))
                ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + size);
            Block b = Decompress(type, buf.AsSpan(0, size), footer.FormatVersion, dictionary, BlockBufferSource.BlockBufferPool);
            serialized = buf;
            return b;
        }
        catch
        {
            BlockBufferPool.Return(buf);
            throw;
        }
    }

    /// <summary>
    /// Verifies and unpacks a serialized block (payload + trailer) held by the caller, e.g. from a persistent cache
    /// (BlockFetcher's ProcessTrailerIfPresent + decompression on a serialized persistent cache hit).
    /// </summary>
    internal static Block ProcessSerializedCopy(ReadOnlySpan<byte> serialized, in Footer footer, BlockHandle handle, bool verifyChecksums,
        CompressionDictionary? dictionary)
    {
        byte[] buf = BlockBufferPool.Rent(Math.Max(serialized.Length, 1));
        serialized.CopyTo(buf);
        return ProcessSerialized(buf, footer, handle, verifyChecksums, dictionary, BlockBufferSource.BlockBufferPool);
    }

    /// <summary>Takes ownership of <paramref name="buf"/> (rented from <paramref name="source"/>) holding payload + trailer.</summary>
    private static Block ProcessSerialized(byte[] buf, in Footer footer, BlockHandle handle, bool verifyChecksums, CompressionDictionary? dictionary,
        BlockBufferSource source)
    {
        int size = (int)handle.Size;
        try
        {
            if (verifyChecksums && !BlockChecksum.Verify(footer.ChecksumType, footer.BaseContextChecksum, buf.AsSpan(0, size + BlockBasedTableConstants.BlockTrailerSize), handle.Offset))
                ThrowHelper.Corruption("block checksum mismatch at offset " + handle.Offset + " size " + size);
        }
        catch
        {
            TableBuilderBuffers.Return(source, buf);
            throw;
        }
        var type = (CompressionType)buf[size];
        if (type == CompressionType.None) return Block.Create(buf, size, source);
        try
        {
            return Decompress(type, buf.AsSpan(0, size), footer.FormatVersion, dictionary, source);
        }
        finally
        {
            TableBuilderBuffers.Return(source, buf);
        }
    }

    [ThreadStatic] private static RentedBufferWriter? t_decompressWriter;

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static Block Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion) =>
        Decompress(type, compressed, formatVersion, null, BlockBufferSource.BlockBufferPool);

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static Block Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, CompressionDictionary? dictionary) =>
        Decompress(type, compressed, formatVersion, dictionary, BlockBufferSource.BlockBufferPool);

    /// <summary>Decompresses into a block whose buffer is rented from <paramref name="source"/>.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static Block Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, CompressionDictionary? dictionary,
        BlockBufferSource source)
    {
        // No initial buffer: the decompressors request the exact uncompressed size (stored in the block or the snappy
        // header) with a single GetSpan, so the block gets one right-sized array and no grow-and-copy.
        // Per-thread writer (it holds no buffer between calls: Detach/Dispose leave it empty), so a block read does not
        // allocate the writer object.
        RentedBufferWriter w = t_decompressWriter ?? new RentedBufferWriter(0, BlockBufferSource.BlockBufferPool);
        t_decompressWriter = null;
        try
        {
            w.Source = source;
            BlockCompression.Decompress(type, compressed, formatVersion, w, dictionary);

            byte[] arr = w.Detach(out int len);
            return Block.Create(arr, len, source);
        }
        finally
        {
            w.Dispose();
            t_decompressWriter = w;
        }
    }

    /// <summary>
    /// Unpacks a serialized block's payload into an array rented from <see cref="RocksArrayPool{T}.Shared"/> (used for filters,
    /// which aren't parsed as blocks).
    /// </summary>
    public static byte[] ReadRawPayload(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums, out int length) =>
        ReadRawPayload(file, footer, handle, verifyChecksums, BlockBufferSource.Shared, out length);

    /// <summary>
    /// Unpacks a serialized block's payload into an array rented from <paramref name="source"/> (the caller returns it
    /// there).
    /// </summary>
    public static byte[] ReadRawPayload(IRandomAccessFile file, in Footer footer, BlockHandle handle, bool verifyChecksums, BlockBufferSource source,
        out int length)
    {
        int n = CheckedLength(handle);
        byte[] buf = TableBuilderBuffers.Rent(source, n);
        try
        {
            long perfRead = PerfContext.StartTimer(); // PERF_TIMER_GUARD(block_read_time)
            int read = ReadFully(file, (long)handle.Offset, buf.AsSpan(0, n));
            PerfContext.RecordBlockRead(read, perfRead);
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
            var w = new RentedBufferWriter(size * 2, source);
            try
            {
                BlockCompression.Decompress(type, buf.AsSpan(0, size), footer.FormatVersion, w);
                TableBuilderBuffers.Return(source, buf);
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
            if (buf.Length > 0) TableBuilderBuffers.Return(source, buf);
            throw;
        }
    }
}

/// <summary>
/// Minimal growable writer over a rented array whose buffer can be detached (ownership transfer). Arrays come from
/// <see cref="Source"/>: <see cref="BlockBufferPool"/> for buffers that become <see cref="Block"/>s,
/// <see cref="RocksArrayPool{T}.Shared"/> for buffers handed to code that returns them there (e.g. cached filters), or the
/// table buffers for the blocks a table reader pins.
/// </summary>
internal sealed class RentedBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;
    private BlockBufferSource _source;

    public RentedBufferWriter(int initialCapacity, BlockBufferSource source)
    {
        _source = source;
        _buffer = initialCapacity <= 0 ? [] : RentArray(Math.Max(initialCapacity, 16));
    }

    /// <summary>The pool buffers are rented from; can only be changed while the writer holds no buffer.</summary>
    public BlockBufferSource Source
    {
        get => _source;
        set
        {
            if (_buffer.Length != 0) throw new InvalidOperationException("The writer holds a buffer.");
            _source = value;
        }
    }

    private byte[] RentArray(int n) => TableBuilderBuffers.Rent(_source, n);

    private void ReturnArray(byte[] a)
    {
        if (a.Length == 0) return;
        TableBuilderBuffers.Return(_source, a);
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
