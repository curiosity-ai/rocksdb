using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// A parsed, uncompressed block held in a (usually pooled) byte array. Port of the parsing part of
/// <c>Block</c> (table/block_based/block.{h,cc}) and <c>DataBlockFooter::DecodeFrom</c>
/// (table/block_based/data_block_footer.cc).
/// <para>
/// Lifetime: blocks are reference counted so a block cache and any number of iterators can share one instance.
/// A new block starts with a reference count of 1 owned by the creator. When the count reaches 0 the buffer is
/// returned to <see cref="ArrayPool{T}.Shared"/> (if it was rented) and the object is recycled.
/// </para>
/// </summary>
public sealed class Block
{
    private const uint HashIndexBit = 1u << 31;
    private const uint ExtendedMetadataBit = 1u << 30;
    private const uint UniformKeysBit = 1u << 29;
    private const uint SeparatedKvBit = 1u << 28;
    private const uint MaxNumRestarts = (1u << 28) - 1;

    [ThreadStatic] private static Block?[]? t_pool;
    [ThreadStatic] private static int t_poolCount;

    private byte[] _buffer = [];
    private int _length;
    private bool _rented;
    private int _refCount;

    /// <summary>Uncompressed block bytes.</summary>
    public ReadOnlySpan<byte> Data => new(_buffer, 0, _length);
    public ReadOnlyMemory<byte> Memory => new(_buffer, 0, _length);
    internal byte[] Buffer => _buffer;
    public int Length => _length;
    /// <summary>Offset of the restart array (= end of the entries section).</summary>
    public int RestartOffset { get; private set; }
    public int NumRestarts { get; private set; }
    /// <summary>Hash index bucket array offset, or -1 if the block has no data block hash index.</summary>
    public int HashMapOffset { get; private set; }
    public int HashNumBuckets { get; private set; }
    public bool HasHashIndex => HashMapOffset >= 0;
    /// <summary>Approximate memory charged for this block (for cache accounting).</summary>
    public int Charge => _buffer.Length;

    private Block() { }

    /// <summary>
    /// Wraps <paramref name="buffer"/>[0..<paramref name="length"/>) as a block with entries + restart array.
    /// If <paramref name="rentedFromSharedPool"/> the block takes ownership and returns the array to
    /// <see cref="ArrayPool{T}.Shared"/> when released. Throws Corruption for a malformed block.
    /// </summary>
    public static Block Create(byte[] buffer, int length, bool rentedFromSharedPool)
    {
        Block b = RentObject();
        b._buffer = buffer;
        b._length = length;
        b._rented = rentedFromSharedPool;
        b._refCount = 1;
        try
        {
            b.Parse();
        }
        catch
        {
            b.Release();
            throw;
        }
        return b;
    }

    /// <summary>Creates a block over a private copy of <paramref name="contents"/> (rented buffer).</summary>
    public static Block CreateCopy(ReadOnlySpan<byte> contents)
    {
        byte[] buf = ArrayPool<byte>.Shared.Rent(Math.Max(contents.Length, 1));
        contents.CopyTo(buf);
        return Create(buf, contents.Length, true);
    }

    private void Parse()
    {
        HashMapOffset = -1;
        HashNumBuckets = 0;
        if (_length < 2 * sizeof(uint)) ThrowHelper.Corruption("bad block contents");
        ReadOnlySpan<byte> data = Data;
        uint packed = Coding.DecodeFixed32(data[(_length - 4)..]);
        bool hash = (packed & HashIndexBit) != 0;
        packed &= ~HashIndexBit;
        if ((packed & SeparatedKvBit) != 0) ThrowHelper.NotSupported("separated key/value data blocks (format_version 8) are not supported");
        packed &= ~UniformKeysBit; // informational only
        if ((packed & ExtendedMetadataBit) != 0) ThrowHelper.Corruption("Unsupported extended block metadata (reserved bit set)");
        if (packed > MaxNumRestarts) ThrowHelper.Corruption("Unrecognized feature in block footer (reserved bits set)");
        int size = _length - 4;
        int numRestarts = (int)packed;
        if (hash)
        {
            if (!DataBlockHashIndex.TryInitialize(data, size, out int mapOffset, out int numBuckets)) ThrowHelper.Corruption("bad data block hash index");
            HashMapOffset = mapOffset;
            HashNumBuckets = numBuckets;
            size = mapOffset;
        }
        if ((long)numRestarts * 4 > size) ThrowHelper.Corruption("bad block contents: too many restarts");
        RestartOffset = size - numRestarts * 4;
        NumRestarts = numRestarts;
        // Validate restart offsets: empty blocks have one restart at 0; otherwise strictly increasing within keys.
        uint prev = 0;
        for (int i = 0; i < numRestarts; i++)
        {
            uint r = Coding.DecodeFixed32(data[(RestartOffset + i * 4)..]);
            if ((RestartOffset == 0 && (i != 0 || r != 0)) || (RestartOffset > 0 && r >= (uint)RestartOffset) || (i > 0 && r <= prev))
                ThrowHelper.Corruption("bad block restart offset");
            prev = r;
        }
        if (numRestarts > 0 && RestartOffset > 0 && Coding.DecodeFixed32(data[RestartOffset..]) != 0)
            ThrowHelper.NotSupported("blocks with a common key prefix section (format_version 8) are not supported");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal uint GetRestartPoint(int index) => Coding.DecodeFixed32(new ReadOnlySpan<byte>(_buffer, RestartOffset + index * 4, 4));

    public void AddRef() => Interlocked.Increment(ref _refCount);

    public void Release()
    {
        int n = Interlocked.Decrement(ref _refCount);
        if (n > 0) return;
        if (n < 0) throw new InvalidOperationException("Block released too many times.");
        byte[] buf = _buffer;
        bool rented = _rented;
        _buffer = [];
        _length = 0;
        if (rented && buf.Length > 0) ArrayPool<byte>.Shared.Return(buf);
        ReturnObject(this);
    }

    private static Block RentObject()
    {
        Block?[]? pool = t_pool;
        if (pool is not null && t_poolCount > 0)
        {
            Block b = pool[--t_poolCount]!;
            pool[t_poolCount] = null;
            return b;
        }
        return new Block();
    }

    private static void ReturnObject(Block b)
    {
        Block?[] pool = t_pool ??= new Block?[32];
        if (t_poolCount < pool.Length) pool[t_poolCount++] = b;
    }
}
