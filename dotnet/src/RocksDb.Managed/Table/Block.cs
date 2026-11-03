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
/// returned to its pool (if it was rented: <see cref="ArrayPool{T}.Shared"/> or the internal block buffer pool, routed
/// by length) and the object is recycled.
/// </para>
/// <para>
/// <see cref="IDisposable.Dispose"/> (explicit) releases one reference, so a block can be handed to a
/// <see cref="Cache.BlockCache"/> as <c>CacheValue.FromObject(block)</c>: the cache owns that reference and releases it
/// when the entry is freed.
/// </para>
/// </summary>
public sealed class Block : IDisposable
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
    private BlockBufferSource _source;
    private int _refCount;
    // block_protection_bytes_per_key requested by the table reader that loaded this block, and the per key-value
    // checksums (computed on first verification, then shared by every iterator over the block).
    private int _kvProtectionBytes;
    private BlockKvProtection? _kvProtection;

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
    /// <summary>
    /// Length of the common user-key prefix stored at the start of the block (format_version &gt;= 8,
    /// optimize_key_common_prefix): restarts[0] when non-zero. Restart entries store their key without it.
    /// </summary>
    public int CommonPrefixLength { get; private set; }
    /// <summary>
    /// Separated key/value storage (separate_key_value_in_data_block): offset of the values section, which is also the
    /// end of the keys section; -1 for blocks with inline values.
    /// </summary>
    public int ValuesSectionOffset { get; private set; }
    /// <summary>End of the entries (keys) section: <see cref="ValuesSectionOffset"/> when separated, else <see cref="RestartOffset"/>.</summary>
    public int KeysEndOffset => ValuesSectionOffset >= 0 ? ValuesSectionOffset : RestartOffset;
    /// <summary>The "is_uniform" footer hint (uniform_cv_threshold): keys are roughly uniformly distributed.</summary>
    public bool IsUniform { get; private set; }
    /// <summary>Approximate memory charged for this block (for cache accounting).</summary>
    public int Charge => _buffer.Length;

    private Block() { }

    /// <summary>
    /// Wraps <paramref name="buffer"/>[0..<paramref name="length"/>) as a block with entries + restart array.
    /// If <paramref name="rentedFromSharedPool"/> the block takes ownership and returns the array to
    /// <see cref="ArrayPool{T}.Shared"/> (or, for the non-power-of-two sizes the table reader rents, to the internal
    /// block buffer pool) when released. Throws Corruption for a malformed block.
    /// </summary>
    public static Block Create(byte[] buffer, int length, bool rentedFromSharedPool) =>
        Create(buffer, length, rentedFromSharedPool ? BlockBufferSource.BlockBufferPool : BlockBufferSource.NotRented);

    /// <summary>
    /// <see cref="Create(byte[], int, bool)"/> with the pool <paramref name="buffer"/> came from (the block returns it
    /// there when released).
    /// </summary>
    internal static Block Create(byte[] buffer, int length, BlockBufferSource source)
    {
        Block b = RentObject();
        b._buffer = buffer;
        b._length = length;
        b._source = source;
        b._refCount = 1;
        b._kvProtectionBytes = 0;
        b._kvProtection = null;
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

    /// <summary>Port of Block::Block's footer handling (DataBlockFooter::DecodeFrom + restart validation).</summary>
    private void Parse()
    {
        HashMapOffset = -1;
        HashNumBuckets = 0;
        ValuesSectionOffset = -1;
        if (_length < 2 * sizeof(uint)) ThrowHelper.Corruption("bad block contents");
        ReadOnlySpan<byte> data = Data;
        uint packed = Coding.DecodeFixed32(data[(_length - 4)..]);
        bool hash = (packed & HashIndexBit) != 0;
        packed &= ~HashIndexBit;
        bool separatedKv = (packed & SeparatedKvBit) != 0;
        packed &= ~SeparatedKvBit;
        IsUniform = (packed & UniformKeysBit) != 0;
        packed &= ~UniformKeysBit;
        // Reserved "extended metadata" escape (bit 30): no feature defines it yet.
        if ((packed & ExtendedMetadataBit) != 0) ThrowHelper.Corruption("Unsupported extended block metadata (reserved bit set)");
        if (packed > MaxNumRestarts) ThrowHelper.Corruption("Unrecognized feature in block footer (reserved bits set)");
        int size = _length - 4;
        uint valuesSectionOffset = 0;
        if (separatedKv)
        {
            // values_section_offset precedes the packed footer word.
            if (size < sizeof(uint)) ThrowHelper.Corruption("Block too small for separated KV values section offset");
            valuesSectionOffset = Coding.DecodeFixed32(data[(size - 4)..]);
            size -= 4;
        }
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
        if (separatedKv)
        {
            if (valuesSectionOffset > (uint)RestartOffset) ThrowHelper.Corruption("bad block contents: values section offset");
            ValuesSectionOffset = (int)valuesSectionOffset;
        }
        // Validate restart offsets: empty blocks have one restart at 0; otherwise strictly increasing within keys.
        int keysEnd = KeysEndOffset;
        uint prev = 0;
        for (int i = 0; i < numRestarts; i++)
        {
            uint r = Coding.DecodeFixed32(data[(RestartOffset + i * 4)..]);
            if ((keysEnd == 0 && (i != 0 || r != 0)) || (keysEnd > 0 && r >= (uint)keysEnd) || (i > 0 && r <= prev))
                ThrowHelper.Corruption("bad block restart offset");
            prev = r;
        }
        // A non-zero restarts[0] self-signals the common-prefix section [0, restarts[0]) (no footer bit).
        CommonPrefixLength = numRestarts > 0 && keysEnd > 0 ? (int)Coding.DecodeFixed32(data[RestartOffset..]) : 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal uint GetRestartPoint(int index) => Coding.DecodeFixed32(new ReadOnlySpan<byte>(_buffer, RestartOffset + index * 4, 4));

    public void AddRef() => Interlocked.Increment(ref _refCount);

    /// <summary>
    /// Requests per key-value checksum protection (block_protection_bytes_per_key: 1, 2, 4 or 8) for this key/value
    /// block: every iterator over it verifies each entry it positions on. Set by the table reader right after loading.
    /// </summary>
    internal void RequestKvProtection(int protectionBytesPerKey) => _kvProtectionBytes = protectionBytesPerKey;

    /// <summary>The requested block_protection_bytes_per_key (0: none).</summary>
    internal int KvProtectionBytes => _kvProtectionBytes;

    /// <summary>The per key-value checksums once computed (see <see cref="PublishKvProtection"/>).</summary>
    internal BlockKvProtection? KvProtection => Volatile.Read(ref _kvProtection);

    /// <summary>Publishes the checksums computed by an iterator; the first published instance wins.</summary>
    internal BlockKvProtection PublishKvProtection(BlockKvProtection protection) =>
        Interlocked.CompareExchange(ref _kvProtection, protection, null) ?? protection;

    public void Release()
    {
        int n = Interlocked.Decrement(ref _refCount);
        if (n > 0) return;
        if (n < 0) throw new InvalidOperationException("Block released too many times.");
        byte[] buf = _buffer;
        BlockBufferSource source = _source;
        _buffer = [];
        _length = 0;
        if (buf.Length > 0) TableBuilderBuffers.Return(source, buf);
        ReturnObject(this);
    }

    /// <summary>Releases one reference (same as <see cref="Release"/>); used by the block cache when an entry is freed.</summary>
    void IDisposable.Dispose() => Release();

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
