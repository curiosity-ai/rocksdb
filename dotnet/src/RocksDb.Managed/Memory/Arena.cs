using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Memory;

/// <summary>
/// Bump allocator interface used by memtable data structures. Port of rocksdb::Allocator (memory/allocator.h).
/// Returned memory stays valid (and at a fixed address) until the allocator is disposed.
/// </summary>
public abstract unsafe class Allocator
{
    /// <summary>Allocates <paramref name="bytes"/> (&gt; 0) with no alignment guarantee.</summary>
    public abstract byte* Allocate(int bytes);
    /// <summary>Allocates <paramref name="bytes"/> (&gt; 0) aligned to at least 8 bytes (pointer size).</summary>
    public abstract byte* AllocateAligned(int bytes);
    public abstract int BlockSize { get; }
}

/// <summary>
/// Single-threaded arena. Port of rocksdb::Arena (memory/arena.h, memory/arena.cc).
/// </summary>
/// <remarks>
/// <para><b>Memory source.</b> Blocks ("slabs") are <c>byte[]</c> allocated on the Pinned Object Heap
/// (<c>GC.AllocateUninitializedArray(pinned: true)</c>) and recycled through <see cref="ArenaSlabPool"/>.
/// This was chosen over <c>NativeMemory</c> because it gives the same raw-pointer performance (addresses never
/// move, so the skiplist can store <c>byte*</c> links and use <see cref="Interlocked"/> on them) while staying
/// memory-safe if an owner forgets to dispose: the GC still reclaims the slabs (no leak), and the POH is not
/// compacted so pinning costs nothing. Slabs are uninitialized; callers must write before reading.</para>
/// <para><b>Lifetime.</b> <see cref="Dispose"/> returns regular slabs to the pool, after which every pointer handed out
/// is invalid. Owners (MemTable) must only dispose once no reader can still access the memory (ref counting).</para>
/// <para>Like upstream, aligned allocations grow from the front of the current block and unaligned ones from the
/// back, so mixed allocations do not waste alignment slop. Requests larger than a quarter block get a dedicated
/// ("irregular") block.</para>
/// </remarks>
public sealed unsafe class Arena : Allocator, IDisposable
{
    public const int MinBlockSize = 4096;
    /// <summary>Upstream allows 2 GiB; we cap at 1 GiB to stay within <c>int</c> sized arrays.</summary>
    public const int MaxBlockSize = 1 << 30;
    /// <summary>alignof(std::max_align_t) on 64-bit platforms.</summary>
    public const int AlignUnit = 16;

    private readonly int _blockSize;
    private readonly List<byte[]> _blocks = new();
    private byte* _unalignedAllocPtr;
    private byte* _alignedAllocPtr;
    private int _allocBytesRemaining;
    private long _blocksMemory;
    private int _irregularBlockNum;
    private bool _disposed;
    private readonly AllocTracker? _tracker;
    /// <summary>
    /// The first <see cref="InlineSize"/> bytes come from this block (upstream: the inline_block_ member), so an empty
    /// memtable (its skip list heads) costs 2 KiB, not a whole arena block. Pinned like the slabs; not in <see cref="_blocks"/>.
    /// </summary>
    private readonly byte[] _inlineBlock;

    /// <summary>kInlineSize (memory/arena.h).</summary>
    public const int InlineSize = 2048;

    public Arena(int blockSize) : this(blockSize, null) { }

    /// <summary>
    /// An arena whose block allocations are reported to <paramref name="tracker"/> (Arena(block_size, tracker)). Port of
    /// Arena::Arena (memory/arena.cc): allocation starts in the inline block, which is counted in the memory allocated and
    /// reported to the tracker.
    /// </summary>
    internal Arena(int blockSize, AllocTracker? tracker)
    {
        _blockSize = OptimizeBlockSize(blockSize);
        _tracker = tracker;
        _inlineBlock = GC.AllocateUninitializedArray<byte>(InlineSize + AlignUnit, pinned: true);
        // POH data is only 8-aligned: align the start, keeping InlineSize usable bytes.
        byte* head = AlignUp((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_inlineBlock)));
        _allocBytesRemaining = InlineSize;
        _blocksMemory += _allocBytesRemaining;
        _alignedAllocPtr = head;
        _unalignedAllocPtr = head + _allocBytesRemaining;
        _tracker?.Allocate(InlineSize);
    }

    public Arena() : this(MinBlockSize) { }

    /// <summary>Port of Arena::OptimizeBlockSize: clamp to [Min, Max] and round up to <see cref="AlignUnit"/>.</summary>
    public static int OptimizeBlockSize(long blockSize)
    {
        blockSize = Math.Clamp(blockSize, MinBlockSize, MaxBlockSize);
        if (blockSize % AlignUnit != 0) blockSize = (1 + blockSize / AlignUnit) * AlignUnit;
        return (int)Math.Min(blockSize, MaxBlockSize);
    }

    public override int BlockSize => _blockSize;

    /// <summary>Bytes of memory owned by the arena minus the unused tail of the current block (upstream formula).</summary>
    public long ApproximateMemoryUsage => _blocksMemory + _blocks.Count * (long)sizeof(nint) - _allocBytesRemaining;

    /// <summary>Total bytes of all blocks allocated.</summary>
    public long MemoryAllocatedBytes => _blocksMemory;

    /// <summary>Bytes still free in the current block.</summary>
    public int AllocatedAndUnused => _allocBytesRemaining;

    /// <summary>Number of dedicated blocks allocated for large requests.</summary>
    public int IrregularBlockNum => _irregularBlockNum;

    /// <summary>True until the first block is allocated: allocations still come from the inline block (IsInInlineBlock).</summary>
    public bool IsEmpty => _blocks.Count == 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte* Allocate(int bytes)
    {
        Debug.Assert(bytes > 0);
        if ((uint)bytes <= (uint)_allocBytesRemaining)
        {
            _unalignedAllocPtr -= bytes;
            _allocBytesRemaining -= bytes;
            return _unalignedAllocPtr;
        }
        return AllocateFallback(bytes, aligned: false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte* AllocateAligned(int bytes)
    {
        Debug.Assert(bytes > 0);
        int currentMod = (int)((nuint)_alignedAllocPtr & (AlignUnit - 1));
        int slop = currentMod == 0 ? 0 : AlignUnit - currentMod;
        int needed = bytes + slop;
        if ((uint)needed <= (uint)_allocBytesRemaining)
        {
            byte* result = _alignedAllocPtr + slop;
            _alignedAllocPtr += needed;
            _allocBytesRemaining -= needed;
            return result;
        }
        return AllocateFallback(bytes, aligned: true);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private byte* AllocateFallback(int bytes, bool aligned)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (bytes > _blockSize / 4)
        {
            ++_irregularBlockNum;
            // Object is more than a quarter of our block size. Allocate it separately to avoid wasting too much
            // space in leftover bytes. Over-allocate by AlignUnit so the result can be aligned.
            byte* p = AllocateNewBlock(bytes + AlignUnit, pooled: false);
            return AlignUp(p);
        }

        // We waste the remaining space in the current block.
        byte* blockHead = AlignUp(AllocateNewBlock(_blockSize, pooled: true));
        int size = _blockSize - AlignUnit; // usable size after aligning the head (POH data is only 8-aligned)
        _allocBytesRemaining = size - bytes;
        if (aligned)
        {
            _alignedAllocPtr = blockHead + bytes;
            _unalignedAllocPtr = blockHead + size;
            return blockHead;
        }
        _alignedAllocPtr = blockHead;
        _unalignedAllocPtr = blockHead + size - bytes;
        return _unalignedAllocPtr;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* AlignUp(byte* p) => (byte*)(((nuint)p + (AlignUnit - 1)) & ~(nuint)(AlignUnit - 1));

    private byte* AllocateNewBlock(int blockBytes, bool pooled)
    {
        byte[] block = pooled ? ArenaSlabPool.Rent(blockBytes) : GC.AllocateUninitializedArray<byte>(blockBytes, pinned: true);
        _blocks.Add(block);
        _blocksMemory += block.Length;
        _tracker?.Allocate(block.Length);
        // Pinned (POH) arrays never move, so the address is stable for the lifetime of the array.
        return (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(block));
    }

    /// <summary>Returns the blocks to <see cref="ArenaSlabPool"/>. All pointers handed out become invalid.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (byte[] b in _blocks)
        {
            if (b.Length == _blockSize) ArenaSlabPool.Return(b);
        }
        _blocks.Clear();
        _allocBytesRemaining = 0;
        _alignedAllocPtr = _unalignedAllocPtr = null;
    }
}

/// <summary>
/// Process-wide pool of pinned arena slabs, so that flushing and recreating memtables does not churn the Pinned
/// Object Heap. Bounded by <see cref="MaxPooledBytes"/>; overflow slabs are left to the GC.
/// </summary>
public static class ArenaSlabPool
{
    private static readonly Lock s_lock = new();
    private static readonly Dictionary<int, Stack<byte[]>> s_bySize = new();
    private static long s_pooledBytes;

    /// <summary>Upper bound on bytes retained by the pool (default 256 MiB).</summary>
    public static long MaxPooledBytes { get; set; } = 256L << 20;

    /// <summary>Bytes currently retained.</summary>
    public static long PooledBytes => Volatile.Read(ref s_pooledBytes);

    /// <summary>Gets a pinned, uninitialized array of exactly <paramref name="size"/> bytes.</summary>
    public static byte[] Rent(int size)
    {
        lock (s_lock)
        {
            if (s_bySize.TryGetValue(size, out Stack<byte[]>? stack) && stack.TryPop(out byte[]? arr))
            {
                s_pooledBytes -= arr.Length;
                return arr;
            }
        }
        return GC.AllocateUninitializedArray<byte>(size, pinned: true);
    }

    /// <summary>Returns a slab obtained from <see cref="Rent"/>. The caller must not use it afterwards.</summary>
    public static void Return(byte[] slab)
    {
        lock (s_lock)
        {
            if (s_pooledBytes + slab.Length > MaxPooledBytes) return;
            if (!s_bySize.TryGetValue(slab.Length, out Stack<byte[]>? stack))
            {
                stack = new Stack<byte[]>();
                s_bySize.Add(slab.Length, stack);
            }
            stack.Push(slab);
            s_pooledBytes += slab.Length;
        }
    }

    /// <summary>Drops all pooled slabs (tests / memory pressure).</summary>
    public static void Trim()
    {
        lock (s_lock)
        {
            s_bySize.Clear();
            s_pooledBytes = 0;
        }
    }
}
