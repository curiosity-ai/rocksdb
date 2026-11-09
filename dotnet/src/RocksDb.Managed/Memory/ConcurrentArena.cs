using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Memory;

/// <summary>
/// Thread-safe arena with per-core shards. Port of rocksdb::ConcurrentArena (memory/concurrent_arena.h,
/// memory/concurrent_arena.cc).
/// </summary>
/// <remarks>
/// Small allocations are carved from per-core shard chunks (each <c>min(128 KiB, blockSize / 8)</c>) under a
/// per-shard spin lock, so concurrent memtable inserts on different cores do not contend. A thread that has
/// never seen contention (thread-local cpu id 0) allocates directly from the backing <see cref="Arena"/> under
/// <c>TryLock</c>, which keeps single-threaded memory accounting tight (exactly like upstream). Large requests
/// (&gt; shard block / 4) always go to the backing arena.
/// <para>Memory statistics are mirrored into fields updated under the arena lock and read without locking
/// (relaxed), matching upstream's <c>Fixup()</c>.</para>
/// </remarks>
public sealed unsafe class ConcurrentArena : Allocator, IDisposable
{
    private const int MaxShardBlockSize = 128 * 1024;

    // Per-thread shard hint; 0 means "never contended, use the arena directly". Shared by all arenas like
    // upstream's static thread_local tls_cpuid.
    [ThreadStatic] private static int t_cpuid;

    /// <summary>One shard per cache-line pair to avoid false sharing (upstream pads to 64 bytes; 128 covers adjacent-line prefetch).</summary>
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct Shard
    {
        [FieldOffset(64)] public int Lock;
        [FieldOffset(72)] public byte* FreeBegin;
        [FieldOffset(80)] public long AllocatedAndUnused;
    }

    private readonly int _shardBlockSize;
    private readonly Shard[] _shards;
    private readonly int _shardMask;
    private readonly Arena _arena;
    private int _arenaLock;
    private long _arenaAllocatedAndUnused;
    private long _memoryAllocatedBytes;
    private int _irregularBlockNum;

    public ConcurrentArena(int blockSize) : this(blockSize, null) { }

    /// <summary>An arena reporting its blocks to <paramref name="tracker"/> (ConcurrentArena(block_size, tracker)).</summary>
    internal ConcurrentArena(int blockSize, AllocTracker? tracker)
    {
        _arena = new Arena(blockSize, tracker);
        _shardBlockSize = Math.Min(MaxShardBlockSize, _arena.BlockSize / 8);
        int n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, Environment.ProcessorCount));
        _shards = new Shard[n];
        _shardMask = n - 1;
        Fixup();
    }

    public ConcurrentArena() : this(Arena.MinBlockSize) { }

    public override int BlockSize => _arena.BlockSize;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte* Allocate(int bytes) => AllocateImpl(bytes, aligned: false);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte* AllocateAligned(int bytes)
    {
        // Round up to a multiple of the pointer size so the shard path hands it out from the (aligned) front.
        int roundedUp = ((bytes - 1) | (sizeof(nint) - 1)) + 1;
        return AllocateImpl(roundedUp, aligned: true);
    }

    /// <summary>Memory in use (allocated blocks minus unused tails, including unused shard chunks).</summary>
    public long ApproximateMemoryUsage
    {
        get
        {
            SpinLock(ref _arenaLock);
            try
            {
                return _arena.ApproximateMemoryUsage - ShardAllocatedAndUnused();
            }
            finally
            {
                Unlock(ref _arenaLock);
            }
        }
    }

    public long MemoryAllocatedBytes => Volatile.Read(ref _memoryAllocatedBytes);
    public long AllocatedAndUnused => Volatile.Read(ref _arenaAllocatedAndUnused) + ShardAllocatedAndUnused();
    public int IrregularBlockNum => Volatile.Read(ref _irregularBlockNum);

    private long ShardAllocatedAndUnused()
    {
        long total = 0;
        Shard[] shards = _shards;
        for (int i = 0; i < shards.Length; i++) total += Volatile.Read(ref shards[i].AllocatedAndUnused);
        return total;
    }

    private byte* AllocateImpl(int bytes, bool aligned)
    {
        int cpu = t_cpuid;
        // Go directly to the arena if the allocation is too large, or if we've never needed to Repick() and the
        // arena mutex is available. Shard 0 having space means some thread was contended; then use shards.
        if (bytes > _shardBlockSize / 4)
        {
            SpinLock(ref _arenaLock);
            return AllocateFromArenaLocked(bytes, aligned);
        }
        if (cpu == 0 && Volatile.Read(ref _shards[0].AllocatedAndUnused) == 0 && TryLock(ref _arenaLock))
        {
            return AllocateFromArenaLocked(bytes, aligned);
        }

        // Pick a shard from which to allocate.
        ref Shard s = ref _shards[cpu & _shardMask];
        if (!TryLock(ref s.Lock))
        {
            s = ref Repick();
            SpinLock(ref s.Lock);
        }
        try
        {
            long avail = s.AllocatedAndUnused;
            if (avail < bytes)
            {
                // Reload.
                SpinLock(ref _arenaLock);
                try
                {
                    // If the arena's current block is within a factor of 2 of the right size, we adjust our request
                    // to avoid arena waste.
                    long exact = _arena.AllocatedAndUnused;
                    if (exact >= bytes && _arena.IsEmpty)
                    {
                        // If we haven't exhausted the arena's inline block yet, allocate from the arena directly, so the
                        // first few small allocations of a memtable (about 1 KB at creation) do not take a full arena
                        // block, which matters with thousands of empty memtables.
                        byte* direct = aligned ? _arena.AllocateAligned(bytes) : _arena.Allocate(bytes);
                        Fixup();
                        return direct;
                    }
                    avail = exact >= _shardBlockSize / 2 && exact < _shardBlockSize * 2 ? exact : _shardBlockSize;
                    s.FreeBegin = _arena.AllocateAligned((int)avail);
                    Fixup();
                }
                finally
                {
                    Unlock(ref _arenaLock);
                }
            }
            Volatile.Write(ref s.AllocatedAndUnused, avail - bytes);

            byte* rv;
            if ((bytes % sizeof(nint)) == 0)
            {
                // Aligned allocation from the beginning.
                rv = s.FreeBegin;
                s.FreeBegin += bytes;
            }
            else
            {
                // Unaligned from the end.
                rv = s.FreeBegin + avail - bytes;
            }
            return rv;
        }
        finally
        {
            Unlock(ref s.Lock);
        }
    }

    private byte* AllocateFromArenaLocked(int bytes, bool aligned)
    {
        try
        {
            byte* rv = aligned ? _arena.AllocateAligned(bytes) : _arena.Allocate(bytes);
            Fixup();
            return rv;
        }
        finally
        {
            Unlock(ref _arenaLock);
        }
    }

    private ref Shard Repick()
    {
        // Even if we are cpu 0, use a non-zero t_cpuid so we can tell we have repicked.
        int idx = Thread.GetCurrentProcessorId() & _shardMask;
        t_cpuid = idx | _shards.Length;
        return ref _shards[idx];
    }

    private void Fixup()
    {
        Volatile.Write(ref _arenaAllocatedAndUnused, _arena.AllocatedAndUnused);
        Volatile.Write(ref _memoryAllocatedBytes, _arena.MemoryAllocatedBytes);
        Volatile.Write(ref _irregularBlockNum, _arena.IrregularBlockNum);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryLock(ref int l) => Volatile.Read(ref l) == 0 && Interlocked.CompareExchange(ref l, 1, 0) == 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SpinLock(ref int l)
    {
        if (Interlocked.CompareExchange(ref l, 1, 0) != 0) SpinLockSlow(ref l);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void SpinLockSlow(ref int l)
    {
        SpinWait sw = default;
        do
        {
            sw.SpinOnce(sleep1Threshold: -1);
        } while (Volatile.Read(ref l) != 0 || Interlocked.CompareExchange(ref l, 1, 0) != 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Unlock(ref int l) => Volatile.Write(ref l, 0);

    /// <summary>Releases all blocks (see <see cref="Arena.Dispose"/>). No allocation may be in flight.</summary>
    public void Dispose()
    {
        SpinLock(ref _arenaLock);
        try
        {
            _arena.Dispose();
            for (int i = 0; i < _shards.Length; i++)
            {
                _shards[i].FreeBegin = null;
                _shards[i].AllocatedAndUnused = 0;
            }
            Fixup();
        }
        finally
        {
            Unlock(ref _arenaLock);
        }
    }
}
