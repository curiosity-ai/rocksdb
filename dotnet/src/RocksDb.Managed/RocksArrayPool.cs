// Port of the .NET runtime's System.Buffers.SharedArrayPool<T> (the implementation behind ArrayPool<T>.Shared),
// src/libraries/System.Private.CoreLib/src/System/Buffers/SharedArrayPool.cs, Utilities.cs and
// src/libraries/System.Private.CoreLib/src/System/Gen2GcCallback.cs at v10.0.0.
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Buffers;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;

namespace RocksDb.Managed;

/// <summary>
/// The array pool of RocksDb.Managed: a private instance of the same algorithm as <see cref="ArrayPool{T}.Shared"/>
/// (a copy of the runtime's <c>SharedArrayPool&lt;T&gt;</c>). Every pooled buffer of the engine (blocks, keys, values,
/// I/O buffers, cache entries) comes from here instead of <see cref="ArrayPool{T}.Shared"/>, so code elsewhere in the
/// process that misuses the process-wide shared pool (returning an array twice, writing to an array after returning
/// it) cannot hand our buffers to someone else, or theirs to us, and corrupt data the engine reads or writes.
/// </summary>
/// <remarks>
/// <para>Same behaviour and performance as the shared pool: arrays of 16 · 2^n elements (n = 0..26), one array per size
/// cached per thread, then per-core bounded stacks (32 arrays per size per core) behind short locks, trimmed after
/// gen 2 GCs depending on memory pressure. <see cref="ArrayPool{T}.Rent"/> may return a larger array than requested;
/// <see cref="ArrayPool{T}.Return"/> throws <see cref="ArgumentException"/> for an array whose length is not a pool size
/// (at most 2^30 elements; larger arrays are not pooled and are dropped).</para>
/// <para>Public extension points that exchange buffers with the engine (<c>CacheValue</c> with
/// <c>ReturnBufferToPool</c>, <c>PersistentCache.TryLookup</c>, <c>PooledValue</c>) use this pool. Thread-safe.</para>
/// </remarks>
public static class RocksArrayPool<T>
{
    /// <summary>The pool instance.</summary>
    public static ArrayPool<T> Shared { get; } = new PrivateSharedArrayPool<T>();
}

/// <summary>
/// The pool behind <see cref="RocksArrayPool{T}.Shared"/>: the runtime's tiered scheme, a small per-thread cache for
/// each array size, followed by a cache per array size shared by all threads, split into per-core stacks meant to be
/// used by threads running on that core. Locks protect each per-core stack, because a thread can migrate after
/// checking its processor number, because multiple threads could interleave on the same core, and because a thread is
/// allowed to check other cores' buckets if its core's bucket is empty/full. (Event-source logging and the
/// environment-variable overrides of the runtime version are not ported.)
/// </summary>
internal sealed class PrivateSharedArrayPool<T> : ArrayPool<T>
{
    /// <summary>The number of buckets (array sizes) in the pool, one for each array length, starting from length 16.</summary>
    private const int NumBuckets = 27; // SelectBucketIndex(1024 * 1024 * 1024 + 1)

    /// <summary>A per-thread array of arrays, to cache one array per array size per thread (one per closed T).</summary>
    [ThreadStatic]
    private static PoolThreadLocalArray[]? t_tlsBuckets;

    /// <summary>Used to keep track of all thread local buckets for trimming if needed.</summary>
    private readonly ConditionalWeakTable<PoolThreadLocalArray[], object?> _allTlsBuckets = new();

    /// <summary>Per-core partitions per array size, lazily created to avoid overhead for unused sizes.</summary>
    private readonly PoolPartitions?[] _buckets = new PoolPartitions[NumBuckets];

    /// <summary>Whether the callback to trim arrays in response to memory pressure has been created.</summary>
    private bool _trimCallbackCreated;

    private PoolPartitions CreatePerCorePartitions(int bucketIndex)
    {
        var inst = new PoolPartitions();
        return Interlocked.CompareExchange(ref _buckets[bucketIndex], inst, null) ?? inst;
    }

    public override T[] Rent(int minimumLength)
    {
        T[]? buffer;

        // The bucket number for the array length; out of range for too large a value or for 0 and negative values.
        int bucketIndex = SelectBucketIndex(minimumLength);

        // First, try to get an array from TLS if possible.
        PoolThreadLocalArray[]? tlsBuckets = t_tlsBuckets;
        if (tlsBuckets is not null && (uint)bucketIndex < (uint)tlsBuckets.Length)
        {
            buffer = Unsafe.As<T[]?>(tlsBuckets[bucketIndex].Array);
            if (buffer is not null)
            {
                tlsBuckets[bucketIndex].Array = null;
                return buffer;
            }
        }

        // Next, try to get an array from one of the partitions.
        PoolPartitions?[] perCoreBuckets = _buckets;
        if ((uint)bucketIndex < (uint)perCoreBuckets.Length)
        {
            PoolPartitions? b = perCoreBuckets[bucketIndex];
            if (b is not null)
            {
                buffer = Unsafe.As<T[]?>(b.TryPop());
                if (buffer is not null) return buffer;
            }

            // No buffer available: allocate the bucket's size so the array can be returned later.
            minimumLength = GetMaxSizeForBucket(bucketIndex);
        }
        else if (minimumLength == 0)
        {
            // Zero-length requests are valid; the pool is effectively infinite for empty arrays.
            return [];
        }
        else
        {
            ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        }

        return GC.AllocateUninitializedArray<T>(minimumLength);
    }

    public override void Return(T[] array, bool clearArray = false)
    {
        ArgumentNullException.ThrowIfNull(array);

        // Determine with what bucket this array length is associated.
        int bucketIndex = SelectBucketIndex(array.Length);

        // Make sure the TLS buckets are initialized (also for erroneous or too-large arrays: rare).
        PoolThreadLocalArray[] tlsBuckets = t_tlsBuckets ?? InitializeTlsBucketsAndTrimming();

        if ((uint)bucketIndex < (uint)tlsBuckets.Length)
        {
            if (clearArray) Array.Clear(array);

            // Check that the buffer has the size of this bucket.
            if (array.Length != GetMaxSizeForBucket(bucketIndex))
                throw new ArgumentException("The buffer is not associated with this pool and may not be returned to it.", nameof(array));

            // Store the array into the TLS bucket. If there's already an array in it, push that one down into the
            // partitions, keeping the latest one in TLS for better locality.
            ref PoolThreadLocalArray tla = ref tlsBuckets[bucketIndex];
            Array? prev = tla.Array;
            tla = new PoolThreadLocalArray(array);
            if (prev is not null)
            {
                PoolPartitions partitionsForArraySize = _buckets[bucketIndex] ?? CreatePerCorePartitions(bucketIndex);
                partitionsForArraySize.TryPush(prev);
            }
        }
    }

    /// <summary>Trims the pool after a gen 2 GC (see the runtime's SharedArrayPool.Trim). Always returns true.</summary>
    internal bool Trim()
    {
        int currentMilliseconds = Environment.TickCount;
        MemoryPressure pressure = GetMemoryPressure();

        // Trim each of the per-core buckets.
        PoolPartitions?[] perCoreBuckets = _buckets;
        for (int i = 0; i < perCoreBuckets.Length; i++) perCoreBuckets[i]?.Trim(currentMilliseconds, pressure);

        // Trim each of the TLS buckets. Threads may modify their TLS slots concurrently; we accept that we may trim
        // an array we did not need to (rare).
        if (pressure == MemoryPressure.High)
        {
            // Under high pressure, release all thread locals.
            foreach (KeyValuePair<PoolThreadLocalArray[], object?> tlsBuckets in _allTlsBuckets) Array.Clear(tlsBuckets.Key);
        }
        else
        {
            // Otherwise release thread locals by how long they have been observed stored: at least two Trim calls
            // (two gen 2 GCs) unless memory pressure is high.
            uint millisecondsThreshold = pressure == MemoryPressure.Medium ? 15_000u : 30_000u;
            foreach (KeyValuePair<PoolThreadLocalArray[], object?> tlsBuckets in _allTlsBuckets)
            {
                PoolThreadLocalArray[] buckets = tlsBuckets.Key;
                for (int i = 0; i < buckets.Length; i++)
                {
                    if (buckets[i].Array is null) continue;

                    // 0 means not yet seen by a Trim call (a Trim recording 0 costs one extra Trim call).
                    int lastSeen = buckets[i].MillisecondsTimeStamp;
                    if (lastSeen == 0) buckets[i].MillisecondsTimeStamp = currentMilliseconds;
                    else if ((uint)(currentMilliseconds - lastSeen) >= millisecondsThreshold) Interlocked.Exchange(ref buckets[i].Array, null);
                }
            }
        }
        return true;
    }

    private PoolThreadLocalArray[] InitializeTlsBucketsAndTrimming()
    {
        var tlsBuckets = new PoolThreadLocalArray[NumBuckets];
        t_tlsBuckets = tlsBuckets;

        _allTlsBuckets.Add(tlsBuckets, null);
        if (!Interlocked.Exchange(ref _trimCallbackCreated, true))
            Gen2GcCallback.Register(this);

        return tlsBuckets;
    }

    /// <summary>
    /// Buffers are bucketed so that a request between 2^(n-1) + 1 and 2^n is given a buffer of 2^n. The bucket index
    /// is log2(bufferSize - 1), except that buffers of 1 to 16 elements are combined, and the index is slid down by 3
    /// to compensate. Zero gets the highest index so that zero-length buffers are not retained.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int SelectBucketIndex(int bufferSize) => BitOperations.Log2((uint)bufferSize - 1 | 15) - 3;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetMaxSizeForBucket(int binIndex) => 16 << binIndex;

    private static MemoryPressure GetMemoryPressure()
    {
        const double HighPressureThreshold = .90;   // fraction of the GC memory load threshold considered "high"
        const double MediumPressureThreshold = .70; // fraction considered "medium"
        GCMemoryInfo memoryInfo = GC.GetGCMemoryInfo();
        if (memoryInfo.MemoryLoadBytes >= memoryInfo.HighMemoryLoadThresholdBytes * HighPressureThreshold) return MemoryPressure.High;
        if (memoryInfo.MemoryLoadBytes >= memoryInfo.HighMemoryLoadThresholdBytes * MediumPressureThreshold) return MemoryPressure.Medium;
        return MemoryPressure.Low;
    }

    /// <summary>
    /// Runs <see cref="Trim"/> after every gen 2 GC: an object that resurrects itself in its finalizer (the runtime's
    /// Gen2GcCallback, for a pool that lives for the process lifetime).
    /// </summary>
    private sealed class Gen2GcCallback : CriticalFinalizerObject
    {
        private readonly PrivateSharedArrayPool<T> _pool;

        private Gen2GcCallback(PrivateSharedArrayPool<T> pool) => _pool = pool;

        public static void Register(PrivateSharedArrayPool<T> pool) => _ = new Gen2GcCallback(pool);

        ~Gen2GcCallback()
        {
            try
            {
                _pool.Trim();
            }
            catch
            {
                // A failed trim must not end the callback (nor the finalizer thread).
            }
            GC.ReRegisterForFinalize(this);
        }
    }
}

internal enum MemoryPressure
{
    Low,
    Medium,
    High,
}

/// <summary>Wrapper for arrays stored in the thread-static buckets.</summary>
internal struct PoolThreadLocalArray(Array array)
{
    /// <summary>The stored array.</summary>
    public Array? Array = array;

    /// <summary>Environment.TickCount timestamp for when this array was observed by Trim.</summary>
    public int MillisecondsTimeStamp = 0;
}

/// <summary>
/// The per-core partitions of one array size. Not generic (only TryPop's result is, handled with Unsafe.As at the
/// call site), as in the runtime, to limit generic code size.
/// </summary>
internal sealed class PoolPartitions
{
    /// <summary>Number of partitions: one per processor.</summary>
    private static readonly int s_partitionCount = Environment.ProcessorCount;

    /// <summary>The maximum number of arrays per array size per partition.</summary>
    internal const int MaxArraysPerPartition = 32;

    private readonly Partition[] _partitions;

    public PoolPartitions()
    {
        var partitions = new Partition[s_partitionCount];
        for (int i = 0; i < partitions.Length; i++) partitions[i] = new Partition();
        _partitions = partitions;
    }

    /// <summary>Pushes the array into the current core's partition, else any with space; drops it when all are full.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPush(Array array)
    {
        Partition[] partitions = _partitions;
        int index = (int)((uint)Thread.GetCurrentProcessorId() % (uint)s_partitionCount);
        for (int i = 0; i < partitions.Length; i++)
        {
            if (partitions[index].TryPush(array)) return true;
            if (++index == partitions.Length) index = 0;
        }
        return false;
    }

    /// <summary>Pops from the current core's partition, else from any; null when all are empty.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Array? TryPop()
    {
        Array? arr;
        Partition[] partitions = _partitions;
        int index = (int)((uint)Thread.GetCurrentProcessorId() % (uint)s_partitionCount);
        for (int i = 0; i < partitions.Length; i++)
        {
            if ((arr = partitions[index].TryPop()) is not null) return arr;
            if (++index == partitions.Length) index = 0;
        }
        return null;
    }

    public void Trim(int currentMilliseconds, MemoryPressure pressure)
    {
        Partition[] partitions = _partitions;
        for (int i = 0; i < partitions.Length; i++) partitions[i].Trim(currentMilliseconds, pressure);
    }

    /// <summary>A simple bounded stack of arrays, protected by a lock.</summary>
    private sealed class Partition
    {
        private readonly Array?[] _arrays = new Array[MaxArraysPerPartition];
        private int _count;
        /// <summary>Timestamp set by Trim when it sees this as 0.</summary>
        private int _millisecondsTimestamp;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPush(Array array)
        {
            bool enqueued = false;
            Monitor.Enter(this);
            Array?[] arrays = _arrays;
            int count = _count;
            if ((uint)count < (uint)arrays.Length)
            {
                // Transitioning from empty to non-empty: Trim will see 0 and initialize the timestamp.
                if (count == 0) _millisecondsTimestamp = 0;
                Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(arrays), count) = array; // arrays[count] = array without stelemref
                _count = count + 1;
                enqueued = true;
            }
            Monitor.Exit(this);
            return enqueued;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Array? TryPop()
        {
            Array? arr = null;
            Monitor.Enter(this);
            Array?[] arrays = _arrays;
            int count = _count - 1;
            if ((uint)count < (uint)arrays.Length)
            {
                arr = arrays[count];
                arrays[count] = null;
                _count = count;
            }
            Monitor.Exit(this);
            return arr;
        }

        public void Trim(int currentMilliseconds, MemoryPressure pressure)
        {
            const int TrimAfterMS = 60 * 1000;     // trim after 60 seconds for low/moderate pressure
            const int HighTrimAfterMS = 10 * 1000; // trim after 10 seconds for high pressure

            if (_count == 0) return;
            int trimMilliseconds = pressure == MemoryPressure.High ? HighTrimAfterMS : TrimAfterMS;

            lock (this)
            {
                if (_count == 0) return;
                if (_millisecondsTimestamp == 0)
                {
                    _millisecondsTimestamp = currentMilliseconds;
                    return;
                }
                if ((currentMilliseconds - _millisecondsTimestamp) <= trimMilliseconds) return;

                // Enough time has elapsed since the first item went into the partition: drop the top item(s).
                int trimCount = pressure switch
                {
                    MemoryPressure.High => MaxArraysPerPartition,
                    MemoryPressure.Medium => 2,
                    _ => 1,
                };
                while (_count > 0 && trimCount-- > 0) _arrays[--_count] = null;

                // Give the remaining items a bit more time.
                _millisecondsTimestamp = _count > 0 ? _millisecondsTimestamp + (trimMilliseconds / 4) : 0;
            }
        }
    }
}
