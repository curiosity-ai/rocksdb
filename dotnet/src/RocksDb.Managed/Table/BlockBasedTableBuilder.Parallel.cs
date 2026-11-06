using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Parallel data block compression. Port of BlockBasedTableBuilder::ParallelCompressionRep, EmitBlockForParallel,
/// BGWorker, MaybeStartParallelCompression and StopParallelCompression (table/block_based/block_based_table_builder.cc),
/// activated by CompressionOptions.parallel_threads &gt; 1.
/// <para>
/// Like upstream, finished uncompressed data blocks are "emitted" by the calling (emit) thread into a ring buffer of
/// block slots (size: a power of two, at least twice the thread count, at most 32), compressed by worker threads, and
/// written in order. The index entry of each block is prepared on the emit thread
/// (<see cref="IndexBuilder.PrepareIndexEntry"/>) and finished with the block handle once the block is written
/// (<see cref="IndexBuilder.FinishIndexEntry"/>).
/// </para>
/// <para>
/// Deviations (the output stays byte-identical to the serial path, which the tests assert):
/// <list type="bullet">
/// <item>Workers only compress (and checksum the compressed payload). Writing - output staging, trailer, padding,
/// index entries, properties - stays on the emit thread, which writes completed blocks in order whenever the head of
/// the ring is ready and when it needs a free slot. Output is staged in memory and handed to the file by the emit
/// thread anyway, so a dedicated writer thread would buy nothing here, and every builder structure stays
/// single-threaded (no synchronization on the index/filter builders or properties).</item>
/// <item>Workers are thread pool work items (no dedicated threads per SST file); at most min(parallel_threads,
/// ring size - 1) run at a time. The emit thread never waits for a block nobody is compressing: it compresses a still
/// pending head block itself (upstream's emit thread also takes compression work when the ring is full).</item>
/// <item>The lock-free state machine of upstream is replaced by a per-slot state (free, pending, compressing, done)
/// with compare-and-swap claims.</item>
/// </list>
/// </para>
/// <para>
/// As upstream, parallel compression is not used without compression, with coupled partitioned filters
/// (partition_filters without decouple_partitioned_filters: filter partitions are cut together with index partitions,
/// which needs the index entry at emit time), and the ring and its buffers are pooled across builders.
/// </para>
/// </summary>
public sealed partial class BlockBasedTableBuilder
{
    private ParallelCompressionRep? _pc;

    /// <summary>
    /// Port of BlockBasedTableBuilder::EstimatedFileSize: <see cref="FileSize"/> plus the (uncompressed) size of the
    /// data blocks emitted for parallel compression but not yet written. Equal to <see cref="FileSize"/> without
    /// parallel compression.
    /// </summary>
    public override ulong EstimatedFileSize => _pc is null ? _offset : _offset + _pc.EstimatedInflightSize;

    /// <summary>Whether data blocks are compressed in parallel (CompressionOptions.parallel_threads &gt; 1 and supported).</summary>
    internal bool IsParallelCompressionActive => _pc is not null;

    /// <summary>Port of MaybeStartParallelCompression (plus the Rep::Rep sanitization of compression_parallel_threads).</summary>
    private void MaybeStartParallelCompression()
    {
        uint threads = _tbo.CompressionOptions.ParallelThreads;
        if (threads <= 1) return;
        // Embedded blob records are written at the running offset: single-threaded writes only.
        if (_embeddedBlobMinSize is not null) return;
        // Force the generally best configuration for no compression: no parallelism.
        if (_tbo.CompressionType == CompressionType.None) return;
        // partition_filters without decoupling requires single-threaded compression.
        if (_options.PartitionFilters && !_options.DecouplePartitionedFilters && _filterBuilder is PartitionedFilterBlockBuilder) return;
        _pc = ParallelCompressionRep.Rent(threads);
        _pc.Builder = this;
    }

    /// <summary>
    /// Port of StopParallelCompression. Normal stop (<paramref name="abort"/> false): writes every emitted block, in
    /// order. Abort: drops the pending blocks after in-flight compressions finished. Either way the ring goes back to the
    /// pool.
    /// </summary>
    private void StopParallelCompression(bool abort)
    {
        ParallelCompressionRep pc = _pc!;
        _pc = null;
        bool returned = false;
        try
        {
            if (!abort)
            {
                while (pc.WriteSeq < pc.EmitSeq) WriteNextParallelBlock(pc, wait: true);
            }
            else
            {
                pc.Abort();
            }
            pc.Builder = null;
            ParallelCompressionRep.Return(pc);
            returned = true;
        }
        finally
        {
            if (!returned)
            {
                // A worker failed: make sure no compression is still running on the ring, then drop it (not pooled).
                pc.Abort();
                pc.Builder = null;
            }
        }
    }

    /// <summary>
    /// Port of EmitBlockForParallel: copies the uncompressed block into the next ring slot, prepares its index entry and
    /// hands it to the compression workers. Writes completed blocks at the head of the ring (and, when the ring is full,
    /// waits for / takes over the head block's compression).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void EmitBlockForParallel(ReadOnlySpan<byte> raw, ReadOnlySpan<byte> lastKey, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNext)
    {
        ParallelCompressionRep pc = _pc!;
        // Need a free slot: write the oldest block first when the ring is full.
        while (pc.EmitSeq - pc.WriteSeq > pc.Mask) WriteNextParallelBlock(pc, wait: true);
        ParallelCompressionRep.Slot slot = pc.Ring[(int)(pc.EmitSeq & pc.Mask)];
        slot.SetRaw(raw);
        _indexBuilder.PrepareIndexEntry(lastKey, firstKeyInNextBlock, hasNext, slot.IndexEntry);
        pc.Publish(slot);
        // Write whatever is already compressed, in order, without waiting.
        while (pc.WriteSeq < pc.EmitSeq && WriteNextParallelBlock(pc, wait: false)) { }
    }

    /// <summary>
    /// Writes the block at the head of the ring (the write half of BGWorker's write_fn: WriteMaybeCompressedBlockImpl,
    /// data size properties and FinishIndexEntry). Returns false without writing when <paramref name="wait"/> is false
    /// and the block is not compressed yet.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool WriteNextParallelBlock(ParallelCompressionRep pc, bool wait)
    {
        ParallelCompressionRep.Slot slot = pc.Ring[(int)(pc.WriteSeq & pc.Mask)];
        if (!pc.WaitCompressed(slot, wait)) return false;
        if (slot.Error is { } error)
        {
            slot.Error = null;
            ExceptionDispatchInfo.Throw(error);
        }
        if (slot.Rejected) _props.NumDataBlocksCompressionRejected++;
        ReadOnlySpan<byte> raw = slot.RawSpan;
        bool compressed = slot.Type != CompressionType.None;
        BlockHandle handle = WriteMaybeCompressedBlock(compressed ? slot.Compressed.WrittenSpan : raw, slot.Type, isDataBlock: true,
            out bool skipDeltaEncoding, hasRawChecksum: slot.HasRawChecksum, rawChecksum: slot.RawChecksum);
        _props.DataSize = _offset;
        _props.UncompressedDataSize += (ulong)raw.Length;
        _indexBuilder.FinishIndexEntry(handle, slot.IndexEntry, skipDeltaEncoding);
        pc.Release(slot);
        return true;
    }

    /// <summary>Compresses one ring slot (worker or emit thread). Exceptions are recorded in the slot for the writer.</summary>
    private void CompressSlot(ParallelCompressionRep.Slot slot)
    {
        try
        {
            ReadOnlySpan<byte> raw = slot.RawSpan;
            CompressionType type = CompressBlock(raw, isDataBlock: true, slot.Compressed, out bool rejected);
            slot.Type = type;
            slot.Rejected = rejected;
            // Like upstream's contents_checksum, the payload checksum (without the offset-dependent context modifier) is
            // computed by the compressing thread.
            ReadOnlySpan<byte> payload = type == CompressionType.None ? raw : slot.Compressed.WrittenSpan;
            slot.RawChecksum = BlockChecksum.ComputeBuiltinChecksumWithLastByte(_options.Checksum, payload, (byte)type);
            slot.HasRawChecksum = true;
        }
        catch (Exception e)
        {
            slot.Error = e;
        }
    }

    /// <summary>The ring of block slots and the worker accounting. Pooled across builders.</summary>
    private sealed class ParallelCompressionRep : IThreadPoolWorkItem
    {
        private const int StateFree = 0, StatePending = 1, StateCompressing = 2, StateDone = 3;

        internal sealed class Slot
        {
            private byte[] _raw = [];
            private int _rawLength;
            public readonly PooledBufferWriter Compressed = new(4096);
            public readonly PreparedIndexEntry IndexEntry = new();
            public CompressionType Type;
            public bool Rejected;
            public bool HasRawChecksum;
            public uint RawChecksum;
            public Exception? Error;
            /// <summary>Free / pending / compressing / done; transitions published with Interlocked / Volatile.</summary>
            public int State;

            public ReadOnlySpan<byte> RawSpan => _raw.AsSpan(0, _rawLength);
            public int RawLength => _rawLength;

            public void SetRaw(ReadOnlySpan<byte> raw)
            {
                if (_raw.Length < raw.Length)
                {
                    byte[] old = _raw;
                    _raw = System.Buffers.ArrayPool<byte>.Shared.Rent(Math.Max(raw.Length, 4096));
                    if (old.Length > 0) System.Buffers.ArrayPool<byte>.Shared.Return(old);
                }
                raw.CopyTo(_raw);
                _rawLength = raw.Length;
                Type = CompressionType.None;
                Rejected = false;
                HasRawChecksum = false;
                Error = null;
            }
        }

        private static readonly Lock s_poolLock = new();
        private static readonly List<ParallelCompressionRep> s_pool = new();
        private const int MaxPooled = 16;

        public readonly Slot[] Ring;
        public readonly long Mask;
        private readonly int _maxWorkers;
        private readonly uint _parallelThreads;
        /// <summary>Workers queued or running (bounded by <see cref="_maxWorkers"/>).</summary>
        private int _activeWorkers;
        private long _inflightBytes;
        /// <summary>Owning builder while in use. Workers read it only after claiming a pending slot.</summary>
        public BlockBasedTableBuilder? Builder;
        // Emit thread only (workers read WriteSeq as a scan hint).
        public long EmitSeq;
        private long _writeSeq;
        public long WriteSeq => _writeSeq;

        private ParallelCompressionRep(uint parallelThreads)
        {
            // ComputeRingBufferNbits: a power of two not exceeding 32 but otherwise at least twice the number of threads.
            int nbits = parallelThreads >= 9 ? 5 : parallelThreads >= 5 ? 4 : parallelThreads >= 3 ? 3 : 2;
            int size = 1 << nbits;
            Mask = size - 1;
            _parallelThreads = parallelThreads;
            _maxWorkers = (int)Math.Min(parallelThreads, (uint)Mask);
            Ring = new Slot[size];
            for (int i = 0; i < size; i++) Ring[i] = new Slot();
        }

        public ulong EstimatedInflightSize => (ulong)Volatile.Read(ref _inflightBytes);

        public static ParallelCompressionRep Rent(uint parallelThreads)
        {
            lock (s_poolLock)
            {
                // Reps are interchangeable per thread count.
                for (int i = s_pool.Count - 1; i >= 0; i--)
                {
                    ParallelCompressionRep r = s_pool[i];
                    if (r._parallelThreads != parallelThreads) continue;
                    s_pool.RemoveAt(i);
                    return r;
                }
            }
            return new ParallelCompressionRep(parallelThreads);
        }

        public static void Return(ParallelCompressionRep rep)
        {
            rep.EmitSeq = 0;
            rep._writeSeq = 0;
            Volatile.Write(ref rep._inflightBytes, 0);
            lock (s_poolLock)
            {
                if (s_pool.Count < MaxPooled) s_pool.Add(rep);
            }
        }

        /// <summary>Emit thread: makes the slot at <see cref="EmitSeq"/> pending and wakes a worker if one is available.</summary>
        public void Publish(Slot slot)
        {
            Interlocked.Add(ref _inflightBytes, slot.RawLength + BlockBasedTableConstants.BlockTrailerSize);
            EmitSeq++;
            // Full fence (Dekker with the worker's decrement + re-check in Execute).
            Interlocked.Exchange(ref slot.State, StatePending);
            TryStartWorker();
        }

        private void TryStartWorker()
        {
            int active = Volatile.Read(ref _activeWorkers);
            while (active < _maxWorkers)
            {
                int seen = Interlocked.CompareExchange(ref _activeWorkers, active + 1, active);
                if (seen == active)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                    return;
                }
                active = seen;
            }
        }

        /// <summary>
        /// Emit thread: true once the head slot is compressed. A pending slot is compressed inline (claimed from the
        /// workers); a slot being compressed by a worker is waited for when <paramref name="wait"/> is set.
        /// </summary>
        public bool WaitCompressed(Slot slot, bool wait)
        {
            int state = Volatile.Read(ref slot.State);
            if (state == StateDone) return true;
            if (!wait) return false;
            if (state == StatePending && Interlocked.CompareExchange(ref slot.State, StateCompressing, StatePending) == StatePending)
            {
                Builder!.CompressSlot(slot);
                Volatile.Write(ref slot.State, StateDone);
                return true;
            }
            var spin = new SpinWait();
            while (Volatile.Read(ref slot.State) != StateDone) spin.SpinOnce(sleep1Threshold: -1);
            return true;
        }

        /// <summary>Emit thread: the head slot was written; frees it.</summary>
        public void Release(Slot slot)
        {
            Interlocked.Add(ref _inflightBytes, -(slot.RawLength + BlockBasedTableConstants.BlockTrailerSize));
            Volatile.Write(ref slot.State, StateFree);
            _writeSeq++;
        }

        /// <summary>Emit thread: drops all emitted blocks once no worker is compressing any of them.</summary>
        public void Abort()
        {
            for (long seq = _writeSeq; seq < EmitSeq; seq++)
            {
                Slot slot = Ring[(int)(seq & Mask)];
                if (Interlocked.CompareExchange(ref slot.State, StateFree, StatePending) != StatePending)
                {
                    var spin = new SpinWait();
                    while (Volatile.Read(ref slot.State) == StateCompressing) spin.SpinOnce(sleep1Threshold: -1);
                    Volatile.Write(ref slot.State, StateFree);
                }
                slot.Error = null;
            }
            _writeSeq = EmitSeq;
        }

        /// <summary>BGWorker: compresses pending slots (oldest first) until none is left.</summary>
        public void Execute()
        {
            while (true)
            {
                while (TryCompressOne()) { }
                Interlocked.Decrement(ref _activeWorkers);
                // Re-check after the (fenced) decrement: the emit thread may have published a slot while this worker was
                // still counted as active, without starting another worker.
                if (!AnyPending()) return;
                int active = Volatile.Read(ref _activeWorkers);
                if (active >= _maxWorkers || Interlocked.CompareExchange(ref _activeWorkers, active + 1, active) != active) return;
            }
        }

        private bool AnyPending()
        {
            Slot[] ring = Ring;
            for (int i = 0; i < ring.Length; i++)
                if (Volatile.Read(ref ring[i].State) == StatePending) return true;
            return false;
        }

        private bool TryCompressOne()
        {
            Slot[] ring = Ring;
            long start = Volatile.Read(ref _writeSeq);
            for (int i = 0; i < ring.Length; i++)
            {
                Slot slot = ring[(int)((start + i) & Mask)];
                if (Volatile.Read(ref slot.State) != StatePending) continue;
                if (Interlocked.CompareExchange(ref slot.State, StateCompressing, StatePending) != StatePending) continue;
                BlockBasedTableBuilder? builder = Volatile.Read(ref Builder);
                if (builder is null)
                {
                    // Cannot happen (pending slots belong to an active builder); keep the slot consistent anyway.
                    slot.Error = new InvalidOperationException("parallel compression slot without builder");
                }
                else
                {
                    builder.CompressSlot(slot);
                }
                Volatile.Write(ref slot.State, StateDone);
                return true;
            }
            return false;
        }
    }
}
