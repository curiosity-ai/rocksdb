using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Table;

/// <summary>
/// Filter partitions loaded at table open and pinned for the reader's lifetime (port of the filter_map_ built by
/// PartitionedFilterBlockReader::CacheDependencies with pinning, table/block_based/partitioned_filter_block.cc).
/// Keyed by partition block offset. The partitions are block cache entries the reader holds handles of
/// (<see cref="TryAddCached"/>), or, for a reader without a block cache, buffers it owns (<see cref="Load"/>).
/// Thread-safe for concurrent lookups (immutable once the reader is published).
/// </summary>
internal sealed class PinnedFilterPartitions : IDisposable
{
    private readonly Dictionary<ulong, IFilterBitsReader> _readers = new();
    private readonly List<byte[]> _buffers = new();
    private readonly List<Cache.CachedBlock> _handles = new();

    public int Count => _readers.Count;

    /// <summary>Bytes of the partition buffers the reader owns (partitions pinned in the block cache are part of the cache's usage).</summary>
    public ulong MemoryUsage
    {
        get
        {
            ulong n = 0;
            foreach (byte[] b in _buffers) n += (ulong)b.Length;
            return n;
        }
    }

    public bool TryGet(ulong offset, out IFilterBitsReader reader) => _readers.TryGetValue(offset, out reader!);

    /// <summary>
    /// Pins a partition held by the block cache: takes over <paramref name="handle"/> (released by <see cref="Dispose"/>).
    /// False when the partition is already pinned; the caller then releases the handle.
    /// </summary>
    public bool TryAddCached(ulong offset, Cache.CachedBlock handle)
    {
        if (!_readers.TryAdd(offset, System.Runtime.CompilerServices.Unsafe.As<IFilterBitsReader>(handle.Value)!)) return false;
        _handles.Add(handle);
        return true;
    }

    /// <summary>Reads every partition listed in the top-level filter partition index <paramref name="top"/>.</summary>
    public static PinnedFilterPartitions Load(IRandomAccessFile file, in Footer footer, BlockFetcher.ChecksumVerification verifyChecksums, FilterPolicy policy, Block top,
        TableProperties props, Comparator comparator, bool ignoreUnreadablePartitions)
    {
        var result = new PinnedFilterPartitions();
        try
        {
            InternalKeyComparator icmp = comparator.Kind == ComparatorKind.Bytewise ? InternalKeyComparator.Bytewise : new InternalKeyComparator(comparator);
            using var it = new BlockIter();
            it.InitializeIndex(top, icmp, props.IndexKeyIsUserKey == 0, props.IndexValueIsDeltaEncoded == 0, false,
                BlockBasedTableConstants.FormatVersionUsesValueDeltaEscape(footer.FormatVersion), BlockIter.DisableGlobalSequenceNumber);
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                BlockHandle h = it.BlockHandle;
                if (h.Size == 0 || result._readers.ContainsKey(h.Offset)) continue;
                byte[] buf;
                int len;
                try
                {
                    buf = BlockFetcher.ReadRawPayload(file, footer, h, verifyChecksums, out len);
                }
                catch (Exception e) when (ignoreUnreadablePartitions && e is RocksDbException or IOException)
                {
                    // Not pinned: lookups read the partition themselves and answer "may match" while that fails
                    // (PartitionedFilterBlockReader::MayMatch, IGNORE_STATUS_IF_ERROR).
                    continue;
                }
                result._buffers.Add(buf);
                IFilterBitsReader reader;
                try
                {
                    reader = BlockBasedTableReader.CreateFilterReader(policy, buf, len);
                }
                catch (RocksDbException) when (ignoreUnreadablePartitions)
                {
                    continue;
                }
                result._readers.Add(h.Offset, reader);
            }
            return result;
        }
        catch
        {
            result.Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        foreach (byte[] b in _buffers) RocksArrayPool<byte>.Shared.Return(b);
        _buffers.Clear();
        foreach (Cache.CachedBlock h in _handles) h.Dispose();
        _handles.Clear();
        _readers.Clear();
    }
}
