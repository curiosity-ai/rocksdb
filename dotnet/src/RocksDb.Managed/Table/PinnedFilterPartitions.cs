using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Table;

/// <summary>
/// Filter partitions loaded at table open and pinned for the reader's lifetime (port of the filter_map_ built by
/// PartitionedFilterBlockReader::CacheDependencies with pinning, table/block_based/partitioned_filter_block.cc).
/// Keyed by partition block offset. Thread-safe for concurrent lookups (immutable after <see cref="Load"/>).
/// </summary>
internal sealed class PinnedFilterPartitions : IDisposable
{
    private readonly Dictionary<ulong, IFilterBitsReader> _readers = new();
    private readonly List<byte[]> _buffers = new();

    public int Count => _readers.Count;

    /// <summary>Bytes of the pinned partition buffers.</summary>
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

    /// <summary>Reads every partition listed in the top-level filter partition index <paramref name="top"/>.</summary>
    public static PinnedFilterPartitions Load(IRandomAccessFile file, in Footer footer, bool verifyChecksums, FilterPolicy policy, Block top,
        TableProperties props, Comparator comparator)
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
                byte[] buf = BlockFetcher.ReadRawPayload(file, footer, h, verifyChecksums, out int len);
                result._buffers.Add(buf);
                result._readers.Add(h.Offset, BlockBasedTableReader.CreateFilterReader(policy, buf, len));
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
        foreach (byte[] b in _buffers) ArrayPool<byte>.Shared.Return(b);
        _buffers.Clear();
        _readers.Clear();
    }
}
