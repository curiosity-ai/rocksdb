using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Table;

/// <summary>Options for opening a <see cref="BlockBasedTableReader"/>.</summary>
public sealed record TableReaderOptions
{
    /// <summary>User comparator the table was built with.</summary>
    public Comparator Comparator { get; init; } = Comparator.Bytewise;
    /// <summary>Filter policy used to interpret the full filter block ("fullfilter.&lt;name&gt;"); null ignores filters.</summary>
    public FilterPolicy? FilterPolicy { get; init; }
    public bool VerifyChecksums { get; init; } = true;
    /// <summary>Block cache for data blocks and index partitions (default: none).</summary>
    public IBlockCache BlockCache { get; init; } = NoBlockCache.Instance;
    /// <summary>
    /// Largest sequence number of the file as recorded in the MANIFEST, used to resolve the global sequence number
    /// of ingested external files; <see cref="DbFormat.MaxSequenceNumber"/> when unknown.
    /// </summary>
    public ulong LargestSeqno { get; init; } = DbFormat.MaxSequenceNumber;

    public static TableReaderOptions Default { get; } = new();
}

/// <summary>Result of a point lookup.</summary>
public readonly record struct TableGetResult(bool Found, ulong Sequence, EntryType Type);

/// <summary>
/// Visitor for <see cref="BlockBasedTableReader.Get{TState}"/>: called for each entry whose user key equals the lookup
/// key, starting at the lookup position. Return false to stop.
/// </summary>
public delegate bool TableEntryVisitor<TState>(ref TState state, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    where TState : allows ref struct;

/// <summary>
/// Reader for block-based tables. Port of BlockBasedTable (table/block_based/block_based_table_reader.cc),
/// BinarySearchIndexReader / PartitionedIndexReader (read side), meta_blocks.cc (ReadTableProperties,
/// FindMetaBlock) and GetGlobalSequenceNumber.
/// <para>
/// The footer, metaindex, properties, (top-level) index, full filter and range-deletion blocks are read at open and
/// pinned for the lifetime of the reader. Data blocks (and index partitions) are fetched on demand through the
/// <see cref="IBlockCache"/> seam. Thread-safe for concurrent lookups and iterators.
/// </para>
/// </summary>
public sealed class BlockBasedTableReader : IDisposable
{
    private static long s_nextTableId;

    private readonly IRandomAccessFile _file;
    private readonly TableReaderOptions _options;
    private readonly Footer _footer;
    private readonly Block _indexBlock;
    private readonly Block? _rangeDelBlock;
    private readonly IFilterBitsReader? _filter;
    private byte[]? _filterBuffer;
    private readonly ulong _tableId;
    private int _disposed;

    public Footer Footer => _footer;
    public TableProperties Properties { get; }
    public IReadOnlyDictionary<string, BlockHandle> MetaBlocks { get; }
    public IndexType IndexType { get; }
    public bool IndexKeyIncludesSeq { get; }
    public bool IndexValueIsFull { get; }
    public bool IndexHasFirstKey => IndexType == IndexType.BinarySearchWithFirstKey;
    public bool WholeKeyFiltering { get; }
    public bool HasFilter => _filter is not null;
    /// <summary>Effective global sequence number (external files), or <see cref="BlockIter.DisableGlobalSequenceNumber"/>.</summary>
    public ulong GlobalSequenceNumber { get; }
    public long FileSize { get; }
    internal Comparator Comparator => _options.Comparator;
    internal IRandomAccessFile File => _file;

    private BlockBasedTableReader(IRandomAccessFile file, TableReaderOptions options, Footer footer, Dictionary<string, BlockHandle> metaBlocks,
        TableProperties props, Block indexBlock, Block? rangeDel, IFilterBitsReader? filter, byte[]? filterBuffer, long fileSize)
    {
        _file = file;
        _options = options;
        _footer = footer;
        MetaBlocks = metaBlocks;
        Properties = props;
        _indexBlock = indexBlock;
        _rangeDelBlock = rangeDel;
        _filter = filter;
        _filterBuffer = filterBuffer;
        FileSize = fileSize;
        _tableId = (ulong)Interlocked.Increment(ref s_nextTableId);

        IndexType = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.BlockBasedIndexType, out byte[]? it) && it.Length >= 4
            ? (IndexType)Coding.DecodeFixed32(it)
            : IndexType.BinarySearch;
        if (IndexType > IndexType.BinarySearchWithFirstKey) ThrowHelper.Corruption("unknown index type " + (uint)IndexType);
        IndexKeyIncludesSeq = props.IndexKeyIsUserKey == 0;
        IndexValueIsFull = props.IndexValueIsDeltaEncoded == 0;
        WholeKeyFiltering = !props.UserCollectedProperties.TryGetValue(TablePropertiesNames.BlockBasedWholeKeyFiltering, out byte[]? wkf)
            || wkf.AsSpan().SequenceEqual("1"u8);
        GlobalSequenceNumber = ResolveGlobalSeqno(props, options.LargestSeqno);
    }

    // ---------------------------------------------------------------------------------------------
    // Open

    /// <summary>Opens a table. The reader takes ownership of <paramref name="file"/> (disposed with the reader).</summary>
    public static BlockBasedTableReader Open(IRandomAccessFile file, TableReaderOptions options)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        long fileSize = file.Length;
        Footer footer = ReadFooter(file, fileSize);
        return OpenWithFooter(file, options, footer, fileSize);
    }

    /// <summary>Opens a table asynchronously (footer and tail meta blocks are read with async I/O).</summary>
    public static async ValueTask<BlockBasedTableReader> OpenAsync(IRandomAccessFile file, TableReaderOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        long fileSize = file.Length;
        if (fileSize < Footer.MinEncodedLength) ThrowHelper.Corruption("file is too short (" + fileSize + " bytes) to be an sstable");
        int n = (int)Math.Min(fileSize, Footer.EncodedLength);
        byte[] buf = new byte[n];
        int total = 0;
        while (total < n)
        {
            int r = await file.ReadAsync(fileSize - n + total, buf.AsMemory(total), cancellationToken).ConfigureAwait(false);
            if (r <= 0) break;
            total += r;
        }
        if (total != n) ThrowHelper.Corruption("truncated footer read");
        Footer footer = Footer.Decode(buf, (ulong)(fileSize - n));
        // Prefetch the tail (meta blocks) asynchronously so the remaining open work is served from memory.
        var tail = new TailPrefetchFile(file);
        await tail.PrefetchAsync(fileSize, footer, cancellationToken).ConfigureAwait(false);
        try
        {
            return OpenWithFooter(tail, options, footer, fileSize, file);
        }
        finally
        {
            tail.ReleaseBuffer();
        }
    }

    private static Footer ReadFooter(IRandomAccessFile file, long fileSize)
    {
        if (fileSize < Footer.MinEncodedLength) ThrowHelper.Corruption("file is too short (" + fileSize + " bytes) to be an sstable");
        int n = (int)Math.Min(fileSize, Footer.EncodedLength);
        Span<byte> buf = stackalloc byte[Footer.EncodedLength];
        int read = BlockFetcher.ReadFully(file, fileSize - n, buf[..n]);
        if (read != n) ThrowHelper.Corruption("truncated footer read");
        return Footer.Decode(buf[..n], (ulong)(fileSize - n));
    }

    private static BlockBasedTableReader OpenWithFooter(IRandomAccessFile file, TableReaderOptions options, Footer footer, long fileSize, IRandomAccessFile? ownedFile = null)
    {
        bool verify = options.VerifyChecksums;
        var metaBlocks = new Dictionary<string, BlockHandle>(StringComparer.Ordinal);
        Block metaindex = BlockFetcher.ReadBlock(file, footer, footer.MetaindexHandle, verify);
        try
        {
            using var it = new BlockIter();
            it.InitializeMeta(metaindex);
            for (it.SeekToFirst(); it.Valid; it.Next())
                metaBlocks[Encoding.UTF8.GetString(it.Key)] = BlockHandle.Decode(it.Value);
        }
        finally
        {
            metaindex.Release();
        }

        TableProperties props;
        if (metaBlocks.TryGetValue(BlockBasedTableConstants.PropertiesBlockName, out BlockHandle propsHandle))
        {
            Block pb = BlockFetcher.ReadBlock(file, footer, propsHandle, verify);
            try { props = TableProperties.Parse(pb, propsHandle.Offset); }
            finally { pb.Release(); }
        }
        else
        {
            props = new TableProperties();
        }

        BlockHandle indexHandle = footer.IndexHandle;
        if (!BlockBasedTableConstants.FormatVersionUsesIndexHandleInFooter(footer.FormatVersion))
        {
            if (!metaBlocks.TryGetValue(BlockBasedTableConstants.IndexBlockName, out indexHandle))
                ThrowHelper.Corruption("Index block handle not found in metaindex");
        }

        Block indexBlock = BlockFetcher.ReadBlock(file, footer, indexHandle, verify);
        Block? rangeDel = null;
        byte[]? filterBuf = null;
        try
        {
            if (metaBlocks.TryGetValue(BlockBasedTableConstants.RangeDelBlockName, out BlockHandle rdHandle))
                rangeDel = BlockFetcher.ReadBlock(file, footer, rdHandle, verify);

            IFilterBitsReader? filter = null;
            if (options.FilterPolicy is { } policy
                && metaBlocks.TryGetValue(BlockBasedTableConstants.FullFilterBlockPrefix + policy.CompatibilityName, out BlockHandle fh))
            {
                filterBuf = BlockFetcher.ReadRawPayload(file, footer, fh, verify, out int flen);
                filter = policy.CreateReader(filterBuf.AsMemory(0, flen));
            }
            return new BlockBasedTableReader(ownedFile ?? file, options, footer, metaBlocks, props, indexBlock, rangeDel, filter, filterBuf, fileSize);
        }
        catch
        {
            indexBlock.Release();
            rangeDel?.Release();
            if (filterBuf is not null) ArrayPool<byte>.Shared.Return(filterBuf);
            throw;
        }
    }

    /// <summary>Port of GetGlobalSequenceNumber (table/meta_blocks.cc).</summary>
    private static ulong ResolveGlobalSeqno(TableProperties props, ulong largestSeqno)
    {
        const ulong disabled = BlockIter.DisableGlobalSequenceNumber;
        if (largestSeqno == disabled) return disabled;
        bool hasVersion = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.ExternalSstFileVersion, out byte[]? versionBytes);
        bool hasSeqno = props.UserCollectedProperties.TryGetValue(TablePropertiesNames.ExternalSstFileGlobalSeqno, out byte[]? seqnoBytes);
        if (!hasVersion)
        {
            if (hasSeqno) ThrowHelper.Corruption("A non-external sst file have global seqno property");
            return disabled;
        }
        if (versionBytes!.Length < 4) ThrowHelper.Corruption("bad external sst file version");
        uint version = Coding.DecodeFixed32(versionBytes);
        if (version != 2)
        {
            if (version != 1) ThrowHelper.Corruption("An external sst file has corrupted version " + version + ".");
            if (hasSeqno) ThrowHelper.Corruption("An external sst file with version 1 has global seqno property");
            return disabled;
        }
        ulong resolved = 0;
        if (hasSeqno && seqnoBytes!.Length >= 8) resolved = Coding.DecodeFixed64(seqnoBytes);
        if (largestSeqno < DbFormat.MaxSequenceNumber)
        {
            if (resolved == 0) resolved = largestSeqno;
            if (resolved != largestSeqno) ThrowHelper.Corruption("An external sst file has a global seqno inconsistent with its largest seqno");
        }
        if (resolved > DbFormat.MaxSequenceNumber) ThrowHelper.Corruption("global seqno greater than kMaxSequenceNumber");
        // Keys of external files are written with seqno 0, so a zero global seqno needs no rewriting.
        return resolved == 0 ? disabled : resolved;
    }

    // ---------------------------------------------------------------------------------------------
    // Blocks

    /// <summary>Gets a data block or index partition (through the cache). The returned block carries a reference owned by the caller.</summary>
    internal Block GetBlock(BlockHandle handle)
    {
        var key = new BlockCacheKey(_tableId, handle.Offset);
        IBlockCache cache = _options.BlockCache;
        if (cache.TryGet(key, out Block? b)) return b;
        b = BlockFetcher.ReadBlock(_file, _footer, handle, _options.VerifyChecksums);
        cache.Insert(key, b);
        return b;
    }

    /// <summary>Async <see cref="GetBlock"/>; completes synchronously on a cache hit.</summary>
    internal ValueTask<Block> GetBlockAsync(BlockHandle handle, CancellationToken ct)
    {
        var key = new BlockCacheKey(_tableId, handle.Offset);
        if (_options.BlockCache.TryGet(key, out Block? b)) return new ValueTask<Block>(b);
        return ReadAndInsertAsync(handle, key, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Block> ReadAndInsertAsync(BlockHandle handle, BlockCacheKey key, CancellationToken ct)
    {
        Block b = await BlockFetcher.ReadBlockAsync(_file, _footer, handle, _options.VerifyChecksums, ct).ConfigureAwait(false);
        _options.BlockCache.Insert(key, b);
        return b;
    }

    internal void InitIndexIter(BlockIter it, Block block) =>
        it.InitializeIndex(block, _options.Comparator, IndexKeyIncludesSeq, IndexValueIsFull, IndexHasFirstKey, GlobalSequenceNumber);

    internal Block IndexBlock => _indexBlock;
    internal bool IsPartitionedIndex => IndexType == IndexType.TwoLevelIndexSearch;

    // ---------------------------------------------------------------------------------------------
    // Point lookups

    /// <summary>
    /// Whether the filter (if any) says <paramref name="userKey"/> may be present. Port of
    /// BlockBasedTable::FullFilterKeyMayMatch (whole key filtering only).
    /// </summary>
    public bool KeyMayMatch(ReadOnlySpan<byte> userKey)
    {
        if (_filter is null || !WholeKeyFiltering) return true;
        return _filter.MayMatch(userKey);
    }

    /// <summary>
    /// Point lookup. <paramref name="internalKey"/> is a lookup key (user key + packed (snapshot seq, kValueTypeForSeek)).
    /// Calls <paramref name="visitor"/> for each entry with the same user key at or after the lookup key, in order,
    /// until it returns false. Port of BlockBasedTable::Get.
    /// </summary>
    public void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor)
        where TState : allows ref struct
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
        if (!KeyMayMatch(userKey)) return;
        IndexCursor idx = IndexCursor.Rent(this);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.Seek(internalKey);
            bool first = true;
            while (idx.Valid)
            {
                Block block = GetBlock(idx.Handle);
                try
                {
                    data.InitializeData(block, _options.Comparator, GlobalSequenceNumber);
                    if (first)
                    {
                        if (!data.SeekForGet(internalKey)) return;
                        first = false;
                    }
                    else
                    {
                        data.SeekToFirst();
                    }
                    for (; data.Valid; data.Next())
                    {
                        ReadOnlySpan<byte> k = data.Key;
                        if (!UserKeyEquals(DbFormat.ExtractUserKey(k), userKey)) return;
                        if (!visitor(ref state, k, data.Value)) return;
                    }
                }
                finally
                {
                    data.Clear();
                    block.Release();
                }
                idx.Next();
            }
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    private struct GetState
    {
        public IBufferWriter<byte>? Output;
        public TableGetResult Result;
    }

    /// <summary>
    /// Finds the first entry for the user key of <paramref name="internalKey"/> visible at the lookup sequence number.
    /// Copies its value into <paramref name="value"/> (if non-null). Returns <c>Found=false</c> if no entry exists.
    /// The caller interprets the entry type (deletion, merge, ...).
    /// </summary>
    public TableGetResult Get(ReadOnlySpan<byte> internalKey, IBufferWriter<byte>? value)
    {
        var state = new GetState { Output = value };
        Get(internalKey, ref state, static (ref GetState s, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) =>
        {
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong seq, out EntryType t);
            s.Result = new TableGetResult(true, seq, t);
            s.Output?.Write(v);
            return false;
        });
        return state.Result;
    }

    /// <summary>Async <see cref="Get(ReadOnlySpan{byte}, IBufferWriter{byte}?)"/>: only data block reads are asynchronous; completes synchronously on cache hits.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<TableGetResult> GetAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, CancellationToken cancellationToken = default)
    {
        if (!KeyMayMatch(DbFormat.ExtractUserKey(internalKey.Span))) return default;
        IndexCursor idx = IndexCursor.Rent(this);
        BlockIter data = IterPool.Rent();
        try
        {
            idx.Seek(internalKey.Span);
            bool first = true;
            while (idx.Valid)
            {
                ValueTask<Block> pending = GetBlockAsync(idx.Handle, cancellationToken);
                Block block = pending.IsCompletedSuccessfully ? pending.Result : await pending.ConfigureAwait(false);
                try
                {
                    int r = ScanBlockForGet(data, block, internalKey.Span, first, value, out TableGetResult result);
                    if (r != 0) return result;
                    first = false;
                }
                finally
                {
                    data.Clear();
                    block.Release();
                }
                idx.Next();
            }
            return default;
        }
        finally
        {
            IterPool.Return(data);
            idx.Return();
        }
    }

    /// <summary>Returns 1 = found, -1 = definitely absent, 0 = continue with the next block.</summary>
    private int ScanBlockForGet(BlockIter data, Block block, ReadOnlySpan<byte> internalKey, bool first, IBufferWriter<byte>? value, out TableGetResult result)
    {
        result = default;
        data.InitializeData(block, _options.Comparator, GlobalSequenceNumber);
        if (first)
        {
            if (!data.SeekForGet(internalKey)) return -1;
        }
        else
        {
            data.SeekToFirst();
        }
        if (!data.Valid) return 0;
        ReadOnlySpan<byte> k = data.Key;
        if (!UserKeyEquals(DbFormat.ExtractUserKey(k), DbFormat.ExtractUserKey(internalKey))) return -1;
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(k), out ulong seq, out EntryType t);
        value?.Write(data.Value);
        result = new TableGetResult(true, seq, t);
        return 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool UserKeyEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        ReferenceEquals(_options.Comparator, Comparator.Bytewise) ? a.SequenceEqual(b) : _options.Comparator.Equal(a, b);

    // ---------------------------------------------------------------------------------------------
    // Iterators

    /// <summary>Creates a two-level (index → data) iterator over the table's point entries. Dispose when done.</summary>
    public BlockBasedTableIterator NewIterator() => new(this);

    public bool HasRangeTombstones => _rangeDelBlock is not null;

    /// <summary>
    /// Iterator over the range tombstones (internal start key → exclusive end user key), or null if none.
    /// The returned iterator is valid while the reader is alive; dispose it when done.
    /// </summary>
    public BlockIter? NewRangeTombstoneIterator()
    {
        if (_rangeDelBlock is null) return null;
        var it = new BlockIter();
        it.InitializeData(_rangeDelBlock, _options.Comparator, GlobalSequenceNumber);
        return it;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _indexBlock.Release();
        _rangeDelBlock?.Release();
        byte[]? fb = Interlocked.Exchange(ref _filterBuffer, null);
        if (fb is not null) ArrayPool<byte>.Shared.Return(fb);
        _file.Dispose();
    }

    /// <summary>Wraps a file and serves reads within an asynchronously prefetched tail region from memory (port of tail prefetching in BlockBasedTable::PrefetchTail).</summary>
    private sealed class TailPrefetchFile : IRandomAccessFile
    {
        private readonly IRandomAccessFile _inner;
        private byte[]? _tail;
        private long _tailOffset;
        private int _tailLength;

        public TailPrefetchFile(IRandomAccessFile inner) => _inner = inner;
        public long Length => _inner.Length;

        public async ValueTask PrefetchAsync(long fileSize, Footer footer, CancellationToken ct)
        {
            // The tail start isn't known before the properties are read; like upstream's default tail prefetch,
            // read the last 512 KiB (at least through the metaindex). Reads outside fall back to the inner file.
            long start = Math.Max(0, fileSize - (512 << 10));
            if (footer.IndexHandle.Size > 0) start = Math.Min(start, (long)footer.IndexHandle.Offset);
            start = Math.Min(start, (long)footer.MetaindexHandle.Offset);
            long len = fileSize - start;
            if (len <= 0 || len > 64L << 20) return;
            byte[] buf = ArrayPool<byte>.Shared.Rent((int)len);
            int total = 0;
            while (total < len)
            {
                int r = await _inner.ReadAsync(start + total, buf.AsMemory(total, (int)len - total), ct).ConfigureAwait(false);
                if (r <= 0) break;
                total += r;
            }
            _tail = buf;
            _tailOffset = start;
            _tailLength = total;
        }

        public int Read(long offset, Span<byte> destination)
        {
            if (_tail is not null && offset >= _tailOffset && offset + destination.Length <= _tailOffset + _tailLength)
            {
                _tail.AsSpan((int)(offset - _tailOffset), destination.Length).CopyTo(destination);
                return destination.Length;
            }
            return _inner.Read(offset, destination);
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            new(Read(offset, destination.Span));

        public void ReleaseBuffer()
        {
            if (_tail is not null) ArrayPool<byte>.Shared.Return(_tail);
            _tail = null;
        }

        public void Dispose() => ReleaseBuffer();
    }
}

/// <summary>Small per-thread pool of <see cref="BlockIter"/> instances for point lookups.</summary>
internal static class IterPool
{
    [ThreadStatic] private static BlockIter?[]? t_items;
    [ThreadStatic] private static int t_count;

    public static BlockIter Rent()
    {
        BlockIter?[]? items = t_items;
        if (items is not null && t_count > 0)
        {
            BlockIter it = items[--t_count]!;
            items[t_count] = null;
            return it;
        }
        return new BlockIter();
    }

    public static void Return(BlockIter it)
    {
        it.Clear();
        BlockIter?[] items = t_items ??= new BlockIter?[8];
        if (t_count < items.Length) items[t_count++] = it;
        else it.Dispose();
    }
}
