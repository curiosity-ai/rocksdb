using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed.Table;

/// <summary>
/// A reader of one table file of any SST format. Port of the <c>TableReader</c> interface (table/table_reader.h):
/// <see cref="BlockBasedTableReader"/> (table/block_based/block_based_table_reader.cc),
/// <see cref="Plain.PlainTableReader"/> (table/plain/plain_table_reader.cc) and
/// <see cref="Cuckoo.CuckooTableReader"/> (table/cuckoo/cuckoo_table_reader.cc). <see cref="TableReaderFactory"/> opens
/// the reader matching the column family's table factory (or the file's magic number for the adaptive factory).
/// </summary>
/// <remarks>
/// The DB's hot read paths test for <see cref="BlockBasedTableReader"/> (sealed) and call it directly, so the virtual
/// (and generic virtual) members here only cost a dispatch for the other formats. Readers are thread-safe for concurrent
/// reads, like upstream.
/// </remarks>
public abstract class TableReader : IDisposable
{
    /// <summary>The table's properties (GetTableProperties).</summary>
    public abstract TableProperties Properties { get; }

    /// <summary>Size of the table file in bytes.</summary>
    public abstract long FileSize { get; }

    /// <summary>The internal key comparator the table is read with.</summary>
    internal abstract InternalKeyComparator InternalComparator { get; }

    /// <summary>
    /// Size of the minimum timestamp padded into every key read from this file (0 when keys are stored with their
    /// timestamps or the comparator has none). Only block-based tables strip timestamps.
    /// </summary>
    public virtual int PadTimestampSize => 0;

    /// <summary>Effective global sequence number (external files), or <see cref="BlockIter.DisableGlobalSequenceNumber"/>.</summary>
    public virtual ulong GlobalSequenceNumber => BlockIter.DisableGlobalSequenceNumber;

    /// <summary>Whether the table has a range deletion block (only block-based tables support range deletions).</summary>
    public virtual bool HasRangeTombstones => false;

    /// <summary>Iterator over the range tombstones, or null if none (NewRangeTombstoneIterator).</summary>
    public virtual BlockIter? NewRangeTombstoneIterator() => null;

    /// <summary>
    /// Point lookup (TableReader::Get). <paramref name="internalKey"/> is a lookup key (user key + packed (snapshot seq,
    /// kValueTypeForSeek)). Calls <paramref name="visitor"/> for each entry with the same user key at or after the lookup
    /// key, in order, until it returns false.
    /// </summary>
    public abstract void Get<TState>(ReadOnlySpan<byte> internalKey, ref TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags)
        where TState : allows ref struct;

    /// <summary>
    /// Async <see cref="Get{TState}(ReadOnlySpan{byte}, ref TState, TableEntryVisitor{TState}, TableReadFlags)"/>. The
    /// formats other than block-based read the whole file at open, so their lookups complete synchronously.
    /// </summary>
    public virtual ValueTask GetAsync<TState>(ReadOnlyMemory<byte> internalKey, TState state, TableEntryVisitor<TState> visitor, TableReadFlags flags,
        CancellationToken cancellationToken) where TState : class
    {
        cancellationToken.ThrowIfCancellationRequested();
        Get(internalKey.Span, ref state, visitor, flags);
        return default;
    }

    /// <summary>
    /// Finds the first entry for the user key of <paramref name="internalKey"/> visible at the lookup sequence number and
    /// copies its value into <paramref name="value"/> (if non-null). Returns <c>Found=false</c> if no entry exists.
    /// </summary>
    public virtual TableGetResult Get(ReadOnlySpan<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags)
    {
        var state = new FirstEntryState(value);
        Get(internalKey, ref state, static (ref FirstEntryState s, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> v) =>
        {
            DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(ikey), out ulong seq, out EntryType t);
            s.Result = new TableGetResult(true, seq, t);
            s.Value?.Write(v);
            return false;
        }, flags);
        return state.Result;
    }

    /// <summary>Async <see cref="Get(ReadOnlySpan{byte}, IBufferWriter{byte}?, TableReadFlags)"/>.</summary>
    public virtual ValueTask<TableGetResult> GetAsync(ReadOnlyMemory<byte> internalKey, IBufferWriter<byte>? value, TableReadFlags flags,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<TableGetResult>(Get(internalKey.Span, value, flags));
    }

    private ref struct FirstEntryState(IBufferWriter<byte>? value)
    {
        public readonly IBufferWriter<byte>? Value = value;
        public TableGetResult Result;
    }

    /// <summary>
    /// Batched point lookup of <paramref name="keys"/>[0..<paramref name="count"/>) (key indices of
    /// <paramref name="batch"/> sorted by internal key). The default looks the keys up one by one (TableReader::MultiGet
    /// default implementation).
    /// </summary>
    internal virtual ValueTask MultiGetAsync<TBatch>(TBatch batch, int[] keys, int count, bool[] mayMatch, TableReadFlags flags, bool synchronous,
        CancellationToken cancellationToken)
        where TBatch : class, ITableMultiGetBatch
    {
        cancellationToken.ThrowIfCancellationRequested();
        for (int i = 0; i < count; i++)
        {
            var state = new MultiGetKeyState<TBatch>(batch, keys[i]);
            Get(batch.InternalKey(keys[i]), ref state,
                static (ref MultiGetKeyState<TBatch> s, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> v) => s.Batch.Visit(s.Key, ikey, v), flags);
        }
        return default;
    }

    private readonly struct MultiGetKeyState<TBatch>(TBatch batch, int key) where TBatch : class, ITableMultiGetBatch
    {
        public readonly TBatch Batch = batch;
        public readonly int Key = key;
    }

    /// <summary>
    /// Whether the table's filter (if any) says the user key of <paramref name="internalKey"/> may be present. Formats
    /// without a filter usable for this always answer true.
    /// </summary>
    public virtual bool InternalKeyMayMatch(ReadOnlySpan<byte> internalKey, TableReadFlags flags) => true;

    /// <summary>TableReader::ApproximateOffsetOf (plain and cuckoo tables return 0 like upstream).</summary>
    public virtual ulong ApproximateOffsetOf(ReadOnlySpan<byte> internalKey) => 0;

    /// <summary>
    /// TableReader::ApproximateMemoryUsage: heap bytes the reader keeps outside the block cache (pinned index and
    /// filter blocks, ...), reported by the "rocksdb.estimate-table-readers-mem" property. 0 by default.
    /// </summary>
    public virtual ulong ApproximateMemoryUsage => 0;

    /// <summary>TableReader::ApproximateSize (plain and cuckoo tables return 0 like upstream).</summary>
    public virtual ulong ApproximateSize(ReadOnlySpan<byte> startInternalKey, ReadOnlySpan<byte> endInternalKey) => 0;

    /// <summary>
    /// TableReader::ApproximateKeyAnchors. Upstream's default returns NotSupported, which subcompaction planning treats as
    /// "no anchors"; this default returns an empty list.
    /// </summary>
    public virtual List<(byte[] UserKey, ulong RangeSize)> ApproximateKeyAnchors() => [];

    /// <summary>
    /// TableReader::NewIterator: an iterator over the table's point entries honouring <paramref name="readOptions"/>'s
    /// prefix seek settings (null: total order) and the column family's <paramref name="prefixExtractor"/>. The iterator
    /// does not own the reader; dispose it before the reader.
    /// </summary>
    public abstract InternalIterator NewInternalIterator(TableReadFlags flags, ReadOptions? readOptions, SliceTransform? prefixExtractor);

    /// <summary>Releases the reader and its file.</summary>
    public abstract void Dispose();
}
