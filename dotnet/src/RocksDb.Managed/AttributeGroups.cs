using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>
/// An attribute group: the wide columns a key has in one column family (rocksdb::AttributeGroup,
/// include/rocksdb/attribute_groups.h). Attribute groups split an entity across column families (for example to give
/// hot and cold columns different options) while writing and reading them as one logical row: see
/// <see cref="RocksDatabase.PutAttributeGroups(ReadOnlySpan{byte}, ReadOnlySpan{AttributeGroup})"/>,
/// <see cref="RocksDatabase.GetAttributeGroups(ReadOptions?, ReadOnlySpan{byte}, ReadOnlySpan{PinnableAttributeGroup})"/> and
/// <see cref="RocksDatabase.NewAttributeGroupIterator(ReadOptions?, ReadOnlySpan{ColumnFamily})"/>.
/// </summary>
public readonly struct AttributeGroup
{
    public AttributeGroup(ColumnFamily columnFamily, ReadOnlyMemory<WideColumn> columns)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        ColumnFamily = columnFamily;
        Columns = columns;
    }

    /// <summary>The column family holding this group's columns.</summary>
    public ColumnFamily ColumnFamily { get; }

    /// <summary>The columns (any order; names unique within the group).</summary>
    public ReadOnlyMemory<WideColumn> Columns { get; }
}

/// <summary>
/// Read-path attribute group (rocksdb::PinnableAttributeGroup): names the column family to read and receives the
/// outcome (<see cref="Status"/>, <see cref="Error"/>) and the <see cref="Columns"/> of the key in that column family.
/// Reusable: pass the same instances to repeated reads to avoid allocating once the column buffers are large enough.
/// Not thread-safe.
/// </summary>
public sealed class PinnableAttributeGroup
{
    public PinnableAttributeGroup(ColumnFamily columnFamily)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        ColumnFamily = columnFamily;
    }

    /// <summary>The column family to read.</summary>
    public ColumnFamily ColumnFamily { get; }

    /// <summary>Found, NotFound (the key has no entry in this column family) or Error (see <see cref="Error"/>).</summary>
    public MultiGetStatus Status { get; private set; }

    /// <summary>The failure of this group's read when <see cref="Status"/> is Error, else null.</summary>
    public RocksDbException? Error { get; private set; }

    /// <summary>The columns read (empty unless <see cref="Status"/> is Found). Valid until the next read into this group.</summary>
    public WideColumns Columns { get; } = new();

    /// <summary>PinnableAttributeGroup::Reset.</summary>
    internal void Reset()
    {
        Status = MultiGetStatus.NotFound;
        Error = null;
        Columns.Clear();
    }

    internal void SetFound(ReadOnlySpan<byte> serializedEntity)
    {
        Status = MultiGetStatus.Found;
        Error = null;
        Columns.SetSerialized(serializedEntity);
    }

    internal void SetError(RocksDbException error)
    {
        Columns.Clear();
        Status = MultiGetStatus.Error;
        Error = error;
    }
}

/// <summary>Attribute-group writes and reads (DB::PutEntity / GetEntity / MultiGetEntity with attribute groups).</summary>
public sealed partial class RocksDatabase
{
    /// <summary>
    /// Writes the attribute groups of <paramref name="key"/> atomically: one wide-column entity per group in the group's
    /// column family, all in one write batch (DB::PutEntity(options, key, attribute_groups)). InvalidArgument when
    /// <paramref name="attributeGroups"/> is empty or a column family has user-defined timestamps.
    /// </summary>
    public void PutAttributeGroups(ReadOnlySpan<byte> key, ReadOnlySpan<AttributeGroup> attributeGroups) => PutAttributeGroups(null, key, attributeGroups);

    /// <inheritdoc cref="PutAttributeGroups(ReadOnlySpan{byte}, ReadOnlySpan{AttributeGroup})"/>
    public void PutAttributeGroups(WriteOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<AttributeGroup> attributeGroups)
    {
        ValidateAttributeGroups(attributeGroups);
        WriteBatch b = WriteBatch.Rent();
        try
        {
            b.PutAttributeGroups(key, attributeGroups);
            _db.WriteAsync(options, b, validateColumnFamilies: true, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
        }
        finally
        {
            b.Dispose();
        }
    }

    /// <summary>Async <see cref="PutAttributeGroups(WriteOptions?, ReadOnlySpan{byte}, ReadOnlySpan{AttributeGroup})"/> (group commit).</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PutAttributeGroupsAsync(WriteOptions? options, ReadOnlyMemory<byte> key, ReadOnlyMemory<AttributeGroup> attributeGroups,
        CancellationToken cancellationToken = default)
    {
        ValidateAttributeGroups(attributeGroups.Span);
        WriteBatch b = WriteBatch.Rent();
        try
        {
            b.PutAttributeGroups(key.Span, attributeGroups.Span);
            await _db.WriteAsync(options, b, validateColumnFamilies: true, synchronous: false, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
    }

    /// <summary>DBImpl::PutEntity(attribute_groups) argument checks.</summary>
    private void ValidateAttributeGroups(ReadOnlySpan<AttributeGroup> attributeGroups)
    {
        _db.ThrowIfDisposed();
        if (attributeGroups.IsEmpty) ThrowHelper.InvalidArgument("Cannot call this method with empty attribute groups");
        for (int gi = 0; gi < attributeGroups.Length; gi++)
        {
            AttributeGroup ag = attributeGroups[gi];
            ColumnFamily cf = ag.ColumnFamily ?? throw new ArgumentException("Attribute group without a column family");
            CheckOwnColumnFamily(cf);
            if (cf.TimestampSize != 0)
            {
                // DBImpl::FailIfCfHasTs
                ThrowHelper.InvalidArgument("cannot call this method on column family " + cf.Name + " that enables timestamp");
            }
        }
    }

    private void CheckOwnColumnFamily(ColumnFamily cf)
    {
        if (cf.Db != _db) ThrowHelper.InvalidArgument("column family does not belong to this DB");
    }

    /// <summary>
    /// Reads the attribute groups of <paramref name="key"/> (DB::GetEntity(options, key, PinnableAttributeGroups*)): for
    /// each group, the entity of <paramref name="key"/> in the group's column family (a plain value reads as an entity
    /// with only the default column). All groups are read from one consistent view (the snapshot of
    /// <paramref name="options"/>, or an implicit one). Per-group outcomes are stored in the groups. Reusing the same
    /// <see cref="PinnableAttributeGroup"/> objects makes repeated reads allocation-free.
    /// </summary>
    public void GetAttributeGroups(ReadOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<PinnableAttributeGroup> attributeGroups)
    {
        _db.ThrowIfDisposed();
        if (attributeGroups.IsEmpty) return;
        AttributeGroupReadScratch scratch = AttributeGroupReadScratch.Rent();
        try
        {
            ReadAttributeGroupsPrepare(scratch, key, null, attributeGroups, null);
            Snapshot? owned = ReadAttributeGroupsView(scratch, options, out ReadOptions? ro, out Snapshot? snap);
            try
            {
                for (int c = 0; c < scratch.CfCount; c++)
                {
                    ColumnFamily cf = scratch.Cfs[c];
                    int m = ReadAttributeGroupsFill(scratch, cf, key, null, attributeGroups, null);
                    RocksDbException? callError = null;
                    try
                    {
                        _db.MultiGetAsync(cf.Data, ro, snap, scratch.Batch, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
                    }
                    catch (RocksDbException e)
                    {
                        callError = e;
                    }
                    ReadAttributeGroupsDistribute(scratch, cf, m, callError, attributeGroups, null);
                }
            }
            finally
            {
                if (owned is not null) _db.ReleaseInternalSnapshot(owned);
            }
        }
        finally
        {
            AttributeGroupReadScratch.Return(scratch);
        }
    }

    /// <summary>
    /// Async <see cref="GetAttributeGroups(ReadOptions?, ReadOnlySpan{byte}, ReadOnlySpan{PinnableAttributeGroup})"/>:
    /// data blocks missing from the block cache are read with async I/O (completes synchronously when everything is
    /// cached or in memtables).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask GetAttributeGroupsAsync(ReadOptions? options, ReadOnlyMemory<byte> key, ReadOnlyMemory<PinnableAttributeGroup> attributeGroups,
        CancellationToken cancellationToken = default)
    {
        _db.ThrowIfDisposed();
        if (attributeGroups.IsEmpty) return;
        AttributeGroupReadScratch scratch = AttributeGroupReadScratch.Rent();
        try
        {
            ReadAttributeGroupsPrepare(scratch, key.Span, null, attributeGroups.Span, null);
            Snapshot? owned = ReadAttributeGroupsView(scratch, options, out ReadOptions? ro, out Snapshot? snap);
            try
            {
                for (int c = 0; c < scratch.CfCount; c++)
                {
                    ColumnFamily cf = scratch.Cfs[c];
                    int m = ReadAttributeGroupsFill(scratch, cf, key.Span, null, attributeGroups.Span, null);
                    RocksDbException? callError = null;
                    try
                    {
                        await _db.MultiGetAsync(cf.Data, ro, snap, scratch.Batch, synchronous: false, cancellationToken).ConfigureAwait(false);
                    }
                    catch (RocksDbException e)
                    {
                        callError = e;
                    }
                    ReadAttributeGroupsDistribute(scratch, cf, m, callError, attributeGroups.Span, null);
                }
            }
            finally
            {
                if (owned is not null) _db.ReleaseInternalSnapshot(owned);
            }
        }
        finally
        {
            AttributeGroupReadScratch.Return(scratch);
        }
    }

    /// <summary>
    /// Batched <see cref="GetAttributeGroups(ReadOptions?, ReadOnlySpan{byte}, ReadOnlySpan{PinnableAttributeGroup})"/>
    /// (DB::MultiGetEntity(options, num_keys, keys, PinnableAttributeGroups* results)): <paramref name="results"/>[i]
    /// holds the attribute groups to read for <paramref name="keys"/>[i]. Every (key, column family) pair is read from
    /// one consistent view; each column family's keys are looked up with one batched MultiGet. Reusing the same key list
    /// and result groups makes repeated calls allocation-free.
    /// </summary>
    public void MultiGetAttributeGroups(ReadOptions? options, IReadOnlyList<ReadOnlyMemory<byte>> keys, IReadOnlyList<PinnableAttributeGroup[]> results)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(results);
        if (keys.Count != results.Count) throw new ArgumentException("keys and results must have the same length");
        _db.ThrowIfDisposed();
        if (keys.Count == 0) return;
        AttributeGroupReadScratch scratch = AttributeGroupReadScratch.Rent();
        try
        {
            ReadAttributeGroupsPrepare(scratch, default, keys, default, results);
            Snapshot? owned = ReadAttributeGroupsView(scratch, options, out ReadOptions? ro, out Snapshot? snap);
            try
            {
                for (int c = 0; c < scratch.CfCount; c++)
                {
                    ColumnFamily cf = scratch.Cfs[c];
                    int m = ReadAttributeGroupsFill(scratch, cf, default, keys, default, results);
                    RocksDbException? callError = null;
                    try
                    {
                        _db.MultiGetAsync(cf.Data, ro, snap, scratch.Batch, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
                    }
                    catch (RocksDbException e)
                    {
                        callError = e;
                    }
                    ReadAttributeGroupsDistribute(scratch, cf, m, callError, default, results);
                }
            }
            finally
            {
                if (owned is not null) _db.ReleaseInternalSnapshot(owned);
            }
        }
        finally
        {
            AttributeGroupReadScratch.Return(scratch);
        }
    }

    /// <summary>
    /// Async <see cref="MultiGetAttributeGroups(ReadOptions?, IReadOnlyList{ReadOnlyMemory{byte}}, IReadOnlyList{PinnableAttributeGroup[]})"/>:
    /// data blocks missing from the block cache are read with async I/O.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask MultiGetAttributeGroupsAsync(ReadOptions? options, IReadOnlyList<ReadOnlyMemory<byte>> keys,
        IReadOnlyList<PinnableAttributeGroup[]> results, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(results);
        if (keys.Count != results.Count) throw new ArgumentException("keys and results must have the same length");
        _db.ThrowIfDisposed();
        if (keys.Count == 0) return;
        AttributeGroupReadScratch scratch = AttributeGroupReadScratch.Rent();
        try
        {
            ReadAttributeGroupsPrepare(scratch, default, keys, default, results);
            Snapshot? owned = ReadAttributeGroupsView(scratch, options, out ReadOptions? ro, out Snapshot? snap);
            try
            {
                for (int c = 0; c < scratch.CfCount; c++)
                {
                    ColumnFamily cf = scratch.Cfs[c];
                    int m = ReadAttributeGroupsFill(scratch, cf, default, keys, default, results);
                    RocksDbException? callError = null;
                    try
                    {
                        await _db.MultiGetAsync(cf.Data, ro, snap, scratch.Batch, synchronous: false, cancellationToken).ConfigureAwait(false);
                    }
                    catch (RocksDbException e)
                    {
                        callError = e;
                    }
                    ReadAttributeGroupsDistribute(scratch, cf, m, callError, default, results);
                }
            }
            finally
            {
                if (owned is not null) _db.ReleaseInternalSnapshot(owned);
            }
        }
        finally
        {
            AttributeGroupReadScratch.Return(scratch);
        }
    }

    // The attribute-group reads in phases (shared by the sync and async methods): one MultiGetEntity per distinct column
    // family over the keys that ask for it, under one view. Keys are the single key (perKey == null) or keys[i]; groups
    // are the single key's groups or perKey[i].

    /// <summary>Validates and resets every group and collects the distinct column families in first-use order.</summary>
    private void ReadAttributeGroupsPrepare(AttributeGroupReadScratch scratch, ReadOnlySpan<byte> singleKey, IReadOnlyList<ReadOnlyMemory<byte>>? keys,
        ReadOnlySpan<PinnableAttributeGroup> single, IReadOnlyList<PinnableAttributeGroup[]>? perKey)
    {
        int n = perKey is null ? 1 : perKey.Count;
        scratch.Begin(n);
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<PinnableAttributeGroup> groups = perKey is null ? single : perKey[i];
            foreach (PinnableAttributeGroup g in groups)
            {
                ArgumentNullException.ThrowIfNull(g);
                CheckOwnColumnFamily(g.ColumnFamily);
                g.Reset();
                scratch.AddColumnFamily(g.ColumnFamily);
            }
        }
    }

    /// <summary>
    /// The consistent view across column families (upstream: MultiCFSnapshot). An explicit snapshot already is one;
    /// otherwise an internal snapshot pins the sequence (returned as the snapshot to release), and each column family's
    /// super version is referenced after it. <paramref name="ro"/> / <paramref name="snapshot"/> are what the per-column
    /// family MultiGets read with.
    /// </summary>
    private Snapshot? ReadAttributeGroupsView(AttributeGroupReadScratch scratch, ReadOptions? options, out ReadOptions? ro, out Snapshot? snapshot)
    {
        ro = options;
        snapshot = options?.Snapshot;
        if (scratch.CfCount <= 1 || snapshot is not null) return null;
        Snapshot owned = _db.AcquireInternalSnapshot();
        bool overridable = _db.TxnHooks is null;
        for (int c = 0; c < scratch.CfCount && overridable; c++) overridable = scratch.Cfs[c].TimestampSize == 0;
        if (!overridable)
        {
            // Transaction DBs and timestamped column families read the snapshot from the options.
            ro = options?.Clone() ?? new ReadOptions();
            ro.Snapshot = owned;
        }
        snapshot = owned;
        return owned;
    }

    /// <summary>Fills the scratch batch with the keys asking for <paramref name="cf"/>; returns their count.</summary>
    private static int ReadAttributeGroupsFill(AttributeGroupReadScratch scratch, ColumnFamily cf, ReadOnlySpan<byte> singleKey,
        IReadOnlyList<ReadOnlyMemory<byte>>? keys, ReadOnlySpan<PinnableAttributeGroup> single, IReadOnlyList<PinnableAttributeGroup[]>? perKey)
    {
        MultiGetBatch sub = scratch.Batch;
        sub.Clear();
        sub.EntityRun = true;
        int n = perKey is null ? 1 : perKey.Count;
        int m = 0;
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<PinnableAttributeGroup> groups = perKey is null ? single : perKey[i];
            foreach (PinnableAttributeGroup g in groups)
            {
                if (g.ColumnFamily.Data != cf.Data) continue;
                sub.Add(keys is null ? singleKey : keys[i].Span);
                scratch.SubKey[m++] = i;
                break;
            }
        }
        return m;
    }

    /// <summary>Stores the outcome of the MultiGet over <paramref name="cf"/>'s keys in their groups.</summary>
    private static void ReadAttributeGroupsDistribute(AttributeGroupReadScratch scratch, ColumnFamily cf, int m, RocksDbException? callError,
        ReadOnlySpan<PinnableAttributeGroup> single, IReadOnlyList<PinnableAttributeGroup[]>? perKey)
    {
        MultiGetBatch sub = scratch.Batch;
        for (int j = 0; j < m; j++)
        {
            int i = scratch.SubKey[j];
            ReadOnlySpan<PinnableAttributeGroup> groups = perKey is null ? single : perKey[i];
            foreach (PinnableAttributeGroup g in groups)
            {
                if (g.ColumnFamily.Data != cf.Data) continue;
                if (callError is not null) g.SetError(callError);
                else
                {
                    switch (sub.GetStatus(j))
                    {
                        case MultiGetStatus.Found: g.SetFound(sub.GetSerializedEntity(j)); break;
                        case MultiGetStatus.Error: g.SetError(sub.GetError(j)!); break;
                        default: g.Reset(); break;
                    }
                }
            }
        }
    }
}

/// <summary>
/// Reusable scratch of the attribute-group reads: the per-column-family MultiGet batch, the batch-index-to-key map and
/// the distinct column families. One instance is cached per thread (a read that suspends returns it on whichever
/// thread it completes on; a nested read on the same thread gets its own).
/// </summary>
internal sealed class AttributeGroupReadScratch
{
    private const int MaxCachedKeys = 1024;

    [ThreadStatic] private static AttributeGroupReadScratch? t_cached;

    public readonly MultiGetBatch Batch = new(16);
    public int[] SubKey = new int[16];
    public ColumnFamily[] Cfs = new ColumnFamily[4];
    public int CfCount;

    public static AttributeGroupReadScratch Rent()
    {
        AttributeGroupReadScratch? s = t_cached;
        if (s is null) return new AttributeGroupReadScratch();
        t_cached = null;
        return s;
    }

    public static void Return(AttributeGroupReadScratch s)
    {
        s.Batch.Clear();
        Array.Clear(s.Cfs, 0, s.CfCount);
        s.CfCount = 0;
        if (s.SubKey.Length > MaxCachedKeys || t_cached is not null)
        {
            s.Batch.Dispose();
            return;
        }
        t_cached = s;
    }

    /// <summary>Prepares for <paramref name="keys"/> keys.</summary>
    public void Begin(int keys)
    {
        if (SubKey.Length < keys) SubKey = new int[Math.Max(keys, SubKey.Length * 2)];
        CfCount = 0;
    }

    /// <summary>Appends <paramref name="cf"/> unless already present (the handful of column families of a read: linear scan).</summary>
    public void AddColumnFamily(ColumnFamily cf)
    {
        for (int i = 0; i < CfCount; i++)
        {
            if (Cfs[i].Data == cf.Data) return;
        }
        if (CfCount == Cfs.Length) Array.Resize(ref Cfs, CfCount * 2);
        Cfs[CfCount++] = cf;
    }
}
