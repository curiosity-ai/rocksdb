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

    private static bool ContainsColumnFamily(List<ColumnFamily> list, ColumnFamily cf)
    {
        foreach (ColumnFamily c in list) if (c.Data == cf.Data) return true;
        return false;
    }

    private void CheckOwnColumnFamily(ColumnFamily cf)
    {
        if (cf.Db != _db) ThrowHelper.InvalidArgument("column family does not belong to this DB");
    }

    /// <summary>
    /// Reads the attribute groups of <paramref name="key"/> (DB::GetEntity(options, key, PinnableAttributeGroups*)): for
    /// each group, the entity of <paramref name="key"/> in the group's column family (a plain value reads as an entity
    /// with only the default column). All groups are read from one consistent view (the snapshot of
    /// <paramref name="options"/>, or an implicit one). Per-group outcomes are stored in the groups.
    /// </summary>
    public void GetAttributeGroups(ReadOptions? options, ReadOnlySpan<byte> key, ReadOnlySpan<PinnableAttributeGroup> attributeGroups)
    {
        _db.ThrowIfDisposed();
        if (attributeGroups.IsEmpty) return;
        var batch = new MultiGetBatch(1);
        try
        {
            batch.Add(key);
            ReadAttributeGroups(options, batch, attributeGroups, null);
        }
        finally
        {
            batch.Dispose();
        }
    }

    /// <summary>
    /// Batched <see cref="GetAttributeGroups(ReadOptions?, ReadOnlySpan{byte}, ReadOnlySpan{PinnableAttributeGroup})"/>
    /// (DB::MultiGetEntity(options, num_keys, keys, PinnableAttributeGroups* results)): <paramref name="results"/>[i]
    /// holds the attribute groups to read for <paramref name="keys"/>[i]. Every (key, column family) pair is read from
    /// one consistent view; each column family's keys are looked up with one batched MultiGet.
    /// </summary>
    public void MultiGetAttributeGroups(ReadOptions? options, IReadOnlyList<ReadOnlyMemory<byte>> keys, IReadOnlyList<PinnableAttributeGroup[]> results)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(results);
        if (keys.Count != results.Count) throw new ArgumentException("keys and results must have the same length");
        _db.ThrowIfDisposed();
        if (keys.Count == 0) return;
        var batch = new MultiGetBatch(keys.Count);
        try
        {
            for (int i = 0; i < keys.Count; i++) batch.Add(keys[i].Span);
            ReadAttributeGroups(options, batch, default, results);
        }
        finally
        {
            batch.Dispose();
        }
    }

    /// <summary>
    /// The shared part of the attribute-group reads: under one snapshot, one MultiGetEntity per distinct column family
    /// over the keys of <paramref name="keys"/> that ask for it. <paramref name="single"/> are the groups of the only key
    /// (GetEntity) when <paramref name="perKey"/> is null.
    /// </summary>
    private void ReadAttributeGroups(ReadOptions? options, MultiGetBatch keys, ReadOnlySpan<PinnableAttributeGroup> single,
        IReadOnlyList<PinnableAttributeGroup[]>? perKey)
    {
        int n = keys.Count;
        // Distinct column families in first-use order.
        var cfs = new List<ColumnFamily>();
        for (int i = 0; i < n; i++)
        {
            ReadOnlySpan<PinnableAttributeGroup> groups = perKey is null ? single : perKey[i];
            foreach (PinnableAttributeGroup g in groups)
            {
                ArgumentNullException.ThrowIfNull(g);
                CheckOwnColumnFamily(g.ColumnFamily);
                g.Reset();
                if (!ContainsColumnFamily(cfs, g.ColumnFamily)) cfs.Add(g.ColumnFamily);
            }
        }
        // One consistent view across column families (upstream: MultiCFSnapshot). An explicit snapshot already is one;
        // otherwise an implicit snapshot pins the sequence, and each column family's super version is referenced after it.
        Snapshot? owned = null;
        ReadOptions? ro = options;
        if (cfs.Count > 1 && options?.Snapshot is null)
        {
            owned = _db.GetSnapshot();
            ro = options?.Clone() ?? new ReadOptions();
            ro.Snapshot = owned;
        }
        var sub = new MultiGetBatch(n);
        var subKey = new int[n];
        try
        {
            foreach (ColumnFamily cf in cfs)
            {
                sub.Clear();
                int m = 0;
                for (int i = 0; i < n; i++)
                {
                    ReadOnlySpan<PinnableAttributeGroup> groups = perKey is null ? single : perKey[i];
                    foreach (PinnableAttributeGroup g in groups)
                    {
                        if (g.ColumnFamily.Data != cf.Data) continue;
                        sub.Add(keys.GetKey(i));
                        subKey[m++] = i;
                        break;
                    }
                }
                RocksDbException? callError = null;
                try
                {
                    cf.MultiGetEntity(ro, sub);
                }
                catch (RocksDbException e)
                {
                    callError = e;
                }
                for (int j = 0; j < m; j++)
                {
                    int i = subKey[j];
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
        finally
        {
            sub.Dispose();
            owned?.Dispose();
        }
    }
}
