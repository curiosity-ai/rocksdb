using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>Result of a lookup of one key in an <see cref="IndexedWriteBatch"/> (WBWIIteratorImpl::Result).</summary>
internal enum BatchLookupResult : byte
{
    /// <summary>The batch has no entry for the key.</summary>
    NotFound,
    /// <summary>The batch determines the value (a put, possibly followed by merges).</summary>
    Found,
    /// <summary>The newest base entry is a deletion and no merges follow it.</summary>
    Deleted,
    /// <summary>Only merges (or merges on top of a deletion): the base value must come from the DB (unless deleted).</summary>
    MergeInProgress,
}

/// <summary>
/// Minimal internal WriteBatchWithIndex (utilities/write_batch_with_index/write_batch_with_index.cc) in
/// <c>overwrite_key = true</c> mode, as used by transactions: a <see cref="WriteBatch"/> plus a hash index from
/// (column family, key) to the key's latest base operation (put / delete / single delete) and the merges after it.
/// Ordered iteration (for <see cref="TransactionIterator"/>) sorts the indexed keys on demand with the column family
/// comparator. Records reference the batch rep by offset, so indexing a key allocates nothing beyond amortized
/// growth of the index arrays.
/// <para>
/// TODO(consolidation): replace with the public <c>Utilities/WriteBatchWithIndex</c> implementation once merged.
/// </para>
/// </summary>
internal sealed class IndexedWriteBatch : IDisposable
{
    private readonly WriteBatch _batch;
    private readonly bool _withNoop;
    private KeyEntry[] _entries = new KeyEntry[16];
    private int _entryCount;
    private MergeRef[] _merges = new MergeRef[8];
    private int _mergeCount;
    private readonly Dictionary<IndexKey, int> _index;
    private readonly Dictionary<IndexKey, int>.AlternateLookup<SpanKey> _lookup;
    private int _sortedVersion = -1; // _entryCount when the per-CF sorted arrays were built
    private readonly Dictionary<uint, int[]> _sorted = new();

    private struct KeyEntry
    {
        public uint Cf;
        public int KeyOffset;
        public int KeyLength;
        public EntryType BaseType;   // Value, Deletion, SingleDeletion or Merge (= no base)
        public int BaseValueOffset;
        public int BaseValueLength;
        public int FirstMerge;       // -1 if none; linked list oldest -> newest
        public int LastMerge;
    }

    private struct MergeRef
    {
        public int ValueOffset;
        public int ValueLength;
        public int Next;
    }

    internal readonly record struct IndexKey(uint Cf, int Offset, int Length);

    internal readonly ref struct SpanKey(uint cf, ReadOnlySpan<byte> key)
    {
        public readonly uint Cf = cf;
        public readonly ReadOnlySpan<byte> Key = key;
    }

    private sealed class IndexComparer(IndexedWriteBatch owner) : IEqualityComparer<IndexKey>, IAlternateEqualityComparer<SpanKey, IndexKey>
    {
        private ReadOnlySpan<byte> Key(IndexKey k) => owner._batch.Data.Slice(k.Offset, k.Length);
        public bool Equals(IndexKey x, IndexKey y) => x.Cf == y.Cf && Key(x).SequenceEqual(Key(y));
        public int GetHashCode(IndexKey k) => ByteKeyComparer.Hash(Key(k)) ^ (int)k.Cf;
        public bool Equals(SpanKey alternate, IndexKey other) => alternate.Cf == other.Cf && alternate.Key.SequenceEqual(Key(other));
        public int GetHashCode(SpanKey alternate) => ByteKeyComparer.Hash(alternate.Key) ^ (int)alternate.Cf;
        public IndexKey Create(SpanKey alternate) => throw new NotSupportedException();
    }

    /// <param name="withNoop">Start the batch (and every <see cref="Clear"/>) with a Noop marker, like a transaction's
    /// batch (TransactionBaseImpl::InitWriteBatch), so that it can later become a prepared section.</param>
    public IndexedWriteBatch(bool withNoop, int maxBytes)
    {
        _batch = new WriteBatch(0, maxBytes);
        _withNoop = withNoop;
        _index = new Dictionary<IndexKey, int>(new IndexComparer(this));
        _lookup = _index.GetAlternateLookup<SpanKey>();
        if (withNoop) _batch.InsertNoop();
    }

    /// <summary>The underlying batch (its rep is what is written to the WAL / memtables).</summary>
    public WriteBatch Batch => _batch;

    public int Count => _batch.Count;

    public void Clear()
    {
        _batch.Clear();
        if (_withNoop) _batch.InsertNoop();
        ClearIndex();
    }

    private void ClearIndex()
    {
        _index.Clear();
        _entryCount = 0;
        _mergeCount = 0;
        _sortedVersion = -1;
        _sorted.Clear();
    }

    public void Dispose() => _batch.Dispose();

    // ---- writes -------------------------------------------------------------------------------------------

    public void Put(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        int before = _batch.DataSize;
        _batch.Put(cf, key, value);
        IndexRecord(before);
    }

    public void Delete(uint cf, ReadOnlySpan<byte> key)
    {
        int before = _batch.DataSize;
        _batch.Delete(cf, key);
        IndexRecord(before);
    }

    public void SingleDelete(uint cf, ReadOnlySpan<byte> key)
    {
        int before = _batch.DataSize;
        _batch.SingleDelete(cf, key);
        IndexRecord(before);
    }

    public void Merge(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        int before = _batch.DataSize;
        _batch.Merge(cf, key, value);
        IndexRecord(before);
    }

    public void PutLogData(ReadOnlySpan<byte> blob) => _batch.PutLogData(blob);

    public void SetSavePoint() => _batch.SetSavePoint();

    /// <summary>WriteBatchWithIndex::RollbackToSavePoint: rolls the batch back and rebuilds the index.</summary>
    public bool RollbackToSavePoint()
    {
        if (!_batch.RollbackToSavePoint()) return false;
        RebuildIndex();
        return true;
    }

    public bool PopSavePoint() => _batch.PopSavePoint();

    /// <summary>Re-indexes the whole batch (WriteBatchWithIndex::Rep::ReBuildIndex).</summary>
    public void RebuildIndex()
    {
        ClearIndex();
        ReadOnlySpan<byte> rep = _batch.Data;
        int pos = WriteBatch.HeaderSize;
        while (pos < rep.Length)
        {
            int start = pos;
            string? error = WriteBatchReader.TryReadRecord(rep, ref pos, out WriteBatchEntry e);
            if (error is not null) ThrowHelper.Corruption(error);
            if (e.Type is EntryType.Value or EntryType.Deletion or EntryType.SingleDeletion or EntryType.Merge) IndexRecord(start);
        }
    }

    [SkipLocalsInit]
    private void IndexRecord(int recordOffset)
    {
        ReadOnlySpan<byte> rep = _batch.Data;
        int pos = recordOffset;
        string? error = WriteBatchReader.TryReadRecord(rep, ref pos, out WriteBatchEntry e);
        if (error is not null) ThrowHelper.Corruption(error);
        ref byte origin = ref MemoryMarshal.GetReference(rep);
        int keyOffset = (int)Unsafe.ByteOffset(ref origin, ref MemoryMarshal.GetReference(e.Key));
        int valueOffset = e.Value.IsEmpty ? 0 : (int)Unsafe.ByteOffset(ref origin, ref MemoryMarshal.GetReference(e.Value));
        ref int slot = ref CollectionsMarshal.GetValueRefOrNullRef(_lookup, new SpanKey(e.ColumnFamilyId, e.Key));
        int idx;
        if (Unsafe.IsNullRef(ref slot))
        {
            idx = _entryCount;
            if (idx == _entries.Length) Array.Resize(ref _entries, idx * 2);
            _entries[idx] = new KeyEntry { Cf = e.ColumnFamilyId, KeyOffset = keyOffset, KeyLength = e.Key.Length, BaseType = EntryType.Merge, FirstMerge = -1, LastMerge = -1 };
            _entryCount++;
            _index.Add(new IndexKey(e.ColumnFamilyId, keyOffset, e.Key.Length), idx);
        }
        else
        {
            idx = slot;
        }
        ref KeyEntry entry = ref _entries[idx];
        if (e.Type == EntryType.Merge)
        {
            int m = _mergeCount;
            if (m == _merges.Length) Array.Resize(ref _merges, m * 2);
            _merges[m] = new MergeRef { ValueOffset = valueOffset, ValueLength = e.Value.Length, Next = -1 };
            _mergeCount++;
            if (entry.LastMerge < 0) entry.FirstMerge = m;
            else _merges[entry.LastMerge].Next = m;
            entry.LastMerge = m;
        }
        else
        {
            // overwrite_key: a put / delete replaces whatever the batch held for the key.
            entry.BaseType = e.Type;
            entry.BaseValueOffset = valueOffset;
            entry.BaseValueLength = e.Value.Length;
            entry.FirstMerge = -1;
            entry.LastMerge = -1;
        }
    }

    // ---- reads --------------------------------------------------------------------------------------------

    /// <summary>Finds the index entry of a key; -1 if the batch has none.</summary>
    public int Find(uint cf, ReadOnlySpan<byte> key) => _lookup.TryGetValue(new SpanKey(cf, key), out int idx) ? idx : -1;

    /// <summary>Classifies entry <paramref name="idx"/> (see <see cref="BatchLookupResult"/>).</summary>
    public BatchLookupResult Classify(int idx)
    {
        if (idx < 0) return BatchLookupResult.NotFound;
        ref KeyEntry e = ref _entries[idx];
        if (e.FirstMerge >= 0) return e.BaseType == EntryType.Merge ? BatchLookupResult.MergeInProgress : BatchLookupResult.Found;
        return e.BaseType == EntryType.Value ? BatchLookupResult.Found : e.BaseType == EntryType.Merge ? BatchLookupResult.MergeInProgress : BatchLookupResult.Deleted;
    }

    public ReadOnlySpan<byte> EntryKey(int idx)
    {
        ref KeyEntry e = ref _entries[idx];
        return _batch.Data.Slice(e.KeyOffset, e.KeyLength);
    }

    public uint EntryColumnFamily(int idx) => _entries[idx].Cf;

    public bool HasMerges(int idx) => _entries[idx].FirstMerge >= 0;

    /// <summary>The base put value of a <see cref="BatchLookupResult.Found"/> entry without merges.</summary>
    public ReadOnlySpan<byte> BaseValue(int idx)
    {
        ref KeyEntry e = ref _entries[idx];
        return _batch.Data.Slice(e.BaseValueOffset, e.BaseValueLength);
    }

    /// <summary>
    /// Computes the value of entry <paramref name="idx"/> into <paramref name="result"/>: the batch's base value (or,
    /// when the batch has only merges, <paramref name="dbValue"/> / no value) with the batch's merges applied
    /// (WriteBatchWithIndexInternal::MergeKey). Returns false if the key does not exist.
    /// </summary>
    public bool ComputeValue(int idx, MergeOperator? mergeOperator, bool dbHasValue, ReadOnlySpan<byte> dbValue, IBufferWriter<byte> result)
    {
        ref KeyEntry e = ref _entries[idx];
        ReadOnlySpan<byte> rep = _batch.Data;
        bool hasBase;
        ReadOnlySpan<byte> baseValue;
        switch (e.BaseType)
        {
            case EntryType.Value:
                hasBase = true;
                baseValue = rep.Slice(e.BaseValueOffset, e.BaseValueLength);
                break;
            case EntryType.Merge:
                hasBase = dbHasValue;
                baseValue = dbValue;
                break;
            default:
                hasBase = false;
                baseValue = default;
                break;
        }
        if (e.FirstMerge < 0)
        {
            if (!hasBase) return false;
            result.Write(baseValue);
            return true;
        }
        if (mergeOperator is null) ThrowHelper.InvalidArgument("Merge operator not specified for the column family");
        int n = 0;
        int total = 0;
        for (int m = e.FirstMerge; m >= 0; m = _merges[m].Next)
        {
            n++;
            total += _merges[m].ValueLength;
        }
        byte[] data = ArrayPool<byte>.Shared.Rent(Math.Max(total, 1));
        int[] offsets = ArrayPool<int>.Shared.Rent(n + 1);
        try
        {
            int p = 0, i = 0;
            for (int m = e.FirstMerge; m >= 0; m = _merges[m].Next)
            {
                offsets[i++] = p;
                rep.Slice(_merges[m].ValueOffset, _merges[m].ValueLength).CopyTo(data.AsSpan(p));
                p += _merges[m].ValueLength;
            }
            offsets[i] = p;
            var operands = new MergeOperands(data.AsSpan(0, p), offsets.AsSpan(0, n + 1));
            MergeHelper.TimedFullMerge(mergeOperator!, rep.Slice(e.KeyOffset, e.KeyLength), hasBase, baseValue, operands, result);
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(data);
            ArrayPool<int>.Shared.Return(offsets);
        }
    }

    /// <summary>The batch's entries of column family <paramref name="cf"/> sorted by <paramref name="comparator"/> (cached until the next new key).</summary>
    public int[] SortedEntries(uint cf, Comparator comparator)
    {
        if (_sortedVersion != _entryCount)
        {
            _sorted.Clear();
            _sortedVersion = _entryCount;
        }
        if (_sorted.TryGetValue(cf, out int[]? sorted)) return sorted;
        int n = 0;
        for (int i = 0; i < _entryCount; i++)
        {
            if (_entries[i].Cf == cf) n++;
        }
        sorted = new int[n];
        n = 0;
        for (int i = 0; i < _entryCount; i++)
        {
            if (_entries[i].Cf == cf) sorted[n++] = i;
        }
        var cmp = new EntryKeyComparer(this, comparator);
        Array.Sort(sorted, cmp);
        _sorted[cf] = sorted;
        return sorted;
    }

    private sealed class EntryKeyComparer(IndexedWriteBatch owner, Comparator comparator) : IComparer<int>
    {
        public int Compare(int x, int y) => comparator.Compare(owner.EntryKey(x), owner.EntryKey(y));
    }

    /// <summary>Version stamp of the sorted views: changes whenever a new key is indexed or the index is rebuilt.</summary>
    public int IndexVersion => _entryCount ^ (_mergeCount << 16) ^ _batch.DataSize;
}
