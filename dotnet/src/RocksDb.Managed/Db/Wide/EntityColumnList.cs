using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Wide;

/// <summary>
/// A reusable list of entity columns (name, value, blob-backed flag) whose bytes are copied into one pooled buffer; the
/// input of <see cref="WideColumnSerialization.Serialize(EntityColumnList, System.Buffers.IBufferWriter{byte})"/> and
/// <see cref="WideColumnSerialization.SerializeV2"/>. Plays the role of upstream's
/// <c>(WideColumns, std::vector&lt;std::pair&lt;size_t, BlobIndex&gt;&gt; blob_columns)</c> pair when an entity is
/// rewritten (CompactionIterator::ExtractLargeColumnValuesIfNeeded / SerializeEntityAfterGC,
/// BlobWriteBatchTransformer::MaybePreprocessWideColumns). Columns must be added in name order. Not thread-safe.
/// </summary>
internal sealed class EntityColumnList : IDisposable
{
    private readonly PooledBufferWriter _data = new(256);
    private readonly List<(int NameOffset, int NameLength, int ValueOffset, int ValueLength, bool Blob)> _columns = new(8);

    public int Count => _columns.Count;

    /// <summary>True if a column added with <c>isBlobIndex</c> is present.</summary>
    public bool HasBlobColumns { get; private set; }

    public void Clear()
    {
        _data.Clear();
        _columns.Clear();
        HasBlobColumns = false;
    }

    /// <summary>Appends a column; <paramref name="value"/> is a serialized blob index when <paramref name="isBlobIndex"/>.</summary>
    public void Add(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value, bool isBlobIndex)
    {
        int no = _data.WrittenCount;
        _data.Write(name);
        int vo = _data.WrittenCount;
        _data.Write(value);
        _columns.Add((no, name.Length, vo, value.Length, isBlobIndex));
        HasBlobColumns |= isBlobIndex;
    }

    public ReadOnlySpan<byte> GetName(int i)
    {
        var c = _columns[i];
        return _data.WrittenSpan.Slice(c.NameOffset, c.NameLength);
    }

    public ReadOnlySpan<byte> GetValue(int i)
    {
        var c = _columns[i];
        return _data.WrittenSpan.Slice(c.ValueOffset, c.ValueLength);
    }

    public bool IsBlobIndex(int i) => _columns[i].Blob;

    /// <summary>
    /// Sorts the columns by name (WideColumnsHelper::SortColumns), for columns added in any order (merge results of
    /// MergeOperator.FullMergeV3). Insertion sort: column counts are small, and nothing is allocated.
    /// </summary>
    public void SortByName()
    {
        ReadOnlySpan<byte> data = _data.WrittenSpan;
        for (int i = 1; i < _columns.Count; i++)
        {
            var c = _columns[i];
            ReadOnlySpan<byte> name = data.Slice(c.NameOffset, c.NameLength);
            int j = i - 1;
            while (j >= 0 && data.Slice(_columns[j].NameOffset, _columns[j].NameLength).SequenceCompareTo(name) > 0)
            {
                _columns[j + 1] = _columns[j];
                j--;
            }
            _columns[j + 1] = c;
        }
    }

    public void Dispose() => _data.Dispose();
}
