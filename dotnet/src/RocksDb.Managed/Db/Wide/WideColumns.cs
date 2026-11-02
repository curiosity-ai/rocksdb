using System.Buffers;
using System.Collections;
using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// One column of a wide-column entity (rocksdb::WideColumn, include/rocksdb/wide_columns.h). The column with the empty
/// name is the "default column": a plain Get of an entity returns its value, and a plain Put is read by GetEntity as an
/// entity with only the default column.
/// </summary>
public readonly struct WideColumn
{
    public WideColumn(ReadOnlyMemory<byte> name, ReadOnlyMemory<byte> value)
    {
        Name = name;
        Value = value;
    }

    public ReadOnlyMemory<byte> Name { get; }
    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>The name of the default column (kDefaultWideColumnName): empty.</summary>
    public static ReadOnlyMemory<byte> DefaultColumnName => ReadOnlyMemory<byte>.Empty;

    /// <summary>A default-column entry with <paramref name="value"/>.</summary>
    public static WideColumn Default(ReadOnlyMemory<byte> value) => new(ReadOnlyMemory<byte>.Empty, value);

    public override string ToString() => System.Text.Encoding.UTF8.GetString(Name.Span) + "=" + Convert.ToHexString(Value.Span);
}

/// <summary>
/// The columns of a wide-column entity, sorted by name (rocksdb::PinnableWideColumns). Holds its own copy of the
/// serialized entity, so it stays valid after the read that filled it. Reusable: pass the same instance to
/// <see cref="ColumnFamily.TryGetEntity(ReadOptions?, ReadOnlySpan{byte}, WideColumns)"/> repeatedly to read entities
/// without allocating once its buffer is large enough. Not thread-safe.
/// </summary>
public sealed class WideColumns : IReadOnlyList<WideColumn>
{
    private byte[] _buffer = [];
    private int _length;
    private readonly List<WideColumnSlot> _slots = new();

    public WideColumns() { }

    /// <summary>Creates an entity from columns (in any order; names must be unique).</summary>
    public WideColumns(IEnumerable<WideColumn> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        WideColumn[] arr = [.. columns];
        WideColumnSerialization.SortColumns(arr);
        var w = new PooledBufferWriter(64);
        try
        {
            WideColumnSerialization.Serialize(arr, w);
            SetSerialized(w.WrittenSpan);
        }
        finally
        {
            w.Dispose();
        }
    }

    /// <summary>Replaces the contents with the entity serialized in <paramref name="entity"/> (validated).</summary>
    public void SetSerialized(ReadOnlySpan<byte> entity)
    {
        if (_buffer.Length < entity.Length) _buffer = new byte[Math.Max(entity.Length, _buffer.Length * 2)];
        entity.CopyTo(_buffer);
        _length = entity.Length;
        WideColumnSerialization.Deserialize(_buffer.AsSpan(0, _length), _slots);
    }

    /// <summary>
    /// Like <see cref="SetSerialized"/> but blob-backed (version 2 kTypeBlobIndex) columns are kept with their serialized
    /// blob index as value (WideColumnSerialization::Deserialize with blob_columns). Returns whether any column is
    /// blob-backed.
    /// </summary>
    internal bool SetSerializedWithBlobColumns(ReadOnlySpan<byte> entity)
    {
        if (_buffer.Length < entity.Length) _buffer = new byte[Math.Max(entity.Length, _buffer.Length * 2)];
        entity.CopyTo(_buffer);
        _length = entity.Length;
        return WideColumnSerialization.DeserializeWithBlobColumns(_buffer.AsSpan(0, _length), _slots);
    }

    /// <summary>True if column <paramref name="index"/> holds a serialized blob index (see <see cref="SetSerializedWithBlobColumns"/>).</summary>
    internal bool IsBlobIndex(int index) => _slots[index].IsBlobIndex;

    internal void Clear()
    {
        _length = 0;
        _slots.Clear();
    }

    /// <summary>The serialized entity (wide-column serialization format version 1 or 2).</summary>
    public ReadOnlySpan<byte> Serialized => _buffer.AsSpan(0, _length);

    public int Count => _slots.Count;

    public WideColumn this[int index]
    {
        get
        {
            WideColumnSlot s = _slots[index];
            return new WideColumn(_buffer.AsMemory(s.NameOffset, s.NameLength), _buffer.AsMemory(s.ValueOffset, s.ValueLength));
        }
    }

    /// <summary>The name of column <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> GetName(int index)
    {
        WideColumnSlot s = _slots[index];
        return _buffer.AsSpan(s.NameOffset, s.NameLength);
    }

    /// <summary>The value of column <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> GetValue(int index)
    {
        WideColumnSlot s = _slots[index];
        return _buffer.AsSpan(s.ValueOffset, s.ValueLength);
    }

    /// <summary>Finds the column named <paramref name="name"/> (binary search).</summary>
    public bool TryGetValue(ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        int lo = 0, hi = _slots.Count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int c = GetName(mid).SequenceCompareTo(name);
            if (c == 0)
            {
                value = GetValue(mid);
                return true;
            }
            if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        value = default;
        return false;
    }

    /// <summary>Whether the entity has a default (empty-named) column.</summary>
    public bool HasDefaultColumn => _slots.Count > 0 && _slots[0].NameLength == 0;

    /// <summary>The value of the default column, or empty when there is none.</summary>
    public ReadOnlySpan<byte> DefaultColumnValue => HasDefaultColumn ? GetValue(0) : default;

    public IEnumerator<WideColumn> GetEnumerator()
    {
        for (int i = 0; i < _slots.Count; i++) yield return this[i];
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
