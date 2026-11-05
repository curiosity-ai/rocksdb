using System.Collections;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>
/// One internal entry of a key (a version: value, merge operand, tombstone, ...) as returned by
/// <see cref="ColumnFamily.GetAllKeyVersions(byte[], byte[], int)"/>. Port of rocksdb::KeyVersion
/// (include/rocksdb/utilities/debug.h). <see cref="UserKey"/> and <see cref="Value"/> point into the owning
/// <see cref="KeyVersionCollection"/>'s pooled buffer: they are valid until that collection is reused or disposed.
/// </summary>
public readonly struct KeyVersion
{
    internal KeyVersion(ReadOnlyMemory<byte> userKey, ReadOnlyMemory<byte> value, ulong sequence, EntryType type)
    {
        UserKey = userKey;
        Value = value;
        Sequence = sequence;
        Type = type;
    }

    /// <summary>The user key (including the user-defined timestamp, when the column family has one).</summary>
    public ReadOnlyMemory<byte> UserKey { get; }

    /// <summary>The raw value of the entry (a blob index for blob values, the serialized columns of an entity).</summary>
    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>The entry's sequence number.</summary>
    public ulong Sequence { get; }

    /// <summary>The entry's type (upstream's <c>int type</c>, a <c>ValueType</c>).</summary>
    public EntryType Type { get; }

    /// <summary>The upstream name of <see cref="Type"/> (KeyVersion::GetTypeName), e.g. "TypeValue"; "Invalid" for unknown values.</summary>
    public string TypeName => GetTypeName(Type);

    /// <summary>
    /// KeyVersion::GetTypeName for any type: the name in utilities/debug.cc's value_type_string_map ("TypeDeletion",
    /// "TypeValue", ...), or "Invalid".
    /// </summary>
    public static string GetTypeName(EntryType type) => (byte)type < s_typeNames.Length ? s_typeNames[(byte)type] : "Invalid";

    // value_type_string_map (utilities/debug.cc), indexed by ValueType.
    private static readonly string[] s_typeNames =
    [
        "TypeDeletion",                        // 0x0
        "TypeValue",                           // 0x1
        "TypeMerge",                           // 0x2
        "TypeLogData",                         // 0x3
        "TypeColumnFamilyDeletion",            // 0x4
        "TypeColumnFamilyValue",               // 0x5
        "TypeColumnFamilyMerge",               // 0x6
        "TypeSingleDeletion",                  // 0x7
        "TypeColumnFamilySingleDeletion",      // 0x8
        "TypeBeginPrepareXID",                 // 0x9
        "TypeEndPrepareXID",                   // 0xA
        "TypeCommitXID",                       // 0xB
        "TypeRollbackXID",                     // 0xC
        "TypeNoop",                            // 0xD
        "TypeColumnFamilyRangeDeletion",       // 0xE
        "TypeRangeDeletion",                   // 0xF
        "TypeColumnFamilyBlobIndex",           // 0x10
        "TypeBlobIndex",                       // 0x11
        "TypeBeginPersistedPrepareXID",        // 0x12
        "TypeBeginUnprepareXID",               // 0x13
        "TypeDeletionWithTimestamp",           // 0x14
        "TypeCommitXIDAndTimestamp",           // 0x15
        "TypeWideColumnEntity",                // 0x16
        "TypeColumnFamilyWideColumnEntity",    // 0x17
        "TypeValuePreferredSeqno",             // 0x18
        "TypeColumnFamilyValuePreferredSeqno", // 0x19
    ];

    /// <inheritdoc/>
    public override string ToString() => $"{Convert.ToHexString(UserKey.Span)} @{Sequence} : {TypeName}";
}

/// <summary>
/// The result of <see cref="ColumnFamily.GetAllKeyVersions(byte[], byte[], int, KeyVersionCollection)"/>: the
/// internal entries in internal-key order (user key ascending, newest version first), stored in one pooled buffer.
/// Reusable: pass the same instance to several calls to avoid allocations (each call clears it, which invalidates the
/// <see cref="KeyVersion"/>s read from it before). Dispose to return the buffer to the pool.
/// </summary>
public sealed class KeyVersionCollection : IReadOnlyList<KeyVersion>, IDisposable
{
    private readonly PooledBufferWriter _data = new(1024);
    private Entry[] _entries = new Entry[16];
    private int _count;

    // Offsets of one entry in _data: user key at [Start, Start+KeyLength), value right after it.
    private readonly record struct Entry(int Start, int KeyLength, int ValueLength, ulong Sequence, EntryType Type);

    /// <summary>The number of entries.</summary>
    public int Count => _count;

    /// <summary>Entry <paramref name="index"/> (0 = smallest internal key).</summary>
    public KeyVersion this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count, nameof(index));
            Entry e = _entries[index];
            ReadOnlyMemory<byte> data = _data.WrittenMemory;
            return new KeyVersion(data.Slice(e.Start, e.KeyLength), data.Slice(e.Start + e.KeyLength, e.ValueLength), e.Sequence, e.Type);
        }
    }

    /// <summary>Removes all entries (the buffer is kept).</summary>
    public void Clear()
    {
        _data.Clear();
        _count = 0;
    }

    internal void Add(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value, ulong sequence, EntryType type)
    {
        int start = _data.WrittenCount;
        _data.Write(userKey);
        _data.Write(value);
        if (_count == _entries.Length) Array.Resize(ref _entries, _entries.Length * 2);
        _entries[_count++] = new Entry(start, userKey.Length, value.Length, sequence, type);
    }

    /// <summary>A struct enumerator (no allocation in <c>foreach</c>).</summary>
    public Enumerator GetEnumerator() => new(this);

    IEnumerator<KeyVersion> IEnumerable<KeyVersion>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Returns the pooled buffer.</summary>
    public void Dispose()
    {
        _count = 0;
        _data.Dispose();
    }

    /// <summary>Enumerates a <see cref="KeyVersionCollection"/>.</summary>
    public struct Enumerator : IEnumerator<KeyVersion>
    {
        private readonly KeyVersionCollection _owner;
        private int _index;

        internal Enumerator(KeyVersionCollection owner)
        {
            _owner = owner;
            _index = -1;
        }

        /// <inheritdoc/>
        public readonly KeyVersion Current => _owner[_index];

        readonly object IEnumerator.Current => Current;

        /// <inheritdoc/>
        public bool MoveNext() => ++_index < _owner._count;

        /// <inheritdoc/>
        public void Reset() => _index = -1;

        /// <inheritdoc/>
        public readonly void Dispose() { }
    }
}
