using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>
/// How a <see cref="UserDefinedIndexFactory"/>'s custom index interacts with the standard index of block-based tables
/// (BlockBasedTableOptions::IndexMode, include/rocksdb/table.h). EXPERIMENTAL upstream. Numeric values as upstream.
/// </summary>
public enum BlockBasedIndexMode
{
    /// <summary>kStandardOnly: only the standard index is used; a configured factory is ignored.</summary>
    StandardOnly = 0,
    /// <summary>
    /// kStandardDefault (default): both indexes are built; reads use the standard index unless
    /// <see cref="ReadOptions.ReadIndex"/> asks for the custom one. Files without the custom index fall back to the
    /// standard index.
    /// </summary>
    StandardDefault = 1,
    /// <summary>kCustomDefault: both indexes are built; all reads (including compaction) use the custom index.</summary>
    CustomDefault = 2,
    /// <summary>
    /// kCustomOnly: only the custom index is built (the standard index is an empty stub) and the footer marks the file as
    /// requiring it (kFooterFeatureRequireUserDefinedIndex). Requires format_version &gt;= 6.
    /// </summary>
    CustomOnly = 3,
    /// <summary>kStandardRequired: like <see cref="StandardDefault"/>, but opening a file without the custom index fails.</summary>
    StandardRequired = 4,
}

/// <summary>Index selection of a read (ReadOptions::ReadIndex, include/rocksdb/options.h).</summary>
public enum ReadIndex : byte
{
    /// <summary>kDefault: the custom index when the index mode makes it primary (or the standard index is a stub).</summary>
    Default = 0,
    /// <summary>kBuiltin: the standard index (fails for files written with <see cref="BlockBasedIndexMode.CustomOnly"/>).</summary>
    Builtin = 1,
    /// <summary>kPreferCustom: the custom index when the file has one, else the standard index.</summary>
    PreferCustom = 2,
}

/// <summary>Kind of an entry passed to <see cref="UserDefinedIndexBuilder.OnKeyAdded"/> (IndexFactoryBuilder::ValueType).</summary>
public enum UserDefinedIndexValueType : byte
{
    Value = 0,
    Delete = 1,
    Merge = 2,
    Other = 3,
}

/// <summary>Options passed to the factory (IndexFactoryOptions).</summary>
public sealed class UserDefinedIndexOptions
{
    /// <summary>The user comparator of the table.</summary>
    public Comparator Comparator { get; init; } = Comparator.Bytewise;
}

/// <summary>
/// Context of one index entry (IndexFactoryBuilder::IndexEntryContext): the packed (sequence, type) tags of the last key
/// of the block and of the first key of the next block (0 when there is none), and whether delta encoding is skipped.
/// </summary>
public readonly record struct UserDefinedIndexEntryContext(ulong LastKeyTag, ulong FirstKeyTag, bool SkipDeltaEncoding);

/// <summary>
/// Builds a custom index over the data blocks of one table. Port of IndexFactoryBuilder (include/rocksdb/index_factory.h):
/// the table builder calls <see cref="OnKeyAdded"/> for every point entry and <see cref="AddIndexEntry"/> once per data
/// block (with user keys), then <see cref="Finish"/>; the contents are stored uncompressed in the meta block
/// <c>"rocksdb.user_defined_index." + factory name</c>.
/// </summary>
public abstract class UserDefinedIndexBuilder : IDisposable
{
    /// <summary>
    /// Adds the entry of a finished data block: <paramref name="lastKeyInCurrentBlock"/> and (when
    /// <paramref name="hasNextBlock"/>) <paramref name="firstKeyInNextBlock"/> are user keys. Returns the separator the
    /// index stores for the block (informational).
    /// </summary>
    public abstract ReadOnlySpan<byte> AddIndexEntry(ReadOnlySpan<byte> lastKeyInCurrentBlock, ReadOnlySpan<byte> firstKeyInNextBlock, bool hasNextBlock,
        BlockHandle blockHandle, in UserDefinedIndexEntryContext context);

    /// <summary>Called for every point entry added to the table, in order.</summary>
    public virtual void OnKeyAdded(ReadOnlySpan<byte> userKey, UserDefinedIndexValueType type, ReadOnlySpan<byte> value) { }

    /// <summary>Finishes the index and returns its serialized contents (valid until the builder is disposed).</summary>
    public abstract ReadOnlySpan<byte> Finish();

    /// <summary>Estimated size of the finished index.</summary>
    public abstract ulong EstimatedSize { get; }

    public virtual void Dispose() => GC.SuppressFinalize(this);
}

/// <summary>
/// Iterator over a custom index (IndexFactoryIterator): positions on data block entries in key order. A positioning
/// method returns false when there is no entry (end of index or out of the iterator's bounds,
/// IterBoundCheck::kOutOfBound / kUnknown upstream); <see cref="Key"/> is then undefined.
/// </summary>
public abstract class UserDefinedIndexIterator : IDisposable
{
    /// <summary>
    /// Positions at the first block that may contain user keys &gt;= <paramref name="targetUserKey"/>;
    /// <paramref name="targetTag"/> is the packed (sequence, type) of the internal lookup key (SeekContext::target_tag).
    /// </summary>
    public abstract bool Seek(ReadOnlySpan<byte> targetUserKey, ulong targetTag);

    /// <summary>Moves to the next block.</summary>
    public abstract bool Next();

    /// <summary>Positions at the first block (default: <see cref="Seek"/> to the empty key).</summary>
    public virtual bool SeekToFirst() => Seek(default, 0);

    /// <summary>Positions at the last block (NotSupported unless overridden).</summary>
    public virtual bool SeekToLast() => throw RocksDbException.NotSupported("SeekToLast not supported by this index");

    /// <summary>Moves to the previous block (NotSupported unless overridden).</summary>
    public virtual bool Prev() => throw RocksDbException.NotSupported("Prev not supported by this index");

    /// <summary>The separator (user key) of the current block: every user key in the block is &lt;= it.</summary>
    public abstract ReadOnlySpan<byte> Key { get; }

    /// <summary>The handle of the current data block.</summary>
    public abstract BlockHandle Value { get; }

    public virtual void Dispose() => GC.SuppressFinalize(this);
}

/// <summary>Reader of a custom index block (IndexFactoryReader). Thread-safe: iterators are created concurrently.</summary>
public abstract class UserDefinedIndexReader : IDisposable
{
    /// <summary>A new iterator for a read with <paramref name="readOptions"/> (null: defaults).</summary>
    public abstract UserDefinedIndexIterator NewIterator(ReadOptions? readOptions);

    /// <summary>Approximate memory used by the reader.</summary>
    public abstract ulong ApproximateMemoryUsage { get; }

    public virtual void Dispose() => GC.SuppressFinalize(this);
}

/// <summary>
/// A custom index for block-based tables (IndexFactory / UserDefinedIndexFactory, include/rocksdb/index_factory.h and
/// include/rocksdb/user_defined_index.h). Configure with <see cref="BlockBasedTableOptions.UserDefinedIndexFactory"/>
/// and <see cref="BlockBasedTableOptions.IndexMode"/>. The <see cref="Name"/> identifies the index block in the file
/// and must match the writer's factory on read.
/// </summary>
public abstract class UserDefinedIndexFactory
{
    /// <summary>kIndexFactoryMetaPrefix: the meta block name is this prefix + <see cref="Name"/>.</summary>
    public const string MetaBlockPrefix = "rocksdb.user_defined_index.";

    /// <summary>The factory's name (Customizable::Name).</summary>
    public abstract string Name { get; }

    /// <summary>A builder for one table.</summary>
    public abstract UserDefinedIndexBuilder NewBuilder(UserDefinedIndexOptions options);

    /// <summary>A reader over <paramref name="indexContents"/> (the index block of one table, kept alive by the table reader).</summary>
    public abstract UserDefinedIndexReader NewReader(UserDefinedIndexOptions options, ReadOnlyMemory<byte> indexContents);
}
