using RocksDb.Managed.Env;

namespace RocksDb.Managed.Table;

/// <summary>
/// A builder of one table file of any SST format. Port of the <c>TableBuilder</c> interface (table/table_builder.h):
/// <see cref="BlockBasedTableBuilder"/>, <see cref="Plain.PlainTableBuilder"/> and <see cref="Cuckoo.CuckooTableBuilder"/>.
/// Keys are added in internal key order. Errors (including entry types a format does not support) throw
/// <see cref="RocksDbException"/> from <see cref="Add"/> / <see cref="Finish"/>, where upstream records a status.
/// </summary>
public abstract class TableBuilder : IDisposable
{
    /// <summary>
    /// The builder for <paramref name="tableBuilderOptions"/>'s <see cref="TableBuilderOptions.TableFactory"/>
    /// (TableFactory::NewTableBuilder): block-based tables use <paramref name="format"/>, plain and cuckoo tables
    /// their options in <paramref name="tableBuilderOptions"/>.
    /// </summary>
    public static TableBuilder Create(IWritableFile file, TableFormatOptions format, TableBuilderOptions tableBuilderOptions)
    {
        ArgumentNullException.ThrowIfNull(tableBuilderOptions);
        return tableBuilderOptions.TableFactory switch
        {
            TableFactoryType.PlainTable => new Plain.PlainTableBuilder(file, tableBuilderOptions, tableBuilderOptions.PlainTableOptions ?? new PlainTableOptions()),
            TableFactoryType.CuckooTable => new Cuckoo.CuckooTableBuilder(file, tableBuilderOptions, tableBuilderOptions.CuckooTableOptions ?? new CuckooTableOptions()),
            TableFactoryType.BlockBasedTable => new BlockBasedTableBuilder(file, format, tableBuilderOptions),
            _ => throw RocksDbException.InvalidArgument("table factory " + tableBuilderOptions.TableFactory + " cannot write tables"),
        };
    }

    /// <summary>Adds an entry (internal key, value).</summary>
    public abstract void Add(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value);

    /// <summary>Adds an entry; formats that stage output may write it asynchronously.</summary>
    public virtual ValueTask AddAsync(ReadOnlyMemory<byte> internalKey, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        Add(internalKey.Span, value.Span);
        return ValueTask.CompletedTask;
    }

    /// <summary>Finishes the table and flushes the file (not synced).</summary>
    public abstract void Finish();

    /// <summary>Async <see cref="Finish"/>.</summary>
    public virtual ValueTask FinishAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Finish();
        return ValueTask.CompletedTask;
    }

    /// <summary>Stops building; the partially written file should be deleted by the caller.</summary>
    public abstract void Abandon();

    /// <summary>Number of entries added so far.</summary>
    public abstract ulong NumEntries { get; }

    /// <summary>No entry was added.</summary>
    public virtual bool IsEmpty => NumEntries == 0;

    /// <summary>Size of the file generated so far (final size after <see cref="Finish"/>).</summary>
    public abstract ulong FileSize { get; }

    /// <summary>Estimated file size including output not yet written (compaction output cutting).</summary>
    public virtual ulong EstimatedFileSize => FileSize;

    /// <summary>Estimated size of the meta blocks and footer still to be written (TableBuilder::EstimatedTailSize, default 0).</summary>
    public virtual ulong EstimatedTailSize() => 0;

    /// <summary>Properties of the table (complete after <see cref="Finish"/>).</summary>
    public abstract TableProperties Properties { get; }

    /// <summary>A table properties collector asked for the file to be compacted (TableBuilder::NeedCompact).</summary>
    public virtual bool NeedCompact => false;

    /// <summary>Smallest user-defined timestamp added ("rocksdb.timestamp_min"); empty when not collected.</summary>
    public virtual byte[] TimestampMin => [];

    /// <summary>Largest user-defined timestamp added ("rocksdb.timestamp_max"); empty when not collected.</summary>
    public virtual byte[] TimestampMax => [];

    /// <summary>Releases the builder's buffers (the file is not closed).</summary>
    public abstract void Dispose();
}
