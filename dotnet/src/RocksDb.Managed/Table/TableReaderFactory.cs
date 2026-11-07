using System.Text;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Table;

/// <summary>
/// Opens the <see cref="TableReader"/> for a table file. Port of TableFactory::NewTableReader for the built-in factories
/// (BlockBasedTableFactory, PlainTableFactory, CuckooTableFactory) and of AdaptiveTableFactory::NewTableReader
/// (table/adaptive/adaptive_table_factory.cc), which picks the reader by the footer's magic number.
/// </summary>
public static class TableReaderFactory
{
    /// <summary>
    /// Opens <paramref name="file"/> with the reader of <paramref name="options"/>' <see cref="TableReaderOptions.TableFactory"/>.
    /// The reader takes ownership of <paramref name="file"/>. A file of another format fails with Corruption (bad
    /// magic number), except with the adaptive factory.
    /// </summary>
    public static TableReader Open(IRandomAccessFile file, TableReaderOptions options)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(options);
        TableFactoryType factory = options.TableFactory;
        if (factory == TableFactoryType.AdaptiveTable)
        {
            Footer footer = TableMetaReader.ReadFooter(file, enforceTableMagicNumber: 0);
            factory = footer.TableMagicNumber switch
            {
                TableMagicNumbers.PlainTable => TableFactoryType.PlainTable,
                TableMagicNumbers.BlockBasedTable => TableFactoryType.BlockBasedTable,
                TableMagicNumbers.CuckooTable => TableFactoryType.CuckooTable,
                _ => throw RocksDbException.NotSupported("Unidentified table format"),
            };
        }
        return factory switch
        {
            TableFactoryType.PlainTable => Plain.PlainTableReader.Open(file, options, options.PlainTableOptions ?? new PlainTableOptions()),
            TableFactoryType.CuckooTable => Cuckoo.CuckooTableReader.Open(file, options),
            _ => BlockBasedTableReader.Open(file, options),
        };
    }

    /// <summary>Async <see cref="Open"/> (block-based tables read their tail with async I/O; other formats open synchronously).</summary>
    public static async ValueTask<TableReader> OpenAsync(IRandomAccessFile file, TableReaderOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.TableFactory == TableFactoryType.BlockBasedTable)
            return await BlockBasedTableReader.OpenAsync(file, options, cancellationToken).ConfigureAwait(false);
        return Open(file, options);
    }
}

/// <summary>
/// Footer, metaindex and properties reading for the table formats without block trailers (plain, cuckoo). Port of
/// ReadFooterFromFile (table/format.cc) and ReadTableProperties / ReadMetaBlock / FindMetaBlockInFile
/// (table/meta_blocks.cc) for those formats: meta blocks are stored raw (no compression type or checksum).
/// </summary>
internal static class TableMetaReader
{
    /// <summary>Reads and decodes the footer; <paramref name="enforceTableMagicNumber"/> 0 accepts any known format.</summary>
    public static Footer ReadFooter(IRandomAccessFile file, ulong enforceTableMagicNumber)
    {
        long fileSize = file.Length;
        if (fileSize < Footer.MinEncodedLength) ThrowHelper.Corruption("file is too short (" + fileSize + " bytes) to be an sstable");
        int n = (int)Math.Min(fileSize, Footer.EncodedLength);
        Span<byte> buf = stackalloc byte[Footer.EncodedLength];
        if (BlockFetcher.ReadFully(file, fileSize - n, buf[..n]) != n) ThrowHelper.Corruption("truncated footer read");
        return Footer.Decode(buf[..n], (ulong)(fileSize - n), enforceTableMagicNumber);
    }

    /// <summary>The raw contents of the block at <paramref name="handle"/> inside <paramref name="data"/> (the whole file).</summary>
    public static ReadOnlySpan<byte> Slice(ReadOnlySpan<byte> data, BlockHandle handle)
    {
        if (handle.Offset > (ulong)data.Length || handle.Size > (ulong)data.Length - handle.Offset)
            ThrowHelper.Corruption("block handle " + handle + " beyond the end of the file (" + data.Length + " bytes)");
        return data.Slice((int)handle.Offset, (int)handle.Size);
    }

    /// <summary>The metaindex (name to handle) of a table whose whole contents are <paramref name="data"/>.</summary>
    public static Dictionary<string, BlockHandle> ReadMetaIndex(ReadOnlySpan<byte> data, in Footer footer)
    {
        var metaBlocks = new Dictionary<string, BlockHandle>(StringComparer.Ordinal);
        Block metaindex = Block.CreateCopy(Slice(data, footer.MetaindexHandle));
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
        return metaBlocks;
    }

    /// <summary>ReadTableProperties: the "rocksdb.properties" block (empty properties when absent).</summary>
    public static TableProperties ReadProperties(ReadOnlySpan<byte> data, Dictionary<string, BlockHandle> metaBlocks)
    {
        if (!metaBlocks.TryGetValue(BlockBasedTableConstants.PropertiesBlockName, out BlockHandle h)) return new TableProperties();
        Block pb = Block.CreateCopy(Slice(data, h));
        try
        {
            return TableProperties.Parse(pb, h.Offset);
        }
        finally
        {
            pb.Release();
        }
    }

    /// <summary>Reads the whole file into one array (the managed equivalent of upstream's mmap'ed file data).</summary>
    public static byte[] ReadAll(IRandomAccessFile file)
    {
        long size = file.Length;
        if (size > Array.MaxLength) ThrowHelper.NotSupported("File size " + size + " is too large to be read into memory");
        byte[] data = GC.AllocateUninitializedArray<byte>((int)size);
        if (BlockFetcher.ReadFully(file, 0, data) != data.Length) ThrowHelper.Corruption("truncated read of table file");
        return data;
    }
}
