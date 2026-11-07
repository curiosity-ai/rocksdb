using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Filters;

/// <summary>Creates a native (librocksdb) DB with one flushed SST for the filter/compression compat tests.</summary>
internal static class NativeSst
{
    public static byte[] Key(int i) => System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D8", System.Globalization.CultureInfo.InvariantCulture));

    public static byte[] Value(int i) => System.Text.Encoding.ASCII.GetBytes("value-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-the-quick-brown-fox-jumps-over-the-lazy-dog");

    /// <summary>
    /// Writes keys [0, count) with <see cref="Value"/>, flushes once and opens the SST with the managed reader.
    /// <paramref name="filterPolicy"/> is a native rocksdb_filterpolicy_t* (ownership passes to the table options) or zero.
    /// </summary>
    public static SstFile Build(TempDir dir, IntPtr filterPolicy, int formatVersion, RocksDbSharp.Compression compression, int count,
        RocksDb.Managed.Table.TableReaderOptions readerOptions)
    {
        var native = RocksDbSharp.Native.Instance;
        var bbto = new RocksDbSharp.BlockBasedTableOptions();
        bbto.SetFormatVersion(formatVersion);
        if (filterPolicy != IntPtr.Zero)
        {
            bbto.SetFilterPolicy(filterPolicy);
        }
        // Deterministic filter sizes (no malloc_usable_size rounding), full handles in the index block.
        native.rocksdb_block_based_options_set_optimize_filters_for_memory(bbto.Handle, 0);
        native.rocksdb_block_based_options_set_index_block_restart_interval(bbto.Handle, 1);
        var options = new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetBlockBasedTableFactory(bbto)
            .SetCompression(compression)
            .SetDisableAutoCompactions(1);
        using (var db = RocksDbSharp.RocksDb.Open(options, dir.Path))
        {
            for (int i = 0; i < count; i++)
            {
                db.Put(Key(i), Value(i));
            }
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        }
        return SstFile.OpenSingle(dir.Path, readerOptions);
    }
}
