using System.Buffers;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>A user key range [<see cref="Start"/>, <see cref="Limit"/>) (rocksdb::Range).</summary>
public readonly record struct KeyRange(ReadOnlyMemory<byte> Start, ReadOnlyMemory<byte> Limit);

/// <summary>Options of <see cref="ColumnFamily.GetApproximateSizes(SizeApproximationOptions, IReadOnlyList{KeyRange})"/> (rocksdb::SizeApproximationOptions).</summary>
public sealed class SizeApproximationOptions
{
    /// <summary>Include the memtables' estimated size (default false).</summary>
    public bool IncludeMemtables { get; set; }
    /// <summary>Include the table files' estimated size (default true). At least one of the two must be set.</summary>
    public bool IncludeFiles { get; set; } = true;
    /// <summary>
    /// When &gt; 0, files only partially covered by a range are estimated as half their size if the error stays below
    /// this fraction of the fully covered files' size (avoids index lookups). Default -1 (always precise).
    /// </summary>
    public double FilesSizeErrorMargin { get; set; } = -1.0;
}

public sealed partial class ColumnFamily
{
    // ---- KeyMayExist ------------------------------------------------------------------------------------

    /// <summary>
    /// Returns false only if <paramref name="key"/> definitely does not exist (DB::KeyMayExist): searches the memtables
    /// and the filters of the table files, without reading data blocks. Cheaper than a Get, but may return true for keys
    /// that don't exist.
    /// </summary>
    public bool KeyMayExist(ReadOnlySpan<byte> key) => KeyMayExist(null, key, null, out _);

    /// <summary>
    /// <see cref="KeyMayExist(ReadOnlySpan{byte})"/>; when the value is found without I/O (in a memtable) it is appended
    /// to <paramref name="value"/> (if not null) and <paramref name="valueFound"/> is true.
    /// </summary>
    public bool KeyMayExist(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte>? value, out bool valueFound)
    {
        _db.ThrowIfDisposed();
        return _db.KeyMayExist(_cfd, options, key, value, out valueFound);
    }

    // ---- size estimates ----------------------------------------------------------------------------------

    /// <summary>Approximate on-disk size of the table data in each range (DB::GetApproximateSizes, files only).</summary>
    public ulong[] GetApproximateSizes(IReadOnlyList<KeyRange> ranges) => GetApproximateSizes(s_defaultSizeOptions, ranges);

    private static readonly SizeApproximationOptions s_defaultSizeOptions = new();

    /// <summary>
    /// Approximate size of the data in each range (DB::GetApproximateSizes): table files (estimated from the index
    /// blocks) and/or memtables, per <paramref name="options"/>. Throws InvalidArgument when neither is included.
    /// </summary>
    public ulong[] GetApproximateSizes(SizeApproximationOptions options, IReadOnlyList<KeyRange> ranges)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(ranges);
        _db.ThrowIfDisposed();
        var list = new (byte[], byte[])[ranges.Count];
        for (int i = 0; i < list.Length; i++) list[i] = (ranges[i].Start.ToArray(), ranges[i].Limit.ToArray());
        return _db.GetApproximateSizes(_cfd, list, options.IncludeMemtables, options.IncludeFiles, options.FilesSizeErrorMargin);
    }

    /// <summary>Approximate size of the table data in [<paramref name="start"/>, <paramref name="limit"/>).</summary>
    public ulong GetApproximateSize(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit) =>
        _db.GetApproximateSizes(_cfd, [(start.ToArray(), limit.ToArray())], includeMemtables: false, includeFiles: true, filesSizeErrorMargin: -1)[0];

    /// <summary>
    /// Approximate number of entries and bytes in the memtables (mutable + immutable) for [<paramref name="start"/>,
    /// <paramref name="limit"/>) (DB::GetApproximateMemTableStats).
    /// </summary>
    public (ulong Count, ulong Size) GetApproximateMemTableStats(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit)
    {
        _db.ThrowIfDisposed();
        return _db.GetApproximateMemTableStats(_cfd, start.ToArray(), limit.ToArray());
    }

    // ---- properties --------------------------------------------------------------------------------------

    /// <summary>
    /// The integer value of a numeric property (DB::GetIntProperty), e.g. "rocksdb.estimate-num-keys",
    /// "rocksdb.cur-size-all-mem-tables", "rocksdb.estimate-live-data-size", "rocksdb.block-cache-usage". Returns false
    /// for unknown or non-numeric properties.
    /// </summary>
    public bool TryGetIntProperty(string property, out ulong value)
    {
        ArgumentNullException.ThrowIfNull(property);
        _db.ThrowIfDisposed();
        return _db.TryGetIntProperty(_cfd, property, out value);
    }

    /// <summary>The integer value of a numeric property, or null (see <see cref="TryGetIntProperty"/>).</summary>
    public ulong? GetIntProperty(string property) => TryGetIntProperty(property, out ulong v) ? v : null;

    /// <summary>
    /// A map-valued property (DB::GetMapProperty): "rocksdb.cfstats" (compaction.&lt;L&gt;.&lt;stat&gt;),
    /// "rocksdb.dbstats", "rocksdb.aggregated-table-properties[-at-level&lt;N&gt;]", "rocksdb.block-cache-entry-stats".
    /// Null for unknown properties.
    /// </summary>
    public Dictionary<string, string>? GetMapProperty(string property)
    {
        ArgumentNullException.ThrowIfNull(property);
        _db.ThrowIfDisposed();
        return _db.GetMapProperty(_cfd, property);
    }

    /// <summary>The table properties of every live table file of this column family, keyed by file path (DB::GetPropertiesOfAllTables).</summary>
    public Dictionary<string, TableProperties> GetPropertiesOfAllTables()
    {
        _db.ThrowIfDisposed();
        return _db.GetPropertiesOfAllTables(_cfd);
    }
}

public sealed partial class RocksDatabase
{
    /// <summary>Writes the periodic stats dump to the info log now (DBImpl::DumpStats; normally every stats_dump_period_sec).</summary>
    internal void DumpStats() => _db.DumpStats();

    /// <summary>The statistics object of the DB (<see cref="DbOptions.Statistics"/>), or null.</summary>
    public Statistics? Statistics => _db.Options.Statistics;

    /// <summary><see cref="ColumnFamily.KeyMayExist(ReadOnlySpan{byte})"/> on the default column family.</summary>
    public bool KeyMayExist(ReadOnlySpan<byte> key) => _default.KeyMayExist(key);

    public bool KeyMayExist(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte>? value, out bool valueFound) =>
        _default.KeyMayExist(options, key, value, out valueFound);

    public ulong[] GetApproximateSizes(IReadOnlyList<KeyRange> ranges) => _default.GetApproximateSizes(ranges);

    public ulong[] GetApproximateSizes(SizeApproximationOptions options, IReadOnlyList<KeyRange> ranges) => _default.GetApproximateSizes(options, ranges);

    public (ulong Count, ulong Size) GetApproximateMemTableStats(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit) =>
        _default.GetApproximateMemTableStats(start, limit);

    public bool TryGetIntProperty(string property, out ulong value) => _default.TryGetIntProperty(property, out value);

    public ulong? GetIntProperty(string property) => _default.GetIntProperty(property);

    public Dictionary<string, string>? GetMapProperty(string property) => _default.GetMapProperty(property);

    public Dictionary<string, TableProperties> GetPropertiesOfAllTables() => _default.GetPropertiesOfAllTables();

    /// <summary>
    /// Sum of a numeric property over all column families (DB::GetAggregatedIntProperty), e.g.
    /// "rocksdb.cur-size-all-mem-tables". Returns false if a column family does not support it.
    /// </summary>
    public bool TryGetAggregatedIntProperty(string property, out ulong value)
    {
        value = 0;
        foreach (ColumnFamily cf in ColumnFamilies)
        {
            if (!cf.TryGetIntProperty(property, out ulong v)) return false;
            value += v;
        }
        return true;
    }
}
