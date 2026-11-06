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
    /// Allocates the result array; see
    /// <see cref="GetApproximateSizes(SizeApproximationOptions, ReadOnlySpan{KeyRange}, Span{ulong})"/>.
    /// </summary>
    public ulong[] GetApproximateSizes(SizeApproximationOptions options, IReadOnlyList<KeyRange> ranges)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(ranges);
        _db.ThrowIfDisposed();
        var sizes = new ulong[ranges.Count];
        ReadOnlySpan<KeyRange> span = ranges switch
        {
            KeyRange[] a => a,
            List<KeyRange> l => System.Runtime.InteropServices.CollectionsMarshal.AsSpan(l),
            _ => CopyRanges(ranges),
        };
        _db.GetApproximateSizes(_cfd, span, sizes, options.IncludeMemtables, options.IncludeFiles, options.FilesSizeErrorMargin);
        return sizes;

        static KeyRange[] CopyRanges(IReadOnlyList<KeyRange> ranges)
        {
            var a = new KeyRange[ranges.Count];
            for (int i = 0; i < a.Length; i++) a[i] = ranges[i];
            return a;
        }
    }

    /// <summary>
    /// <see cref="GetApproximateSizes(IReadOnlyList{KeyRange})"/> into caller memory (DB::GetApproximateSizes with a
    /// <c>uint64_t* sizes</c> output): table files only. Does not allocate.
    /// </summary>
    /// <param name="ranges">The user-key ranges [start, limit).</param>
    /// <param name="sizes">Receives one size per range; at least <c>ranges.Length</c> long.</param>
    public void GetApproximateSizes(ReadOnlySpan<KeyRange> ranges, Span<ulong> sizes) => GetApproximateSizes(s_defaultSizeOptions, ranges, sizes);

    /// <summary>
    /// <see cref="GetApproximateSizes(SizeApproximationOptions, IReadOnlyList{KeyRange})"/> into caller memory
    /// (DB::GetApproximateSizes(options, column_family, ranges, n, sizes)). Does not allocate.
    /// </summary>
    /// <param name="options">What to include (files and / or memtables).</param>
    /// <param name="ranges">The user-key ranges [start, limit).</param>
    /// <param name="sizes">Receives one size per range; at least <c>ranges.Length</c> long.</param>
    /// <exception cref="ArgumentException"><paramref name="sizes"/> is shorter than <paramref name="ranges"/>.</exception>
    public void GetApproximateSizes(SizeApproximationOptions options, ReadOnlySpan<KeyRange> ranges, Span<ulong> sizes)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (sizes.Length < ranges.Length) throw new ArgumentException("The output span must have at least one element per range.", nameof(sizes));
        _db.ThrowIfDisposed();
        _db.GetApproximateSizes(_cfd, ranges, sizes, options.IncludeMemtables, options.IncludeFiles, options.FilesSizeErrorMargin);
    }

    /// <summary>Approximate size of the table data in [<paramref name="start"/>, <paramref name="limit"/>). Does not allocate.</summary>
    public ulong GetApproximateSize(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit)
    {
        _db.ThrowIfDisposed();
        return _db.GetApproximateSize(_cfd, start, limit, includeMemtables: false, includeFiles: true, filesSizeErrorMargin: -1);
    }

    /// <summary>
    /// Approximate number of entries and bytes in the memtables (mutable + immutable) for [<paramref name="start"/>,
    /// <paramref name="limit"/>) (DB::GetApproximateMemTableStats).
    /// </summary>
    public (ulong Count, ulong Size) GetApproximateMemTableStats(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit)
    {
        _db.ThrowIfDisposed();
        return _db.GetApproximateMemTableStats(_cfd, start, limit);
    }

    // ---- properties --------------------------------------------------------------------------------------

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

    /// <summary><see cref="ColumnFamily.GetApproximateSizes(ReadOnlySpan{KeyRange}, Span{ulong})"/> on the default column family.</summary>
    public void GetApproximateSizes(ReadOnlySpan<KeyRange> ranges, Span<ulong> sizes) => _default.GetApproximateSizes(ranges, sizes);

    /// <summary><see cref="ColumnFamily.GetApproximateSizes(SizeApproximationOptions, ReadOnlySpan{KeyRange}, Span{ulong})"/> on the default column family.</summary>
    public void GetApproximateSizes(SizeApproximationOptions options, ReadOnlySpan<KeyRange> ranges, Span<ulong> sizes) =>
        _default.GetApproximateSizes(options, ranges, sizes);

    public (ulong Count, ulong Size) GetApproximateMemTableStats(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit) =>
        _default.GetApproximateMemTableStats(start, limit);

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
        IReadOnlyList<ColumnFamily> cfs = ColumnFamilies;
        for (int i = 0; i < cfs.Count; i++)
        {
            if (!cfs[i].TryGetIntProperty(property, out ulong v)) return false;
            value += v;
        }
        return true;
    }
}
