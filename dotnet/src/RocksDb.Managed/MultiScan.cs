using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>One range of a <see cref="MultiScanArgs"/>: [<see cref="Start"/>, <see cref="Limit"/>) (no limit = to the end).</summary>
public readonly record struct ScanRange(ReadOnlyMemory<byte> Start, ReadOnlyMemory<byte>? Limit);

/// <summary>
/// The ranges of a multi-range scan, in increasing start-key order, and its prefetch settings. Port of
/// rocksdb::MultiScanArgs / ScanOptions (include/rocksdb/options.h).
/// </summary>
public sealed class MultiScanArgs
{
    private readonly List<ScanRange> _ranges = new();

    /// <summary>The ranges added so far.</summary>
    public IReadOnlyList<ScanRange> Ranges => _ranges;

    /// <summary>Adds the unbounded range [<paramref name="start"/>, end).</summary>
    public MultiScanArgs Add(ReadOnlyMemory<byte> start)
    {
        _ranges.Add(new ScanRange(start, null));
        return this;
    }

    /// <summary>Adds the range [<paramref name="start"/>, <paramref name="limit"/>).</summary>
    public MultiScanArgs Add(ReadOnlyMemory<byte> start, ReadOnlyMemory<byte> limit)
    {
        _ranges.Add(new ScanRange(start, limit));
        return this;
    }

    /// <summary>Data block reads of one file closer than this many bytes are merged into one prefetch (io_coalesce_threshold, default 16 KiB).</summary>
    public ulong IoCoalesceThreshold { get; init; } = 16 << 10;

    /// <summary>
    /// Maximum bytes prefetched per file (max_prefetch_size, 0 = no limit). Unlike upstream, reaching it only stops the
    /// prefetching (upstream's iterator then fails with PrefetchLimitReached).
    /// </summary>
    public ulong MaxPrefetchSize { get; init; }

    /// <summary>Scan the ranges in reverse order, each from its limit down to its start (reverse); every range needs a limit.</summary>
    public bool Reverse { get; init; }
}

/// <summary>
/// Scans several key ranges with one iterator (DB::NewMultiScan, db/multi_scan.cc). Port of MultiScan /
/// MultiScanIterator: <see cref="MoveNextRange"/> positions at the next range, then <see cref="Valid"/> /
/// <see cref="Key"/> / <see cref="Value"/> / <see cref="Next"/> walk it. When every range is bounded (or none is),
/// the data blocks of all ranges are prefetched up front (DBIter::Prepare, BlockBasedTableIterator::Prepare):
/// per table file overlapping a range the index locates the block span, nearby spans are coalesced and the OS is asked
/// to read them ahead. Not supported with user-defined timestamps (like upstream). Dispose to release the iterator.
/// </summary>
public sealed class MultiScan : IDisposable
{
    private readonly Iterator _it;
    private readonly Comparator _cmp;
    private readonly IReadOnlyList<ScanRange> _ranges;
    private readonly bool _reverse;
    private int _index = -1;
    // Prefetch planning scratch of Prepare (per thread; Prepare does not suspend).
    [ThreadStatic] private static List<(long Offset, long Length)>? t_spans;
    private bool _valid;

    internal MultiScan(DbImpl db, ColumnFamilyData cfd, Iterator it, MultiScanArgs args)
    {
        _it = it;
        _cmp = cfd.UserComparator;
        _ranges = args.Ranges;
        _reverse = args.Reverse;
        if (!MixedBounds(args.Ranges)) Prepare(db, cfd, args);
    }

    /// <summary>ValidateScanOptionsForMultiScan.</summary>
    internal static void Validate(ColumnFamilyData cfd, MultiScanArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (cfd.TimestampSize != 0) ThrowHelper.NotSupported("MultiScan is not supported with user-defined timestamps");
        if (args.Ranges.Count == 0) ThrowHelper.InvalidArgument("Empty MultiScanArgs");
        for (int i = 0; i < args.Ranges.Count; i++)
        {
            ScanRange r = args.Ranges[i];
            if (args.Reverse && r.Limit is null) ThrowHelper.InvalidArgument("Reverse MultiScan requires limit at index " + i);
            if (r.Limit is { } limit && cfd.UserComparator.Compare(r.Start.Span, limit.Span) >= 0)
            {
                ThrowHelper.InvalidArgument("Scan start key is large or equal than limit at index " + i);
            }
        }
        // DBIter::ValidateScanOptions, which upstream's MultiScan runs through Iterator::Prepare unless the ranges mix
        // bounded and unbounded ones (the slow path skips Prepare): only the first of several ranges may be unbounded
        // (so several unbounded ranges are rejected), and the ranges must be in order and must not overlap.
        if (MixedBounds(args.Ranges)) return;
        if (args.Ranges[0].Limit is null && args.Ranges.Count > 1) ThrowHelper.InvalidArgument("Scan has no upper bound");
        for (int i = 1; i < args.Ranges.Count; i++)
        {
            if (cfd.UserComparator.Compare(args.Ranges[i].Start.Span, args.Ranges[i - 1].Limit!.Value.Span) < 0)
            {
                ThrowHelper.InvalidArgument("Overlapping ranges at index " + i);
            }
        }
    }

    /// <summary>Mixed bounded and unbounded ranges take upstream's slow path without Prepare.</summary>
    private static bool MixedBounds(IReadOnlyList<ScanRange> ranges)
    {
        for (int i = 1; i < ranges.Count; i++)
        {
            if ((ranges[i].Limit is null) != (ranges[0].Limit is null)) return true;
        }
        return false;
    }

    /// <summary>
    /// The prefetch of DBIter::Prepare: for every table file of the current version overlapping a range, the data block
    /// span of the range, merged per file when closer than <see cref="MultiScanArgs.IoCoalesceThreshold"/>, capped at
    /// <see cref="MultiScanArgs.MaxPrefetchSize"/>, then handed to the OS (readahead).
    /// </summary>
    private static void Prepare(DbImpl db, ColumnFamilyData cfd, MultiScanArgs args)
    {
        SuperVersion sv = DbImpl.GetReferencedSuperVersion(cfd);
        try
        {
            VersionStorageInfo vs = sv.Current.Storage;
            Comparator ucmp = cfd.UserComparator;
            IReadOnlyList<ScanRange> ranges = args.Ranges;
            List<(long Offset, long Length)> spans = t_spans ??= new List<(long Offset, long Length)>();
            // Internal-key scratch for the range bounds (two keys per range; grown for long keys).
            byte[] keyBuf = RocksArrayPool<byte>.Shared.Rent(256);
            try
            {
                for (int level = 0; level < vs.NumLevels; level++)
                {
                    foreach (FileMetaData f in vs.LevelFiles(level))
                    {
                        spans.Clear();
                        for (int ri = 0; ri < ranges.Count; ri++)
                        {
                            ScanRange r = ranges[ri];
                            // Skip ranges that don't overlap the file.
                            if (ucmp.Compare(DbFormat.ExtractUserKey(f.Largest), r.Start.Span) < 0) continue;
                            if (r.Limit is { } lim && ucmp.Compare(DbFormat.ExtractUserKey(f.Smallest), lim.Span) >= 0) continue;
                            using TableReaderLease lease = cfd.TableCache.GetReader(f);
                            // Data-block prefetch planning needs the block-based index; other table formats are scanned without it.
                            if (lease.Reader is not Table.BlockBasedTableReader bbt) continue;
                            int startLen = r.Start.Length + DbFormat.InternalKeyFooterSize;
                            int endLen = r.Limit is { } l0 ? l0.Length + DbFormat.InternalKeyFooterSize : 0;
                            if (keyBuf.Length < startLen + endLen)
                            {
                                RocksArrayPool<byte>.Shared.Return(keyBuf);
                                keyBuf = RocksArrayPool<byte>.Shared.Rent(startLen + endLen);
                            }
                            Span<byte> start = keyBuf.AsSpan(0, startLen);
                            DbFormat.AppendInternalKey(start, r.Start.Span, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                            Span<byte> end = keyBuf.AsSpan(startLen, endLen);
                            if (r.Limit is { } l) DbFormat.AppendInternalKey(end, l.Span, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                            if (bbt.TryGetDataBlockSpan(start, end, r.Limit is not null, out long off, out long len)) spans.Add((off, len));
                        }
                        if (spans.Count == 0) continue;
                        spans.Sort(static (a, b) => a.Offset.CompareTo(b.Offset));
                        long budget = args.MaxPrefetchSize == 0 ? long.MaxValue : (long)Math.Min(args.MaxPrefetchSize, long.MaxValue);
                        long curOff = spans[0].Offset, curEnd = spans[0].Offset + spans[0].Length;
                        using TableReaderLease reader = cfd.TableCache.GetReader(f);
                        for (int i = 1; i <= spans.Count && budget > 0; i++)
                        {
                            if (i < spans.Count && spans[i].Offset <= curEnd + (long)args.IoCoalesceThreshold)
                            {
                                curEnd = Math.Max(curEnd, spans[i].Offset + spans[i].Length);
                                continue;
                            }
                            long n = Math.Min(curEnd - curOff, budget);
                            ((Table.BlockBasedTableReader)reader.Reader).File.Prefetch(curOff, n);
                            budget -= n;
                            if (i < spans.Count)
                            {
                                curOff = spans[i].Offset;
                                curEnd = curOff + spans[i].Length;
                            }
                        }
                    }
                }
            }
            finally
            {
                spans.Clear();
                RocksArrayPool<byte>.Shared.Return(keyBuf);
            }
        }
        finally
        {
            db.ReturnSuperVersion(sv);
        }
    }

    /// <summary>Index of the current range (-1 before the first <see cref="MoveNextRange"/>).</summary>
    public int RangeIndex => _index;

    /// <summary>The current range.</summary>
    public ScanRange CurrentRange => _ranges[_index];

    /// <summary>
    /// Positions at the first entry of the next range (forward: its start; reverse: the last key before its limit).
    /// Returns false when all ranges were visited.
    /// </summary>
    public bool MoveNextRange()
    {
        if (++_index >= _ranges.Count)
        {
            _index = _ranges.Count;
            _valid = false;
            return false;
        }
        ScanRange r = _ranges[_index];
        if (_reverse)
        {
            ReadOnlySpan<byte> limit = r.Limit!.Value.Span;
            _it.SeekForPrev(limit);
            if (_it.Valid && _cmp.Compare(_it.Key, limit) >= 0) _it.Prev();
        }
        else
        {
            _it.Seek(r.Start.Span);
        }
        UpdateValid();
        return true;
    }

    /// <summary>Async <see cref="MoveNextRange"/> (table blocks are read with async I/O).</summary>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> MoveNextRangeAsync(CancellationToken cancellationToken = default)
    {
        if (++_index >= _ranges.Count)
        {
            _index = _ranges.Count;
            _valid = false;
            return false;
        }
        ScanRange r = _ranges[_index];
        if (_reverse)
        {
            ReadOnlyMemory<byte> limit = r.Limit!.Value;
            await _it.SeekForPrevAsync(limit, cancellationToken).ConfigureAwait(false);
            if (_it.Valid && _cmp.Compare(_it.Key, limit.Span) >= 0) await _it.PrevAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _it.SeekAsync(r.Start, cancellationToken).ConfigureAwait(false);
        }
        UpdateValid();
        return true;
    }

    private void UpdateValid()
    {
        if (!_it.Valid)
        {
            _valid = false;
            return;
        }
        ScanRange r = _ranges[_index];
        _valid = _reverse
            ? _cmp.Compare(_it.Key, r.Start.Span) >= 0
            : r.Limit is not { } limit || _cmp.Compare(_it.Key, limit.Span) < 0;
    }

    /// <summary>True if positioned at an entry of the current range.</summary>
    public bool Valid => _valid;

    /// <summary>Current key. REQUIRES: <see cref="Valid"/>; valid until the scan moves.</summary>
    public ReadOnlySpan<byte> Key => _valid ? _it.Key : throw new InvalidOperationException("Trying to deref invalid iterator");

    /// <summary>Current value. REQUIRES: <see cref="Valid"/>; valid until the scan moves.</summary>
    public ReadOnlySpan<byte> Value => _valid ? _it.Value : throw new InvalidOperationException("Trying to deref invalid iterator");

    /// <summary>Moves to the next entry of the current range (the previous one with <see cref="MultiScanArgs.Reverse"/>).</summary>
    public void Next()
    {
        if (!_valid) throw new InvalidOperationException("Trying to advance invalid iterator");
        if (_reverse) _it.Prev();
        else _it.Next();
        UpdateValid();
    }

    /// <summary>Async <see cref="Next"/>.</summary>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        if (!_valid) throw new InvalidOperationException("Trying to advance invalid iterator");
        if (_reverse) await _it.PrevAsync(cancellationToken).ConfigureAwait(false);
        else await _it.NextAsync(cancellationToken).ConfigureAwait(false);
        UpdateValid();
    }

    public void Dispose() => _it.Dispose();
}

public sealed partial class ColumnFamily
{
    /// <summary>
    /// Creates a scan over several key ranges of this column family (DB::NewMultiScan). The ranges must be in increasing
    /// start order, each start below its limit (InvalidArgument otherwise). <see cref="ReadOptions.IterateLowerBound"/>
    /// and <see cref="ReadOptions.IterateUpperBound"/> are ignored (each range bounds the scan).
    /// </summary>
    public MultiScan NewMultiScan(ReadOptions? options, MultiScanArgs args)
    {
        MultiScan.Validate(_cfd, args);
        // The iterate bounds come from the ranges (MultiScan sets iterate_lower/upper_bound per range): options with
        // bounds are copied without them; options without bounds are used as they are.
        ReadOptions? ro = options;
        if (options is not null && (options.IterateLowerBound is not null || options.IterateUpperBound is not null))
        {
            ro = options.Clone();
            ro.IterateLowerBound = null;
            ro.IterateUpperBound = null;
        }
        Iterator it = NewIterator(ro);
        try
        {
            return new MultiScan(_db, _cfd, it, args);
        }
        catch
        {
            it.Dispose();
            throw;
        }
    }

    /// <inheritdoc cref="NewMultiScan(ReadOptions, MultiScanArgs)"/>
    public MultiScan NewMultiScan(MultiScanArgs args) => NewMultiScan(null, args);
}

public sealed partial class RocksDatabase
{
    /// <inheritdoc cref="ColumnFamily.NewMultiScan(ReadOptions, MultiScanArgs)"/>
    public MultiScan NewMultiScan(MultiScanArgs args) => _default.NewMultiScan(args);

    /// <inheritdoc cref="ColumnFamily.NewMultiScan(ReadOptions, MultiScanArgs)"/>
    public MultiScan NewMultiScan(ReadOptions? options, MultiScanArgs args) => _default.NewMultiScan(options, args);
}
