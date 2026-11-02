using RocksDb.Managed.Table;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// Concatenating iterator over the sorted, non-overlapping files of one level (L1+), opening each file lazily through
/// an <see cref="ITableCache"/>. Port of LevelIterator (db/version_set.cc) together with FindFile
/// (binary search on each file's largest key, db/version_set.cc).
/// </summary>
/// <remarks>
/// <para>Only the current file's iterator is open; moving past a file boundary disposes it and opens the neighbour.
/// With <see cref="ReadOptions.IterateUpperBound"/> (resp. lower bound) set, files whose smallest (resp. largest) user
/// key is outside the bound are not opened (upstream's KeyReachedUpperBound check in SkipEmptyFileForward).</para>
/// <para>Range tombstones are not surfaced here (upstream emits file-boundary sentinel keys for them); the DB layer
/// collects every file's tombstones into the <see cref="RangeDelAggregator"/> when the iterator is created.</para>
/// <para>Async variants open files through <see cref="ITableCache.GetReaderAsync"/> first (so a table open awaits
/// instead of blocking) and use the file iterator's async paths.</para>
/// </remarks>
public sealed class LevelIterator : InternalIterator
{
    private readonly ITableCache _tableCache;
    private readonly InternalKeyComparator _icmp;
    private readonly FileMetaData[] _files;
    private readonly ReadOnlyMemory<byte>? _lower;
    private readonly ReadOnlyMemory<byte>? _upper;
    private readonly ReadOptions? _readOptions;
    /// <summary>The column family's prefix extractor (file iterators consult prefix filters), or null.</summary>
    private readonly SliceTransform? _prefixExtractor;
    /// <summary>Non-null in legacy prefix seek mode (no total_order_seek / auto_prefix_mode): Seek invalidates past the prefix.</summary>
    private readonly SliceTransform? _prefixSeekExtractor;
    private InternalIterator? _fileIter;
    private InternalIterator? _spare; // a file iterator parked for reuse while the level iterator is exhausted
    private int _fileIndex = -1;
    private bool _disposed;

    /// <param name="tableCache">Opens file iterators.</param>
    /// <param name="comparator">Internal key comparator.</param>
    /// <param name="files">Files of the level, sorted by smallest key and non-overlapping.</param>
    /// <param name="readOptions">Iterate bounds (may be null).</param>
    public LevelIterator(ITableCache tableCache, InternalKeyComparator comparator, IReadOnlyList<FileMetaData> files, ReadOptions? readOptions)
        : this(tableCache, comparator, files, readOptions, prefixExtractor: null)
    {
    }

    /// <param name="tableCache">Opens file iterators.</param>
    /// <param name="comparator">Internal key comparator.</param>
    /// <param name="files">Files of the level, sorted by smallest key and non-overlapping.</param>
    /// <param name="readOptions">Iterate bounds and prefix seek mode (may be null).</param>
    /// <param name="prefixExtractor">The column family's prefix extractor (null: total order).</param>
    public LevelIterator(ITableCache tableCache, InternalKeyComparator comparator, IReadOnlyList<FileMetaData> files, ReadOptions? readOptions,
        SliceTransform? prefixExtractor)
    {
        _readOptions = readOptions;
        _prefixExtractor = prefixExtractor;
        if (prefixExtractor is not null && readOptions?.TotalOrderSeek != true && readOptions?.AutoPrefixMode != true)
            _prefixSeekExtractor = prefixExtractor;
        ArgumentNullException.ThrowIfNull(tableCache);
        ArgumentNullException.ThrowIfNull(comparator);
        ArgumentNullException.ThrowIfNull(files);
        _tableCache = tableCache;
        _icmp = comparator;
        _files = new FileMetaData[files.Count];
        for (int i = 0; i < _files.Length; i++) _files[i] = files[i];
        _lower = readOptions?.IterateLowerBound;
        _upper = readOptions?.IterateUpperBound;
    }

    public int FileCount => _files.Length;

    /// <summary>Index of the file the iterator is positioned in (-1 if none).</summary>
    public int CurrentFileIndex => _fileIter is null ? -1 : _fileIndex;

    public override bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _fileIter is TableInternalIterator t ? t.Valid : _fileIter is not null && _fileIter.Valid;
    }

    // File iterators are table iterators (unless a table cache returns another type): direct, inlinable calls.
    public override ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _fileIter is TableInternalIterator t ? t.Key : _fileIter!.Key;
    }

    public override ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _fileIter is TableInternalIterator t ? t.Value : _fileIter!.Value;
    }

    /// <summary>
    /// Port of FindFile (db/version_set.cc): index of the first file whose largest key is &gt;= <paramref name="key"/>,
    /// or <c>files.Count</c> if none.
    /// </summary>
    public static int FindFile(InternalKeyComparator icmp, IReadOnlyList<FileMetaData> files, ReadOnlySpan<byte> key)
    {
        int left = 0, right = files.Count;
        while (left < right)
        {
            int mid = (left + right) >>> 1;
            if (icmp.Compare(files[mid].Largest, key) < 0)
            {
                // Key at "mid.largest" is < "target". Therefore all files at or before "mid" are uninteresting.
                left = mid + 1;
            }
            else
            {
                // Key at "mid.largest" is >= "target". Therefore all files after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    private int FindFile(ReadOnlySpan<byte> key) => FindFile(_icmp, _files, key);

    public override void SeekToFirst()
    {
        InitFileIterator(0);
        _fileIter?.SeekToFirst();
        SkipEmptyFileForward();
    }

    public override void SeekToLast()
    {
        InitFileIterator(_files.Length - 1);
        _fileIter?.SeekToLast();
        SkipEmptyFileBackward();
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        InitFileIterator(FindFile(target));
        _fileIter?.Seek(target);
        if (SkipEmptyFileForward()) CheckPrefixAfterSkippedFile(target);
    }

    /// <summary>
    /// Port of the prefix check in LevelIterator::Seek: we've skipped the file we initially positioned to. In the prefix
    /// seek case, it is likely that the file is skipped because of prefix bloom, where more keys are skipped. We then
    /// check the current key and invalidate the iterator if the prefix is already passed (so the merging iterator
    /// merges fewer children and results within the prefix stay exact).
    /// </summary>
    private void CheckPrefixAfterSkippedFile(ReadOnlySpan<byte> target)
    {
        SliceTransform? pe = _prefixSeekExtractor;
        if (pe is null || _fileIter is null || !_fileIter.Valid) return;
        ReadOnlySpan<byte> targetUserKey = DbFormat.ExtractUserKey(target);
        ReadOnlySpan<byte> fileUserKey = DbFormat.ExtractUserKey(_fileIter.Key);
        if (pe.InDomain(targetUserKey)
            && (!pe.InDomain(fileUserKey) || _icmp.CompareUserKey(pe.Transform(targetUserKey), pe.Transform(fileUserKey)) != 0))
        {
            SetFileIterator(null, -1);
        }
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        int index = FindFile(target);
        if (index >= _files.Length) index = _files.Length - 1;
        InitFileIterator(index);
        _fileIter?.SeekForPrev(target);
        SkipEmptyFileBackward();
    }

    public override void Next()
    {
        if (_fileIter is TableInternalIterator t) t.Next();
        else _fileIter!.Next();
        SkipEmptyFileForward();
    }

    public override void Prev()
    {
        _fileIter!.Prev();
        SkipEmptyFileBackward();
    }

    /// <summary>Moves forward to the first non-empty file. Returns true if the current file was empty (seen_empty_file).</summary>
    private bool SkipEmptyFileForward()
    {
        bool seenEmptyFile = false;
        while (_fileIter is null || !_fileIter.Valid)
        {
            seenEmptyFile = true;
            // Move to next file
            int next = _fileIter is null ? _files.Length : _fileIndex + 1;
            if (next >= _files.Length || FileStartsAtOrAfterUpperBound(next))
            {
                SetFileIterator(null, -1);
                return seenEmptyFile;
            }
            InitFileIterator(next);
            _fileIter!.SeekToFirst();
        }
        return seenEmptyFile;
    }

    private void SkipEmptyFileBackward()
    {
        while (_fileIter is null || !_fileIter.Valid)
        {
            // Move to previous file
            int prev = _fileIter is null ? -1 : _fileIndex - 1;
            if (prev < 0 || FileEndsBeforeLowerBound(prev))
            {
                SetFileIterator(null, -1);
                return;
            }
            InitFileIterator(prev);
            _fileIter!.SeekToLast();
        }
    }

    private bool FileStartsAtOrAfterUpperBound(int index) =>
        _upper is ReadOnlyMemory<byte> ub && _icmp.CompareUserKey(DbFormat.ExtractUserKey(_files[index].Smallest), ub.Span) >= 0;

    private bool FileEndsBeforeLowerBound(int index) =>
        _lower is ReadOnlyMemory<byte> lb && _icmp.CompareUserKey(DbFormat.ExtractUserKey(_files[index].Largest), lb.Span) < 0;

    /// <summary>Opens (or keeps) the iterator for file <paramref name="index"/>; null iterator if out of range.</summary>
    private void InitFileIterator(int index)
    {
        if ((uint)index >= (uint)_files.Length)
        {
            SetFileIterator(null, -1);
            return;
        }
        // If the file iterator shows incomplete, we try it again if users seek to the same file, as this time we may
        // go to a different data block which is cached in block cache.
        if (_fileIter is not null && _fileIndex == index) return;
        // The current file's iterator is recycled for the new file (no allocation per file switch).
        InternalIterator? old = _fileIter ?? _spare;
        if (_fileIter is not null) _spare?.Dispose();
        _fileIter = null;
        _spare = null;
        _fileIndex = -1;
        try
        {
            _fileIter = NewFileIterator(index, old);
        }
        catch
        {
            // Opening the file failed; the old iterator was not recycled (Dispose is idempotent).
            old?.Dispose();
            throw;
        }
        _fileIndex = index;
    }

    private InternalIterator NewFileIterator(int index, InternalIterator? reuse)
    {
        if (_prefixExtractor is null) return _tableCache.NewIterator(_files[index], reuse);
        TableReaderLease lease = _tableCache.GetReader(_files[index]);
        if (reuse is TableInternalIterator t && t.Flags == TableReadFlags.None)
        {
            t.Retarget(lease, _readOptions, _prefixExtractor);
            return t;
        }
        reuse?.Dispose();
        return new TableInternalIterator(lease, TableReadFlags.None, _readOptions, _prefixExtractor);
    }

    private void SetFileIterator(InternalIterator? iter, int index)
    {
        InternalIterator? old = _fileIter;
        _fileIter = iter;
        _fileIndex = index;
        if (old is null || ReferenceEquals(old, iter)) return;
        if (iter is null && !_disposed && _spare is null && old is TableInternalIterator t)
        {
            // Keep the objects for the next file (see InitFileIterator) but release the file now, like upstream's
            // LevelIterator dropping file_iter_.
            t.Park();
            _spare = t;
            return;
        }
        old.Dispose();
    }

    // ------------------------------------------------------------------ async

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) =>
        PositionAsync(0, default, Op.First, cancellationToken);

    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        int index = FindFile(target.Span);
        if (_prefixSeekExtractor is null) return PositionAsync(index, target, Op.Seek, cancellationToken);
        // Prefix seek: apply LevelIterator::Seek's prefix check when the seek moved past the initial file.
        ValueTask t = PositionAsync(index, target, Op.Seek, cancellationToken);
        if (!t.IsCompletedSuccessfully) return SeekPrefixSlowAsync(t, index, target);
        t.GetAwaiter().GetResult();
        if (_fileIndex != index) CheckPrefixAfterSkippedFile(target.Span);
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SeekPrefixSlowAsync(ValueTask pending, int index, ReadOnlyMemory<byte> target)
    {
        await pending.ConfigureAwait(false);
        if (_fileIndex != index) CheckPrefixAfterSkippedFile(target.Span);
    }

    public override ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        ValueTask t = _fileIter!.NextAsync(cancellationToken);
        if (!t.IsCompletedSuccessfully) return NextSlowAsync(t, cancellationToken);
        t.GetAwaiter().GetResult();
        if (_fileIter.Valid) return ValueTask.CompletedTask;
        return AdvanceFileAsync(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask NextSlowAsync(ValueTask pending, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (!_fileIter!.Valid) await AdvanceFileAsync(ct).ConfigureAwait(false);
    }

    private ValueTask AdvanceFileAsync(CancellationToken ct)
    {
        int next = _fileIndex + 1;
        if (next >= _files.Length || FileStartsAtOrAfterUpperBound(next))
        {
            SetFileIterator(null, -1);
            return ValueTask.CompletedTask;
        }
        return PositionAsync(next, default, Op.First, ct);
    }

    private enum Op : byte { First, Seek }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionAsync(int index, ReadOnlyMemory<byte> target, Op op, CancellationToken ct)
    {
        while (true)
        {
            if ((uint)index >= (uint)_files.Length)
            {
                SetFileIterator(null, -1);
                return;
            }
            if (_fileIter is null || _fileIndex != index)
            {
                // Warm the table cache asynchronously so NewIterator does not block on the table open.
                using (await _tableCache.GetReaderAsync(_files[index], ct).ConfigureAwait(false))
                {
                    InitFileIterator(index);

                }
            }
            InternalIterator it = _fileIter!;
            if (op == Op.Seek) await it.SeekAsync(target, ct).ConfigureAwait(false);
            else await it.SeekToFirstAsync(ct).ConfigureAwait(false);
            if (it.Valid) return;
            index++;
            op = Op.First;
            if (index >= _files.Length || FileStartsAtOrAfterUpperBound(index))
            {
                SetFileIterator(null, -1);
                return;
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _disposed = true;
        SetFileIterator(null, -1);
        _spare?.Dispose();
        _spare = null;
    }
}
