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
    private InternalIterator? _fileIter;
    private int _fileIndex = -1;
    private bool _disposed;

    /// <param name="tableCache">Opens file iterators.</param>
    /// <param name="comparator">Internal key comparator.</param>
    /// <param name="files">Files of the level, sorted by smallest key and non-overlapping.</param>
    /// <param name="readOptions">Iterate bounds (may be null).</param>
    public LevelIterator(ITableCache tableCache, InternalKeyComparator comparator, IReadOnlyList<FileMetaData> files, ReadOptions? readOptions)
    {
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
        get => _fileIter is not null && _fileIter.Valid;
    }

    public override ReadOnlySpan<byte> Key => _fileIter!.Key;
    public override ReadOnlySpan<byte> Value => _fileIter!.Value;

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
        SkipEmptyFileForward();
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
        _fileIter!.Next();
        SkipEmptyFileForward();
    }

    public override void Prev()
    {
        _fileIter!.Prev();
        SkipEmptyFileBackward();
    }

    private void SkipEmptyFileForward()
    {
        while (_fileIter is null || !_fileIter.Valid)
        {
            // Move to next file
            int next = _fileIter is null ? _files.Length : _fileIndex + 1;
            if (next >= _files.Length || FileStartsAtOrAfterUpperBound(next))
            {
                SetFileIterator(null, -1);
                return;
            }
            InitFileIterator(next);
            _fileIter!.SeekToFirst();
        }
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
        SetFileIterator(_tableCache.NewIterator(_files[index]), index);
    }

    private void SetFileIterator(InternalIterator? iter, int index)
    {
        InternalIterator? old = _fileIter;
        _fileIter = iter;
        _fileIndex = index;
        old?.Dispose();
    }

    // ------------------------------------------------------------------ async

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) =>
        PositionAsync(0, default, Op.First, cancellationToken);

    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) =>
        PositionAsync(FindFile(target.Span), target, Op.Seek, cancellationToken);

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
                    SetFileIterator(_tableCache.NewIterator(_files[index]), index);
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
    }
}
