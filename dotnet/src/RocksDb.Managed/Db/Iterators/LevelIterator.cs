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
/// <para>Async variants (forward and reverse) open files through <see cref="ITableCache.GetReaderAsync"/> first (so a
/// table open awaits instead of blocking) and use the file iterator's async paths.</para>
/// </remarks>
public sealed class LevelIterator : InternalIterator
{
    private readonly ITableCache _tableCache;
    private readonly InternalKeyComparator _icmp;
    private readonly FileMetaData[] _files;
    private readonly ReadOnlyMemory<byte>? _lower;
    private readonly ReadOnlyMemory<byte>? _upper;
    private readonly ReadOptions? _readOptions;
    // fill_cache / rate_limiter_priority of _readOptions for the file iterators.
    private TableReadFlags _readFlags;
    /// <summary>The column family's prefix extractor (file iterators consult prefix filters), or null.</summary>
    private readonly SliceTransform? _prefixExtractor;
    /// <summary>Non-null in legacy prefix seek mode (no total_order_seek / auto_prefix_mode): Seek invalidates past the prefix.</summary>
    private readonly SliceTransform? _prefixSeekExtractor;
    private InternalIterator? _fileIter;
    private InternalIterator? _spare; // a file iterator parked for reuse while the level iterator is exhausted
    private int _fileIndex = -1;
    private bool _disposed;
    private PinnedIteratorsManager? _pinMgr;

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
        _readFlags = TableReadFlagsExtensions.FromReadOptions(readOptions);
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
        SampleRead(isSeek: true);
    }

    public override void SeekToLast()
    {
        InitFileIterator(_files.Length - 1);
        _fileIter?.SeekToLast();
        SkipEmptyFileBackward();
        SampleRead(isSeek: true);
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        InitFileIterator(FindFile(target));
        _fileIter?.Seek(target);
        if (SkipEmptyFileForward()) CheckPrefixAfterSkippedFile(target);
        SampleRead(isSeek: true);
    }

    /// <summary>
    /// LevelIterator::SampleRead (db/version_set.cc): a sampled seek (1/1024) or step (1/65536) counts as a read of the
    /// current file, and as a collapsible-entry read when the entry is a deletion, single deletion or merge operand.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SampleRead(bool isSeek)
    {
        bool sampled = isSeek ? FileReadSample.ShouldSampleFileRead() : FileReadSample.ShouldSampleFileReadNext();
        if (sampled) SampleCurrentFile();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void SampleCurrentFile()
    {
        if ((uint)_fileIndex >= (uint)_files.Length || _fileIter is null || !_fileIter.Valid) return;
        FileMetaData meta = _files[_fileIndex];
        FileReadSample.SampleFileReadInc(meta);
        ReadOnlySpan<byte> key = _fileIter.Key;
        if (key.Length < DbFormat.InternalKeyFooterSize) return;
        var type = (EntryType)key[^DbFormat.InternalKeyFooterSize];
        if (type is EntryType.Deletion or EntryType.SingleDeletion or EntryType.DeletionWithTimestamp or EntryType.Merge)
        {
            FileReadSample.SampleCollapsibleEntryFileReadInc(meta);
        }
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
        SampleRead(isSeek: true);
    }

    public override void Next()
    {
        if (_fileIter is TableInternalIterator t) t.Next();
        else _fileIter!.Next();
        SkipEmptyFileForward();
        SampleRead(isSeek: false);
    }

    public override void Prev()
    {
        _fileIter!.Prev();
        SkipEmptyFileBackward();
        SampleRead(isSeek: false);
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
        _upper is ReadOnlyMemory<byte> ub && CompareToBound(DbFormat.ExtractUserKey(_files[index].Smallest), ub.Span) >= 0;

    private bool FileEndsBeforeLowerBound(int index) =>
        _lower is ReadOnlyMemory<byte> lb && CompareToBound(DbFormat.ExtractUserKey(_files[index].Largest), lb.Span) < 0;

    /// <summary>A file boundary user key against an iterate bound; bounds carry no user-defined timestamp.</summary>
    private int CompareToBound(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> bound)
    {
        Comparator ucmp = _icmp.UserComparator;
        return ucmp.TimestampSize == 0 ? _icmp.CompareUserKey(userKey, bound) : ucmp.CompareWithoutTimestamp(userKey, true, bound, false);
    }

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
            _fileIter.SetPinnedItersMgr(_pinMgr);
        }
        catch
        {
            // Opening the file failed; the old iterator was not recycled (Dispose is idempotent).
            old?.Dispose();
            throw;
        }
        _fileIndex = index;
    }

    /// <summary>Port of LevelIterator::SetPinnedItersMgr: passed to every file iterator.</summary>
    public override void SetPinnedItersMgr(PinnedIteratorsManager? manager)
    {
        _pinMgr = manager;
        _fileIter?.SetPinnedItersMgr(manager);
        _spare?.SetPinnedItersMgr(manager);
    }

    public override bool TryGetPinnedValue(out PinnedSlice value) => _fileIter!.TryGetPinnedValue(out value);

    /// <summary>Files <paramref name="from"/>..<paramref name="to"/> (inclusive) all have seqnos below <paramref name="seq"/> (known ranges).</summary>
    private bool FilesBelow(int from, int to, ulong seq)
    {
        for (int i = from; i <= to; i++)
        {
            FileMetaData f = _files[i];
            if (f.SmallestSeqno > f.LargestSeqno || f.LargestSeqno >= seq) return false;
        }
        return true;
    }

    /// <summary>
    /// Range-deletion skip (see <see cref="InternalIterator.SkipCoveredForward"/>): seeks to <paramref name="target"/>
    /// when every file from the current one to the one holding the target has seqnos below the tombstone's. Not used in
    /// legacy prefix seek mode (a seek there may stop at the prefix end).
    /// </summary>
    public override bool SkipCoveredForward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (!CanSkipForward(target, tombstoneSeq)) return false;
        Seek(target);
        return true;
    }

    public override bool SkipCoveredBackward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (!CanSkipBackward(target, tombstoneSeq)) return false;
        SeekForPrev(target);
        return true;
    }

    public override ValueTask<bool> SkipCoveredForwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default) =>
        CanSkipForward(target.Span, tombstoneSeq) ? TableInternalIterator.Moved(SeekAsync(target, cancellationToken)) : new(false);

    public override ValueTask<bool> SkipCoveredBackwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default) =>
        CanSkipBackward(target.Span, tombstoneSeq) ? TableInternalIterator.Moved(SeekForPrevAsync(target, cancellationToken)) : new(false);

    private bool CanSkipForward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (_prefixSeekExtractor is not null || _fileIter is null || !_fileIter.Valid || _icmp.Compare(target, _fileIter.Key) <= 0) return false;
        int last = Math.Min(FindFile(target), _files.Length - 1);
        return FilesBelow(_fileIndex, last, tombstoneSeq);
    }

    private bool CanSkipBackward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (_prefixSeekExtractor is not null || _fileIter is null || !_fileIter.Valid || _icmp.Compare(target, _fileIter.Key) >= 0) return false;
        int first = Math.Min(FindFile(target), _files.Length - 1);
        return first <= _fileIndex && FilesBelow(first, _fileIndex, tombstoneSeq);
    }

    /// <summary>Adds table read flags for every file of the level (e.g. skip_filters of Version::IsFilterSkipped).</summary>
    internal void AddReadFlags(TableReadFlags flags) => _readFlags |= flags;

    private InternalIterator NewFileIterator(int index, InternalIterator? reuse)
    {
        InternalIterator it = NewFileIteratorCore(index, reuse);
        // ReadOptions::readahead_size: explicit readahead instead of the automatic one.
        if (_readOptions is { ReadaheadSize: > 0 } ro && it is TableInternalIterator t) t.SetUserReadahead(ro.ReadaheadSize);
        return it;
    }

    private InternalIterator NewFileIteratorCore(int index, InternalIterator? reuse)
    {
        if (_prefixExtractor is null)
        {
            return _readFlags == TableReadFlags.None
                ? _tableCache.NewIterator(_files[index], reuse)
                : TableCache.Recycle(reuse, _tableCache.GetReader(_files[index]), _readFlags);
        }
        TableReaderLease lease = _tableCache.GetReader(_files[index]);
        if (reuse is TableInternalIterator t && t.Flags == _readFlags && lease.Reader is BlockBasedTableReader)
        {
            t.Retarget(lease, _readOptions, _prefixExtractor);
            return t;
        }
        reuse?.Dispose();
        return TableInternalIterator.Create(lease, _readFlags, _readOptions, _prefixExtractor);
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

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        ValueTask t = PositionAsync(0, default, Op.First, cancellationToken);
        if (!t.IsCompletedSuccessfully) return SampleAfterAsync(t, isSeek: true);
        t.GetAwaiter().GetResult();
        SampleRead(isSeek: true);
        return ValueTask.CompletedTask;
    }

    /// <summary>Awaits <paramref name="pending"/>, then <see cref="SampleRead"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SampleAfterAsync(ValueTask pending, bool isSeek)
    {
        await pending.ConfigureAwait(false);
        SampleRead(isSeek);
    }

    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        int index = FindFile(target.Span);
        ValueTask t = PositionAsync(index, target, Op.Seek, cancellationToken);
        if (!t.IsCompletedSuccessfully) return SeekSlowAsync(t, index, target);
        t.GetAwaiter().GetResult();
        // Prefix seek: apply LevelIterator::Seek's prefix check when the seek moved past the initial file.
        if (_prefixSeekExtractor is not null && _fileIndex != index) CheckPrefixAfterSkippedFile(target.Span);
        SampleRead(isSeek: true);
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SeekSlowAsync(ValueTask pending, int index, ReadOnlyMemory<byte> target)
    {
        await pending.ConfigureAwait(false);
        if (_prefixSeekExtractor is not null && _fileIndex != index) CheckPrefixAfterSkippedFile(target.Span);
        SampleRead(isSeek: true);
    }

    public override ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        ValueTask t = _fileIter!.NextAsync(cancellationToken);
        if (!t.IsCompletedSuccessfully) return NextSlowAsync(t, cancellationToken);
        t.GetAwaiter().GetResult();
        if (_fileIter.Valid)
        {
            SampleRead(isSeek: false);
            return ValueTask.CompletedTask;
        }
        t = AdvanceFileAsync(cancellationToken);
        if (!t.IsCompletedSuccessfully) return SampleAfterAsync(t, isSeek: false);
        t.GetAwaiter().GetResult();
        SampleRead(isSeek: false);
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask NextSlowAsync(ValueTask pending, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (!_fileIter!.Valid) await AdvanceFileAsync(ct).ConfigureAwait(false);
        SampleRead(isSeek: false);
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

    public override ValueTask SeekToLastAsync(CancellationToken cancellationToken = default) =>
        SampleAfter(PositionBackwardAsync(_files.Length - 1, default, seek: false, cancellationToken), isSeek: true);

    public override ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        int index = FindFile(target.Span);
        if (index >= _files.Length) index = _files.Length - 1;
        return SampleAfter(PositionBackwardAsync(index, target, seek: true, cancellationToken), isSeek: true);
    }

    public override ValueTask PrevAsync(CancellationToken cancellationToken = default)
    {
        ValueTask t = _fileIter!.PrevAsync(cancellationToken);
        if (!t.IsCompletedSuccessfully) return PrevSlowAsync(t, cancellationToken);
        t.GetAwaiter().GetResult();
        if (_fileIter.Valid)
        {
            SampleRead(isSeek: false);
            return ValueTask.CompletedTask;
        }
        return SampleAfter(RetreatFileAsync(cancellationToken), isSeek: false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PrevSlowAsync(ValueTask pending, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (!_fileIter!.Valid) await RetreatFileAsync(ct).ConfigureAwait(false);
        SampleRead(isSeek: false);
    }

    /// <summary>SkipEmptyFileBackward with async file opens and positioning.</summary>
    private ValueTask RetreatFileAsync(CancellationToken ct)
    {
        int prev = _fileIndex - 1;
        if (prev < 0 || FileEndsBeforeLowerBound(prev))
        {
            SetFileIterator(null, -1);
            return ValueTask.CompletedTask;
        }
        return PositionBackwardAsync(prev, default, seek: false, ct);
    }

    /// <summary><see cref="SampleRead"/> once <paramref name="pending"/> completes (synchronously when it already has).</summary>
    private ValueTask SampleAfter(ValueTask pending, bool isSeek)
    {
        if (!pending.IsCompletedSuccessfully) return SampleAfterAsync(pending, isSeek);
        pending.GetAwaiter().GetResult();
        SampleRead(isSeek);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Backward counterpart of <see cref="PositionAsync"/>: positions file <paramref name="index"/> (SeekForPrev when
    /// <paramref name="seek"/>, else SeekToLast) and moves to earlier files while empty (SkipEmptyFileBackward, stopping
    /// at files that end before the lower bound). Completes synchronously when the files are open and their blocks
    /// cached.
    /// </summary>
    private ValueTask PositionBackwardAsync(int index, ReadOnlyMemory<byte> target, bool seek, CancellationToken ct)
    {
        while (true)
        {
            if ((uint)index >= (uint)_files.Length)
            {
                SetFileIterator(null, -1);
                return ValueTask.CompletedTask;
            }
            if (_fileIter is null || _fileIndex != index) return PositionBackwardSlowAsync(index, target, seek, ct);
            InternalIterator it = _fileIter;
            ValueTask t = seek ? it.SeekForPrevAsync(target, ct) : it.SeekToLastAsync(ct);
            if (!t.IsCompletedSuccessfully) return PositionBackwardSlowAsync(t, index, target, seek, ct);
            t.GetAwaiter().GetResult();
            if (it.Valid) return ValueTask.CompletedTask;
            index--;
            seek = false;
            if (index < 0 || FileEndsBeforeLowerBound(index))
            {
                SetFileIterator(null, -1);
                return ValueTask.CompletedTask;
            }
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionBackwardSlowAsync(ValueTask pending, int index, ReadOnlyMemory<byte> target, bool seek, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (_fileIter!.Valid) return;
        index--;
        if (index < 0 || FileEndsBeforeLowerBound(index))
        {
            SetFileIterator(null, -1);
            return;
        }
        await PositionBackwardAsync(index, default, seek: false, ct).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionBackwardSlowAsync(int index, ReadOnlyMemory<byte> target, bool seek, CancellationToken ct)
    {
        // Warm the table cache asynchronously so InitFileIterator does not block on the table open.
        using (await _tableCache.GetReaderAsync(_files[index], ct).ConfigureAwait(false))
        {
            InitFileIterator(index);
        }
        await PositionBackwardAsync(index, target, seek, ct).ConfigureAwait(false);
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
