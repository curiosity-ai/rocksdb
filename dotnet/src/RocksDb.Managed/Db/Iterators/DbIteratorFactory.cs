using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// Everything needed to build a user iterator over one column family: the memtables and table files of a super
/// version plus read settings. Cold-path object (one per iterator creation).
/// </summary>
public sealed class DbIteratorSources
{
    /// <summary>Internal key comparator of the column family.</summary>
    public required InternalKeyComparator Comparator { get; init; }

    /// <summary>Read options (bounds, IgnoreRangeDeletions, MaxSkippableInternalKeys); null = defaults.</summary>
    public ReadOptions? ReadOptions { get; init; }

    /// <summary>Snapshot sequence number (the snapshot's, or the DB's last sequence).</summary>
    public ulong Sequence { get; init; } = DbFormat.MaxSequenceNumber;

    /// <summary>The column family's merge operator, if any.</summary>
    public MergeOperator? MergeOperator { get; init; }

    /// <summary>Mutable memtable followed by immutable memtables (any order is correct; newest first is customary).</summary>
    public IReadOnlyList<MemTable> MemTables { get; init; } = [];

    /// <summary>
    /// Files per level, as in <c>ColumnFamilyVersion.Levels</c>: <c>Levels[0]</c> is L0 (overlapping files, one table
    /// iterator each); <c>Levels[n]</c> for n &gt;= 1 must be sorted by smallest key and non-overlapping (one
    /// <see cref="LevelIterator"/> each).
    /// </summary>
    public IReadOnlyList<IReadOnlyList<FileMetaData>> Levels { get; init; } = [];

    /// <summary>Opens table files; required when <see cref="Levels"/> contains files.</summary>
    public ITableCache? TableCache { get; init; }

    /// <summary>
    /// The column family's prefix extractor (ColumnFamilyOptions.PrefixExtractor). Unless
    /// <see cref="ReadOptions.TotalOrderSeek"/> is set, table seeks consult prefix filters and the iterator follows
    /// prefix seek semantics (see <see cref="DbIter"/>).
    /// </summary>
    public SliceTransform? PrefixExtractor { get; init; }

    /// <summary>max_sequential_skip_in_iterations (ColumnFamilyOptions, default 8).</summary>
    public ulong MaxSequentialSkipInIterations { get; init; } = 8;

    /// <summary>Disposed when the iterator is disposed, after its internal iterators (e.g. unrefs the super version).</summary>
    public IDisposable? Cleanup { get; init; }

    /// <summary>Read callback of a WritePrepared / WriteUnprepared read (DBIter's read_callback_), or null.</summary>
    internal ReadCallback? ReadCallback { get; init; }

    /// <summary>Reads the blobs of blob-index entries (the column family's blob source); null = blob values are NotSupported.</summary>
    internal Blob.BlobSource? BlobSource { get; init; }

    /// <summary>The version the table files come from (blob files must belong to it).</summary>
    internal VersionStorageInfo? BlobVersion { get; init; }

    /// <summary>
    /// optimize_filters_for_hits of the column family: table iterators of the last non-empty level, when it is not L0, do
    /// not consult prefix filters (Version::IsFilterSkipped(level), the skip_filters argument of AddIteratorsForLevel).
    /// </summary>
    internal bool OptimizeFiltersForHits { get; init; }

    /// <summary>VersionStorageInfo::num_non_empty_levels of the version the table files come from.</summary>
    internal int NumNonEmptyLevels { get; init; }
}

/// <summary>
/// Builds the iterator stack of a DB read: memtable iterators + one table iterator per L0 file + one
/// <see cref="LevelIterator"/> per non-empty level, merged by a <see cref="MergingIterator"/>, with all range
/// tombstones (memtables and files, truncated to file boundaries) collected into a <see cref="RangeDelAggregator"/>,
/// wrapped by a <see cref="DbIter"/>. Port of DBImpl::NewIteratorImpl / NewInternalIterator and
/// Version::AddIterators (db/db_impl/db_impl.cc, db/version_set.cc).
/// </summary>
/// <remarks>
/// Range tombstones of every file are loaded when the iterator is created (one table-cache lookup per file; the
/// fragmented list is cached per table reader), unlike upstream which loads level files' tombstones lazily.
/// </remarks>
public static class DbIteratorFactory
{
    /// <summary>Creates the public iterator (one call for the DB layer).</summary>
    public static Iterator Create(DbIteratorSources sources) => new(CreateDbIter(sources));

    /// <summary>Like <see cref="Create"/>, but table files are opened with async I/O.</summary>
    public static async ValueTask<Iterator> CreateAsync(DbIteratorSources sources, CancellationToken cancellationToken = default) =>
        new(await CreateDbIterAsync(sources, cancellationToken).ConfigureAwait(false));

    /// <summary>Creates the <see cref="DbIter"/> (without the public wrapper).</summary>
    public static DbIter CreateDbIter(DbIteratorSources sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        // Range tombstones are bounded by the read timestamp (NewRangeTombstoneIterator(read_options, ...) ts_upper_bound).
        RangeDelAggregator? rangeDel = sources.ReadOptions?.IgnoreRangeDeletions == true
            ? null
            : new RangeDelAggregator(sources.Comparator, sources.Sequence, sources.ReadOptions?.Timestamp is { } ts ? ts.Span : default);
        InternalIterator internalIter = NewInternalIterator(sources, rangeDel);
        return Wrap(sources, internalIter, rangeDel);
    }

    /// <summary>Async <see cref="CreateDbIter"/>.</summary>
    public static async ValueTask<DbIter> CreateDbIterAsync(DbIteratorSources sources, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sources);
        // Warm the table cache with async opens; the sync build below then only hits the cache.
        if (sources.TableCache is ITableCache tc)
        {
            foreach (IReadOnlyList<FileMetaData> level in sources.Levels)
            {
                foreach (FileMetaData f in level)
                {
                    using TableReaderLease lease = await tc.GetReaderAsync(f, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        return CreateDbIter(sources);
    }

    private static DbIter Wrap(DbIteratorSources sources, InternalIterator internalIter, RangeDelAggregator? rangeDel)
    {
        var it = new DbIter(sources.Comparator, internalIter, sources.Sequence, sources.ReadOptions, sources.MergeOperator, rangeDel,
            sources.MaxSequentialSkipInIterations, sources.Cleanup, sources.PrefixExtractor);
        if (sources.ReadCallback is { } cb) it.SetReadCallback(cb);
        if (sources.BlobSource is not null && sources.BlobVersion is not null) it.SetBlobSource(sources.BlobSource, sources.BlobVersion);
        return it;
    }

    /// <summary>
    /// Builds the merged internal iterator over all sources and adds their range tombstones to
    /// <paramref name="rangeDel"/> (if non-null). Port of DBImpl::NewInternalIterator.
    /// </summary>
    public static InternalIterator NewInternalIterator(DbIteratorSources sources, RangeDelAggregator? rangeDel)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var children = new List<InternalIterator>(sources.MemTables.Count + 8);
        try
        {
            foreach (MemTable mem in sources.MemTables)
            {
                children.Add(new MemTableInternalIterator(mem, sources.ReadOptions, sources.PrefixExtractor));
                if (rangeDel is not null && mem.NumRangeDeletes > 0) rangeDel.AddTombstones(mem.GetFragmentedRangeTombstones());
            }

            for (int level = 0; level < sources.Levels.Count; level++)
            {
                IReadOnlyList<FileMetaData> files = sources.Levels[level];
                if (files.Count == 0) continue;
                ITableCache tc = sources.TableCache ?? throw new ArgumentException("TableCache is required when files are present", nameof(sources));
                if (rangeDel is not null)
                {
                    foreach (FileMetaData f in files) AddFileTombstones(tc, f, rangeDel);
                }
                if (level == 0)
                {
                    // Merge all level zero files together since they may overlap.
                    bool shouldSample = FileReadSample.ShouldSampleFileRead();
                    for (int fi = 0; fi < files.Count; fi++)
                    {
                        FileMetaData f = files[fi];
                        TableReadFlags readFlags = TableReadFlagsExtensions.FromReadOptions(sources.ReadOptions);
                        InternalIterator child = sources.PrefixExtractor is null
                            ? readFlags == TableReadFlags.None ? tc.NewIterator(f) : TableInternalIterator.Create(tc.GetReader(f), readFlags, null, null)
                            : TableInternalIterator.Create(tc.GetReader(f), readFlags, sources.ReadOptions, sources.PrefixExtractor);
                        child.SetIterateUpperBound(sources.ReadOptions?.IterateUpperBound);
                        if (child is TableInternalIterator rt)
                        {
                            rt.SetReadaheadOptions(sources.ReadOptions, sources.PrefixExtractor);
                            if (sources.ReadOptions is { ReadaheadSize: > 0 } ro) rt.SetUserReadahead(ro.ReadaheadSize, ro.AsyncIo);
                        }
                        children.Add(child);
                        // Lets DbIter skip ranges covered by newer range tombstones (SkipCoveredForward / Backward).
                        if (child is TableInternalIterator t) t.SetSeqnoRange(f.SmallestSeqno, f.LargestSeqno);
                        // Count once for every L0 file. This is done per iterator creation rather than Seek(), while
                        // files in other levels are sampled on seek/next/prev (Version::AddIteratorsForLevel).
                        if (shouldSample) FileReadSample.SampleFileReadInc(f);
                    }
                }
                else
                {
                    // For levels > 0, we can use a concatenating iterator that sequentially walks through the
                    // non-overlapping files in the level, opening them lazily. Only here does IsFilterSkipped(level)
                    // apply: L0 table iterators pass skip_filters = false (version_set.cc AddIteratorsForLevel).
                    var levelIter = new LevelIterator(tc, sources.Comparator, files, sources.ReadOptions, sources.PrefixExtractor);
                    if (sources.OptimizeFiltersForHits && level == sources.NumNonEmptyLevels - 1) levelIter.AddReadFlags(TableReadFlags.SkipFilters);
                    children.Add(levelIter);
                }

            }
        }
        catch
        {
            foreach (InternalIterator c in children) c.Dispose();
            throw;
        }
        return children.Count switch
        {
            0 => EmptyInternalIterator.Instance,
            1 => children[0],
            _ => new MergingIterator(sources.Comparator, System.Runtime.InteropServices.CollectionsMarshal.AsSpan(children)),
        };
    }

    private static void AddFileTombstones(ITableCache tc, FileMetaData file, RangeDelAggregator rangeDel)
    {
        using TableReaderLease lease = tc.GetReader(file);
        FragmentedRangeTombstoneList? list = RangeDelTableTombstones.Get(lease.Reader);
        if (list is not null) rangeDel.AddTombstones(list, file.Smallest, file.Largest);
    }
}
