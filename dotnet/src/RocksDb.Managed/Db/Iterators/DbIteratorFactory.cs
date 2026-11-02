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
        RangeDelAggregator? rangeDel = sources.ReadOptions?.IgnoreRangeDeletions == true ? null : new RangeDelAggregator(sources.Comparator, sources.Sequence);
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

    private static DbIter Wrap(DbIteratorSources sources, InternalIterator internalIter, RangeDelAggregator? rangeDel) =>
        new(sources.Comparator, internalIter, sources.Sequence, sources.ReadOptions, sources.MergeOperator, rangeDel,
            sources.MaxSequentialSkipInIterations, sources.Cleanup, sources.PrefixExtractor);

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
                children.Add(new MemTableInternalIterator(mem));
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
                    foreach (FileMetaData f in files)
                    {
                        children.Add(sources.PrefixExtractor is null
                            ? tc.NewIterator(f)
                            : new TableInternalIterator(tc.GetReader(f), TableReadFlags.None, sources.ReadOptions, sources.PrefixExtractor));
                    }
                }
                else
                {
                    // For levels > 0, we can use a concatenating iterator that sequentially walks through the
                    // non-overlapping files in the level, opening them lazily.
                    children.Add(new LevelIterator(tc, sources.Comparator, files, sources.ReadOptions, sources.PrefixExtractor));
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
