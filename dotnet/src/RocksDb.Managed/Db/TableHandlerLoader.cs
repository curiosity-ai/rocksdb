using System.Runtime.ExceptionServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// Opens the table readers of a version being built into the table cache with several threads. Port of
/// VersionBuilder::Rep::LoadTableHandlers (db/version_builder.cc), which picks the files and the load limit, and
/// LoadTableHandlersHelper (db/version_util.cc), which opens them.
/// </summary>
internal static class TableHandlerLoader
{
    /// <summary>kInitialLoadLimit (VersionBuilder::Rep::LoadTableHandlers).</summary>
    private const int InitialLoadLimit = 16;

    /// <summary>
    /// VersionBuilder::Rep::LoadTableHandlers over <paramref name="files"/> (per level; the files the version adds):
    /// with a bounded table cache at most min(16, capacity / 4) readers on the initial load (DB::Open) and capacity / 4
    /// otherwise, counting the readers already cached, and nothing once that many are cached; files whose reader is
    /// pinned are skipped. The files are opened by <paramref name="maxThreads"/> threads (the calling thread and
    /// maxThreads - 1 more). Returns the first error, or null.
    /// </summary>
    public static Exception? LoadTableHandlers(TableCache tableCache, FileMetaData[][] files, int maxThreads,
        bool prefetchIndexAndFilterInCache, bool isInitialLoad)
    {
        int capacity = tableCache.StoreCapacity;
        bool alwaysLoad = capacity == int.MaxValue;
        int maxLoad = int.MaxValue;
        if (!alwaysLoad)
        {
            // If it is initial loading and not set to always loading all the files, we only load up to kInitialLoadLimit
            // files, to limit the time reopening the DB. If the table cache is not 1/4 full, the readers are kept open.
            int loadLimit = isInitialLoad ? Math.Min(InitialLoadLimit, capacity / 4) : capacity / 4;
            int usage = tableCache.StoreUsage;
            if (usage >= loadLimit) return null;
            maxLoad = loadLimit - usage;
        }

        // <file metadata, level>
        var filesMeta = new List<(FileMetaData File, int Level)>();
        for (int level = 0; level < files.Length && filesMeta.Count < maxLoad; level++)
        {
            foreach (FileMetaData f in files[level])
            {
                // If the file has been opened before, just skip it.
                if (!tableCache.IsPinned(f)) filesMeta.Add((f, level));
                if (filesMeta.Count >= maxLoad) break;
            }
        }
        return LoadTableHandlersHelper(filesMeta, tableCache, maxThreads, prefetchIndexAndFilterInCache);
    }

    /// <summary>
    /// Port of LoadTableHandlersHelper: <paramref name="maxThreads"/> workers (the calling thread and maxThreads - 1
    /// dedicated threads, as upstream's port::Thread) take the next file until the list is done, an error was seen or the
    /// table cache is full, and open it through <paramref name="tableCache"/> (TableCache::FindTable). The first error
    /// wins and is returned; the other workers stop at their next file.
    /// </summary>
    public static Exception? LoadTableHandlersHelper(IReadOnlyList<(FileMetaData File, int Level)> filesMeta, TableCache tableCache,
        int maxThreads, bool prefetchIndexAndFilterInCache)
    {
        var state = new LoadState(filesMeta, tableCache, prefetchIndexAndFilterInCache);
        Thread[]? threads = null;
        // There is no point in more threads than files (upstream starts them anyway; they exit at once).
        int extra = Math.Min(maxThreads, filesMeta.Count) - 1;
        if (extra > 0)
        {
            threads = new Thread[extra];
            for (int i = 0; i < threads.Length; i++)
            {
                threads[i] = new Thread(static s => ((LoadState)s!).Run()) { IsBackground = true, Name = "rocksdb:file-open" };
                threads[i].Start(state);
            }
        }
        state.Run();
        if (threads is not null)
        {
            foreach (Thread t in threads) t.Join();
        }
        return state.Error;
    }

    private sealed class LoadState(IReadOnlyList<(FileMetaData File, int Level)> filesMeta, TableCache tableCache, bool prefetch)
    {
        private int _nextFileMetaIdx = -1;
        private Exception? _error;
        // The read activity of the caller (ReadOptions::io_activity, e.g. kDBOpen), installed on every worker so their
        // reads are timed in the same histogram.
        private readonly int _readActivity = Monitoring.FileReadActivity.Current;

        public Exception? Error => Volatile.Read(ref _error);

        public void Run()
        {
            int previousActivity = Monitoring.FileReadActivity.Current;
            Monitoring.FileReadActivity.Exit(_readActivity);
            try
            {
                RunWorker();
            }
            finally
            {
                Monitoring.FileReadActivity.Exit(previousActivity);
            }
        }

        private void RunWorker()
        {
            while (true)
            {
                int fileIdx = Interlocked.Increment(ref _nextFileMetaIdx);
                if (Volatile.Read(ref _error) is not null) break;
                if (fileIdx >= filesMeta.Count) break;
                int capacity = tableCache.StoreCapacity;
                if (capacity != int.MaxValue && tableCache.StoreUsage >= capacity) break;

                (FileMetaData file, int level) = filesMeta[fileIdx];
                // The reader records its reads in the histogram of the file's level (internal_stats->GetFileReadHist(level)).
                file.LevelHint = level;
                TestHooks.Invoke("VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable");
                try
                {
                    using TableReaderLease lease = tableCache.GetReader(file, prefetch);
                }
                catch (Exception e)
                {
                    // has_error.compare_exchange_strong: the first error is returned. Every exception is caught here, not
                    // only status-like ones: it must not escape a worker thread; the caller rethrows what is not a status.
                    Interlocked.CompareExchange(ref _error, e, null);
                }
                TestHooks.Invoke("VersionBuilder::Rep::LoadTableHandlers::AfterFindTable");
            }
        }
    }

    /// <summary>
    /// Rethrows <paramref name="error"/> when it is not a status of a table open (a RocksDbException, IOException or
    /// InvalidDataException): a bug in a worker thread surfaces on the caller with its original stack.
    /// </summary>
    public static void ThrowIfNotStatus(Exception? error)
    {
        if (error is null or RocksDbException or IOException or InvalidDataException) return;
        ExceptionDispatchInfo.Capture(error).Throw();
    }
}
