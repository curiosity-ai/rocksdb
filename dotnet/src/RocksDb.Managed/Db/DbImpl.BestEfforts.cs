using System.Runtime.ExceptionServices;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

/// <summary>best_efforts_recovery support of DB open (DBImpl::Recover, db/db_impl/db_impl_open.cc).</summary>
internal sealed partial class DbImpl
{
    /// <summary>A non-empty MANIFEST exists in the DB directory (best-efforts recovery's "DB exists" test).</summary>
    private bool HasNonEmptyManifest()
    {
        IEnumerable<string> children;
        try { children = FileSystem.GetChildren(DbName); }
        catch (Exception e) when (e is IOException or RocksDbException) { return false; }
        foreach (string child in children)
        {
            if (FileNames.TryParseFileName(child, out ulong n, out FileType t) && t == FileType.DescriptorFile)
            {
                try
                {
                    if (FileSystem.GetFileSize(FileNames.DescriptorFileName(DbName, n)) != 0) return true;
                }
                catch (Exception e) when (e is IOException or RocksDbException) { }
            }
        }
        return false;
    }

    /// <summary>
    /// The file checks of best-efforts recovery (VersionEditHandlerPointInTime, db/version_edit_handler.cc), through the
    /// column families' table caches like upstream, so the readers opened while the MANIFEST is replayed stay cached for
    /// the DB afterwards: VerifyFile (VersionSet::VerifyFileMetadata, db/version_set.cc), LoadTables
    /// (VersionBuilder::LoadTableHandlers from MaybeCreateVersionBeforeApplyEdit, with max_file_opening_threads) and
    /// VerifyBlobFile.
    /// <para>
    /// Upstream creates each column family (with its TableCache) when the replay meets it (VersionEditHandler::
    /// CreateCfAndInit); here the column family objects are built after the replay, so this verifier creates the table
    /// cache view of each column family it opens (<see cref="ColumnFamilyData.NewTableCache"/>, from the same sanitized
    /// options) and hands it to the ColumnFamilyData (<see cref="TakeTableCache"/>). Column families the DB does not open
    /// (no descriptor; the persistent stats column family is opened implicitly) are not checked: upstream applies no
    /// edit to them (do_not_open_column_families_), and the open then fails with "Column families not opened".
    /// </para>
    /// </summary>
    private sealed class RecoveryFileVerifier : IRecoveryFileVerifier, IDisposable
    {
        private readonly DbImpl _db;
        private readonly Dictionary<string, ColumnFamilyOptions> _descriptors;
        // column family id -> (name, table cache view). Kept when a MANIFEST fails and an older one is tried (VersionSet::
        // TryRecover): upstream's Reset() drops the column families, but their readers stay in the shared table cache.
        private readonly Dictionary<uint, (string Name, TableCache Cache)> _tableCaches = new();

        public RecoveryFileVerifier(DbImpl db, Dictionary<string, ColumnFamilyOptions> descriptors)
        {
            _db = db;
            _descriptors = descriptors;
        }

        /// <summary>The table cache view of <paramref name="cf"/>, or null when the DB does not open it (OnColumnFamilyAdd).</summary>
        private TableCache? TableCacheFor(ColumnFamilyVersion cf)
        {
            bool known = _tableCaches.TryGetValue(cf.Id, out (string Name, TableCache Cache) entry);
            if (known && entry.Name == cf.Name) return entry.Cache;
            ColumnFamilyOptions? options = _descriptors.TryGetValue(cf.Name, out ColumnFamilyOptions? d) ? d
                : cf.Name == PersistentStatsColumnFamilyName ? OptimizeForPersistentStats(new ColumnFamilyOptions()) : null;
            if (options is null) return null;
            // Another MANIFEST gave this id to another column family.
            if (known) entry.Cache.Dispose();
            ColumnFamilyOptions o = _db.SanitizedCfOptions(options);
            TableCache cache = ColumnFamilyData.NewTableCache(_db,
                ColumnFamilyData.CreateReaderOptions(o, TableFactory.CreateFormatOptions(o.TableOptions), _db), Math.Max(1, o.NumLevels));
            _tableCaches[cf.Id] = (cf.Name, cache);
            return cache;
        }

        /// <summary>The table cache view used for column family <paramref name="id"/> named <paramref name="name"/>, which the caller now owns.</summary>
        public TableCache? TakeTableCache(uint id, string name)
        {
            if (!_tableCaches.TryGetValue(id, out (string Name, TableCache Cache) entry) || entry.Name != name) return null;
            _tableCaches.Remove(id);
            return entry.Cache;
        }

        /// <summary>Drops the views no column family took (dropped column families): their readers are closed.</summary>
        public void Dispose()
        {
            foreach ((_, TableCache cache) in _tableCaches.Values) cache.Dispose();
            _tableCaches.Clear();
        }

        /// <summary>
        /// The statuses that make a file missing (VersionBuilder::Rep::ApplyFileAddition / ApplyBlobFileAddition:
        /// IsPathNotFound, IsNotFound or IsCorruption); with <paramref name="notFound"/> false, LoadTables' (IsCorruption, or
        /// IsPathNotFound with no_error_if_files_missing_). A table that cannot be parsed is a Corruption.
        /// </summary>
        private static bool IsMissing(Exception e, bool notFound)
        {
            if (e is InvalidDataException) return true;
            if (e is not (IOException or RocksDbException)) return false;
            RocksDbException r = RocksDbException.FromIOException(null, null, e);
            return r.Code == StatusCode.Corruption || r.SubCode == StatusSubCode.PathNotFound || (notFound && r.Code == StatusCode.NotFound);
        }

        /// <summary>
        /// VersionEditHandlerPointInTime::VerifyFile: VersionSet::VerifyFileMetadata, which requires the file to have the
        /// size the MANIFEST records and, with verify_sst_unique_id_in_manifest, opens it through the table cache
        /// (FindTable, prefetch_index_and_filter_in_cache = false), so the reader stays cached. A missing, size-mismatched or
        /// corrupted (also: wrong unique id) file counts as missing; any other error fails the recovery from this MANIFEST.
        /// Deviation: upstream stats the file in cf_paths[0] whatever its path id (VersionBuilder::Rep::ApplyFileAddition,
        /// MakeTableFileName(cf_paths[0])), so a table in another db_path would always count as missing; the file's own
        /// path is used here, as FindTable does.
        /// </summary>
        public bool VerifyTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta)
        {
            TableCache? cache = TableCacheFor(cf);
            if (cache is null) return true;
            string path = cache.TableFilePath(meta);
            try
            {
                long size = _db.FileSystem.GetFileSize(path);
                if ((ulong)size != meta.FileSize)
                {
                    _db.LogWarn("Best-efforts recovery: File size mismatch: " + path);
                    return false;
                }
                if (!_db.Options.VerifySstUniqueIdInManifest) return true;
                meta.LevelHint = level;
                using TableReaderLease lease = cache.GetReader(meta, prefetchIndexAndFilterInCache: false);
                return true;
            }
            catch (Exception e) when (IsMissing(e, notFound: true))
            {
                _db.LogWarn("Best-efforts recovery: table file " + path + " is not usable: " + e.Message);
                return false;
            }
        }

        /// <summary>
        /// The VersionBuilder::LoadTableHandlers of VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit for the
        /// version about to be created: its files are opened into the table cache by max_file_opening_threads threads, as
        /// on an initial load (at most min(16, capacity / 4) readers with a bounded table cache, none once that many are
        /// cached), without prefetching index and filter (TableHandlerLoader). A Corruption or PathNotFound skips this
        /// point in time ("This point in time cannot be recovered; skip it and continue"); any other error fails the
        /// recovery from this MANIFEST.
        /// </summary>
        public bool LoadTables(ColumnFamilyVersion version)
        {
            TableCache? cache = TableCacheFor(version);
            if (cache is null) return true;
            var files = new FileMetaData[version.NumLevels][];
            for (int l = 0; l < files.Length; l++)
            {
                IReadOnlyList<FileMetaData> level = version.Levels[l];
                files[l] = new FileMetaData[level.Count];
                for (int i = 0; i < level.Count; i++) files[l][i] = level[i];
            }
            Exception? error = TableHandlerLoader.LoadTableHandlers(cache, files, _db.Options.MaxFileOpeningThreads,
                prefetchIndexAndFilterInCache: false, isInitialLoad: true);
            if (error is null) return true;
            TableHandlerLoader.ThrowIfNotStatus(error);
            if (!IsMissing(error, notFound: false)) ExceptionDispatchInfo.Capture(error).Throw();
            _db.LogWarn("Best-efforts recovery: skipping a point in time of column family " + version.Name + ", a table file cannot be opened: " + error.Message);
            return false;
        }

        /// <summary>
        /// VersionEditHandlerPointInTime::VerifyBlobFile: the blob file must open (header and footer). Missing or corrupted
        /// counts as missing; any other error fails the recovery from this MANIFEST. Deviation: upstream opens it through
        /// the column family's BlobSource (the blob file cache), whose reader then stays cached; here the reader is closed
        /// again, because the BlobSource is created with the column family after the replay.
        /// </summary>
        public bool VerifyBlobFile(ColumnFamilyVersion cf, BlobFileAddition addition)
        {
            if (TableCacheFor(cf) is null) return true;
            string path = FileNames.BlobFileName(_db.DbPaths[0], addition.BlobFileNumber);
            try
            {
                Blob.BlobFileReader.Open(_db.FileSystem, path, cf.Id, null, null).Dispose();
                return true;
            }
            catch (Exception e) when (IsMissing(e, notFound: true))
            {
                _db.LogWarn("Best-efforts recovery: blob file " + path + " is not usable: " + e.Message);
                return false;
            }
        }
    }
}
