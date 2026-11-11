using RocksDb.Managed.Env;
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
    /// Port of VersionSet::VerifyFileMetadata (db/version_set.cc) and VersionEditHandlerPointInTime::VerifyBlobFile
    /// (db/version_edit_handler.cc): a table file must exist with the recorded size and, with
    /// verify_sst_unique_id_in_manifest, open (footer, meta blocks); a blob file must open (header and footer).
    /// </summary>
    private sealed class RecoveryFileVerifier : IRecoveryFileVerifier
    {
        private readonly DbImpl _db;
        private readonly Dictionary<string, ColumnFamilyOptions> _descriptors;
        private readonly Dictionary<string, TableReaderOptions> _readerOptions = new(StringComparer.Ordinal);

        public RecoveryFileVerifier(DbImpl db, Dictionary<string, ColumnFamilyOptions> descriptors)
        {
            _db = db;
            _descriptors = descriptors;
        }

        // Table files already opened successfully ("If the file has been opened before, just skip it",
        // VersionBuilder::Rep::LoadTableHandlers).
        private readonly HashSet<ulong> _loaded = new();

        public bool VerifyTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta)
        {
            string path = FileNames.TableFileName(_db.DbPaths, meta.FileNumber, meta.PathId);
            try
            {
                if (!_db.FileSystem.FileExists(path) || (ulong)_db.FileSystem.GetFileSize(path) != meta.FileSize) return false;
                if (!_db.Options.VerifySstUniqueIdInManifest) return true;
                OpenTable(cf, meta, path);
                return true;
            }
            catch (Exception e) when (e is IOException or RocksDbException or InvalidDataException)
            {
                _db.LogWarn("Best-efforts recovery: table file " + path + " is not usable: " + e.Message);
                return false;
            }
        }

        /// <summary>
        /// The table open of VersionBuilder::LoadTableHandlers for a point-in-time version: Corruption skips the version
        /// (VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit, "This point in time cannot be recovered;
        /// skip it and continue"), any other error fails the recovery. Upstream loads at most 16 files here when
        /// max_open_files is finite; all files are opened here.
        /// </summary>
        public bool LoadTableFile(ColumnFamilyVersion cf, int level, FileMetaData meta)
        {
            if (_loaded.Contains(meta.FileNumber)) return true;
            string path = FileNames.TableFileName(_db.DbPaths, meta.FileNumber, meta.PathId);
            try
            {
                OpenTable(cf, meta, path);
                return true;
            }
            catch (Exception e) when (e is InvalidDataException || (e is RocksDbException r && r.Code == StatusCode.Corruption))
            {
                _db.LogWarn("Best-efforts recovery: skipping a point in time, table file " + path + " is corrupt: " + e.Message);
                return false;
            }
        }

        private void OpenTable(ColumnFamilyVersion cf, FileMetaData meta, string path)
        {
            if (!_readerOptions.TryGetValue(cf.Name, out TableReaderOptions? opts))
            {
                ColumnFamilyOptions o = _descriptors.TryGetValue(cf.Name, out ColumnFamilyOptions? d) ? d : new ColumnFamilyOptions();
                opts = TableFactory.CreateReaderOptions(o, TableFactory.CreateFormatOptions(o.TableOptions));
                _readerOptions[cf.Name] = opts;
            }
            opts = opts with { LargestSeqno = meta.LargestSeqno, FileNumber = meta.FileNumber };
            IRandomAccessFile file = _db.FileSystem.OpenRandomAccess(path);
            try
            {
                TableReaderFactory.Open(file, opts with { AllowMmapReads = _db.Options.AllowMmapReads }).Dispose();
            }
            catch
            {
                file.Dispose();
                throw;
            }
            _loaded.Add(meta.FileNumber);
        }

        public bool VerifyBlobFile(ColumnFamilyVersion cf, BlobFileAddition addition)
        {
            string path = FileNames.BlobFileName(_db.DbPaths[0], addition.BlobFileNumber);
            try
            {
                if (!_db.FileSystem.FileExists(path)) return false;
                Blob.BlobFileReader.Open(_db.FileSystem, path, cf.Id, null, null).Dispose();
                return true;
            }
            catch (Exception e) when (e is IOException or RocksDbException or InvalidDataException)
            {
                _db.LogWarn("Best-efforts recovery: blob file " + path + " is not usable: " + e.Message);
                return false;
            }
        }
    }
}
