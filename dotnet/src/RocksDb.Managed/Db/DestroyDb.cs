using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db;

/// <summary>
/// Port of rocksdb::DestroyDB (db/db_impl/db_impl.cc) with DeleteUnaccountedDBFile / DeleteExistingTrashDBFile
/// (file/file_util.cc): under the DB LOCK, deletes every file of the DB directory that ParseFileName recognises
/// (tables, blobs, WALs, MANIFEST, CURRENT, IDENTITY, OPTIONS, info logs, temp files; METADB-N recursively), leftover
/// trash files, the table / blob files of the extra db_paths, the archived WALs and the WALs of a separate wal_dir,
/// then the LOCK file and the (then empty) directories. Table, blob and WAL files (and trash) go through the
/// SstFileManager when there is one (rate-limited trash deletion), and DestroyDB waits until that trash is gone.
/// </summary>
/// <remarks>
/// Managed differences: the SstFileManager has no per-call trash bucket, so the wait is for the manager's whole
/// trash queue (WaitForEmptyTrash) rather than only this call's files; column family cf_paths are not ported, so
/// only <see cref="DbOptions.DbPaths"/> are searched. Errors of individual deletions are collected and the first one
/// is thrown after the rest of the DB was removed, like the first non-OK status upstream returns.
/// </remarks>
internal static class DestroyDb
{
    public static void Run(string dbname, DbOptions options)
    {
        dbname = dbname.TrimEnd('/');
        if (dbname.Length == 0) dbname = "/";
        IFileSystem fs = options.FileSystem;
        SstFileManager? sfm = options.SstFileManager;
        string walDir = string.IsNullOrEmpty(options.WalDir) ? dbname : options.WalDir.TrimEnd('/');
        bool walInDbPath = walDir == dbname;
        // SanitizeOptions creates the info logger (CreateLoggerFromOptions), which creates the DB directory when there is
        // no db_log_dir; upstream then resets the logger so it does not hold the LOG file. The managed port does not
        // open a logger, but creates the directory the same way, so destroying a missing DB succeeds like upstream.
        if (string.IsNullOrEmpty(options.DbLogDir) && options.InfoLog is null)
        {
            try
            {
                fs.CreateDirectoryIfMissing(dbname);
            }
            catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
            {
                // The LOCK below reports the problem.
            }
        }
        var pathsToDelete = new SortedSet<string>(StringComparer.Ordinal);
        string lockname = FileNames.LockFileName(dbname);
        RocksDbException? result = null;
        IDisposable fileLock = fs.LockFile(lockname); // a failure to lock (e.g. the DB is open) is the result
        try
        {
            string infoLogPrefix = FileNames.InfoLogPrefix(!string.IsNullOrEmpty(options.DbLogDir), Path.GetFullPath(dbname));
            foreach (string fname in Children(fs, dbname))
            {
                if (IsTrashFile(fname))
                {
                    Delete(fs, sfm, dbname + "/" + fname, dbname, forceForeground: false, ref result);
                    continue;
                }
                if (FileNames.TryParseFileName(fname, infoLogPrefix, out _, out FileType type, out _) && type != FileType.DbLockFile)
                {
                    // Lock file will be deleted at end
                    string pathToDelete = dbname + "/" + fname;
                    if (type == FileType.MetaDatabase)
                    {
                        Capture(() => Run(pathToDelete, options), ref result);
                    }
                    else if (type is FileType.TableFile or FileType.WalFile or FileType.BlobFile)
                    {
                        Delete(fs, sfm, pathToDelete, dbname, forceForeground: false, ref result);
                    }
                    else
                    {
                        Capture(() => fs.DeleteFile(pathToDelete), ref result);
                    }
                }
            }
            pathsToDelete.Add(dbname);

            var paths = new SortedSet<string>(StringComparer.Ordinal);
            foreach ((string p, _) in options.DbPaths) paths.Add(p.TrimEnd('/'));
            foreach (string path in paths)
            {
                if (!TryChildren(fs, path, out IEnumerable<string> names)) continue;
                foreach (string fname in names)
                {
                    if (IsTrashFile(fname))
                    {
                        Delete(fs, sfm, path + "/" + fname, path, forceForeground: false, ref result);
                    }
                    else if (FileNames.TryParseFileName(fname, out _, out FileType type) && type is FileType.TableFile or FileType.BlobFile)
                    {
                        Delete(fs, sfm, path + "/" + fname, dbname, forceForeground: false, ref result);
                    }
                }
            }
            pathsToDelete.UnionWith(paths);

            // Archive dir may be inside wal dir or dbname and should be processed and removed before those otherwise
            // we have issues removing them.
            string archiveDir = FileNames.ArchivalDirectory(walDir);
            bool walDirExists = false;
            IEnumerable<string> walDirFiles = [];
            if (!walInDbPath) walDirExists = TryChildren(fs, walDir, out walDirFiles);
            if (TryChildren(fs, archiveDir, out IEnumerable<string> archiveFiles))
            {
                foreach (string file in archiveFiles)
                {
                    if (IsTrashFile(file))
                    {
                        Delete(fs, sfm, archiveDir + "/" + file, archiveDir, forceForeground: !walInDbPath, ref result);
                    }
                    else if (FileNames.TryParseFileName(file, out _, out FileType type) && type == FileType.WalFile)
                    {
                        Delete(fs, sfm, archiveDir + "/" + file, archiveDir, forceForeground: !walInDbPath, ref result);
                    }
                }
                pathsToDelete.Add(archiveDir);
            }

            // Delete log files in the WAL dir
            if (walDirExists)
            {
                foreach (string file in walDirFiles)
                {
                    if (IsTrashFile(file))
                    {
                        Delete(fs, sfm, walDir + "/" + file, walDir, forceForeground: !walInDbPath, ref result);
                    }
                    else if (FileNames.TryParseFileName(file, out ulong number, out FileType type) && type == FileType.WalFile)
                    {
                        Delete(fs, sfm, FileNames.LogFileName(walDir, number), walDir, forceForeground: !walInDbPath, ref result);
                    }
                }
                pathsToDelete.Add(walDir);
            }
        }
        finally
        {
            // Ignore error since state is already gone
            fileLock.Dispose();
        }
        try
        {
            fs.DeleteFile(lockname);
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
        }

        // Make sure trash files are all cleared before return.
        sfm?.WaitForEmptyTrash();

        // Ignore error in case dir contains other files. Upstream iterates a std::set<std::string>, i.e. in byte order,
        // so a parent ("db") comes before its "db/archive" and stays behind (empty) when the archive existed; kept as is
        // so a destroyed directory looks the same as after native DestroyDB.
        foreach (string path in pathsToDelete)
        {
            try
            {
                fs.DeleteDirectory(path);
            }
            catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
            {
            }
        }
        if (result is not null) throw result;
    }

    /// <summary>DeleteUnaccountedDBFile / DeleteExistingTrashDBFile: through the SstFileManager unless forced to the foreground.</summary>
    private static void Delete(IFileSystem fs, SstFileManager? sfm, string path, string dirToSync, bool forceForeground, ref RocksDbException? result)
    {
        if (sfm is not null && !forceForeground)
        {
            sfm.ScheduleFileDeletion(path, dirToSync, forceBackground: false);
            return;
        }
        Capture(() => fs.DeleteFile(path), ref result);
    }

    private static void Capture(Action action, ref RocksDbException? result)
    {
        try
        {
            action();
        }
        catch (RocksDbException e)
        {
            result ??= e;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            result ??= RocksDbException.FromException(e);
        }
    }

    /// <summary>DeleteScheduler::IsTrashFile (file/delete_scheduler.cc): the ".trash" suffix.</summary>
    private static bool IsTrashFile(string fname) => fname.EndsWith(".trash", StringComparison.Ordinal);

    /// <summary>GetChildren with the error ignored (in case the directory does not exist).</summary>
    private static IEnumerable<string> Children(IFileSystem fs, string dir) => TryChildren(fs, dir, out IEnumerable<string> names) ? names : [];

    private static bool TryChildren(IFileSystem fs, string dir, out IEnumerable<string> names)
    {
        try
        {
            names = [.. fs.GetChildren(dir)];
            return true;
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
            names = [];
            return false;
        }
    }
}
