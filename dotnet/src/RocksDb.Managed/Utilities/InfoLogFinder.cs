using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed;

/// <summary>
/// Lists the info LOG files of a database. Port of GetInfoLogList (include/rocksdb/utilities/info_log_finder.h,
/// utilities/convenience/info_log_finder.cc) and GetInfoLogFiles (file/filename.cc).
/// </summary>
public static class InfoLogFinder
{
    /// <summary>
    /// The names (not paths) of the info log files of <paramref name="db"/>: <c>LOG</c>, <c>LOG.old</c> and
    /// <c>LOG.old.&lt;micros&gt;</c> in the DB directory, or, when <see cref="DbOptions.DbLogDir"/> is set, the files
    /// named after the DB path (<c>&lt;escaped absolute db path&gt;_LOG[.old[.&lt;micros&gt;]]</c>) in that directory.
    /// The names are relative to <see cref="GetInfoLogDirectory"/>; their order is the file system's.
    /// </summary>
    /// <remarks>
    /// With a log directory the prefix is computed from the absolute DB path, as the logger names the files
    /// (CreateLoggerFromOptions); upstream passes DB::GetName() as given, which only differs for relative paths.
    /// </remarks>
    public static List<string> GetInfoLogList(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        DbImpl impl = db.DefaultColumnFamily.Db;
        impl.ThrowIfDisposed();
        return GetInfoLogFiles(impl.FileSystem, impl.Options.DbLogDir, AbsolutePath(impl.DbName), out _);
    }

    /// <summary>The directory holding the info log files of <paramref name="db"/> (db_log_dir, else the DB directory).</summary>
    public static string GetInfoLogDirectory(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        DbImpl impl = db.DefaultColumnFamily.Db;
        string logDir = impl.Options.DbLogDir.TrimEnd('/');
        return logDir.Length != 0 ? logDir : impl.DbName;
    }

    /// <summary>
    /// GetInfoLogFiles: the info log files of the database <paramref name="dbname"/> in <paramref name="dbLogDir"/>
    /// (empty = the database directory), which <paramref name="parentDirectory"/> receives. With a log directory,
    /// <paramref name="dbname"/> should be the absolute DB path the logger used. Throws when the directory cannot be
    /// listed.
    /// </summary>
    public static List<string> GetInfoLogFiles(IFileSystem fileSystem, string dbLogDir, string dbname, out string parentDirectory)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(dbLogDir);
        ArgumentNullException.ThrowIfNull(dbname);
        string logDir = dbLogDir.TrimEnd('/');
        parentDirectory = logDir.Length != 0 ? logDir : dbname;
        string prefix = FileNames.InfoLogPrefix(logDir.Length != 0, dbname);
        var result = new List<string>();
        foreach (string f in fileSystem.GetChildren(parentDirectory))
        {
            if (FileNames.TryParseFileName(f, prefix, out _, out FileType type, out _) && type == FileType.InfoLogFile) result.Add(f);
        }
        return result;
    }

    private static string AbsolutePath(string dbname)
    {
        try
        {
            return System.IO.Path.GetFullPath(dbname);
        }
        catch (Exception e) when (e is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return dbname;
        }
    }
}
