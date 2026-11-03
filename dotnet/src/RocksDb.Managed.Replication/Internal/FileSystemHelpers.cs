using System.Text;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Replication.Internal;

/// <summary>
/// Small helpers over <see cref="IFileSystem"/> used by the replication layer: crash-safe small state files
/// (temp file + fsync + rename, the <c>AtomicWrite</c> of rocksdb-sharp's RaftPersistentState), whole-file reads and
/// recursive deletes. Going through <see cref="IFileSystem"/> rather than System.IO lets every piece of persisted
/// replication state live on a <see cref="MemoryFileSystem"/> in tests.
/// </summary>
internal static class FileSystemHelpers
{
    public static string Combine(string directory, string name) =>
        directory.Length == 0 ? name : directory.EndsWith('/') || directory.EndsWith('\\') ? directory + name : directory + "/" + name;

    /// <summary>
    /// Writes <paramref name="content"/> to <paramref name="path"/> through a temp file that is fsync'd and then renamed
    /// over the target, and fsyncs the directory, so a crash leaves either the old or the new content - never a torn
    /// file - and a promise (term, vote, epoch) persisted here is never rolled back by power loss.
    /// </summary>
    public static void AtomicWrite(IFileSystem fs, string path, string content)
    {
        string tmp = path + ".tmp";
        WriteSynced(fs, tmp, content);
        fs.RenameFile(tmp, path);
        string? dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) fs.SyncDirectory(dir);
    }

    /// <summary>
    /// <see cref="AtomicWrite"/> of two files in the same directory with one directory fsync: both temp files are written
    /// and fsync'd, then renamed in order (<paramref name="firstPath"/> first), then the directory is fsync'd. A crash
    /// leaves each file old or new, and the first one new whenever the second is (renames in one directory are ordered).
    /// </summary>
    public static void AtomicWriteBoth(IFileSystem fs, string firstPath, string firstContent, string secondPath, string secondContent)
    {
        WriteSynced(fs, firstPath + ".tmp", firstContent);
        WriteSynced(fs, secondPath + ".tmp", secondContent);
        fs.RenameFile(firstPath + ".tmp", firstPath);
        fs.RenameFile(secondPath + ".tmp", secondPath);
        string? dir = Path.GetDirectoryName(firstPath);
        if (!string.IsNullOrEmpty(dir)) fs.SyncDirectory(dir);
        string? dir2 = Path.GetDirectoryName(secondPath);
        if (!string.IsNullOrEmpty(dir2) && dir2 != dir) fs.SyncDirectory(dir2);
    }

    private static void WriteSynced(IFileSystem fs, string path, string content)
    {
        using IWritableFile f = fs.CreateWritable(path, truncate: true);
        f.Append(Encoding.UTF8.GetBytes(content));
        f.Sync();
    }

    /// <summary>Reads a whole (small) file as UTF-8, or null if it does not exist.</summary>
    public static string? TryReadAllText(IFileSystem fs, string path)
    {
        if (!fs.FileExists(path)) return null;
        long size = fs.GetFileSize(path);
        byte[] buffer = new byte[size];
        using ISequentialFile f = fs.OpenSequential(path);
        int total = 0;
        while (total < buffer.Length)
        {
            int n = f.Read(buffer.AsSpan(total));
            if (n <= 0) break;
            total += n;
        }
        return Encoding.UTF8.GetString(buffer, 0, total);
    }

    public static bool DirectoryExists(IFileSystem fs, string path)
    {
        try
        {
            return fs.DirectoryExists(path);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.NotSupported)
        {
            try
            {
                fs.GetChildren(path);
                return true;
            }
            catch (RocksDbException)
            {
                return false;
            }
        }
    }

    /// <summary>Deletes a directory and everything below it (no-op if missing).</summary>
    public static void DeleteDirectoryRecursive(IFileSystem fs, string path)
    {
        if (!DirectoryExists(fs, path)) return;
        foreach (string child in fs.GetChildren(path))
        {
            string full = Combine(path, child);
            if (fs.FileExists(full)) fs.DeleteFile(full);
            else DeleteDirectoryRecursive(fs, full);
        }
        fs.DeleteDirectory(path);
    }
}
