namespace RocksDb.Managed.CompatTests.Infrastructure;

/// <summary>Disposable temporary directory for a test database.</summary>
public sealed class TempDir : IDisposable
{
    public string Path { get; }

    public TempDir(string? prefix = null)
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rocksdb-managed-" + (prefix ?? "t") + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public void Dispose()
    {
        try { Directory.Delete(Path, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
