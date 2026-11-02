using RocksDb.Managed;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Exceptions, status codes and background-error recovery.</summary>
internal static class Errors
{
    public static async Task ErrorHandling(string path)
    {
        StatusCode openCode = StatusCode.Ok, readOnlyCode = StatusCode.Ok;

        // <snippet:error-handling>
        // Failures are RocksDbException; Code/SubCode mirror rocksdb::Status codes.
        string empty = Directory.CreateDirectory(Path.Combine(path, "empty")).FullName;
        try
        {
            using RocksDatabase missing = RocksDatabase.Open(empty, new DbOptions());   // CreateIfMissing = false
        }
        catch (RocksDbException e) when (e.Code == StatusCode.InvalidArgument)
        {
            openCode = e.Code;
            Console.WriteLine(e.Message);                    // "Invalid argument: ... does not exist (create_if_missing is false)"
        }

        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true })) db.Put("k"u8, "v"u8);
        using (RocksDatabase readOnly = RocksDatabase.OpenReadOnly(path, new DbOptions()))
        {
            try { readOnly.Put("k"u8, "v2"u8); }
            catch (RocksDbException e) when (e.Code == StatusCode.NotSupported) { readOnlyCode = e.Code; }
        }

        // Background errors (failed flush/compaction/WAL write) stop writes like in native RocksDB. Retryable I/O errors
        // are resumed automatically (DbOptions.MaxBgErrorResumeCount); otherwise fix the cause and call Resume.
        await using RocksDatabase live = await RocksDatabase.OpenAsync(path, new DbOptions());
        if (live.BackgroundError is RocksDbException bg)
        {
            Console.WriteLine($"{bg.Code} severity={bg.Severity} retryable={bg.Retryable}");
            await live.ResumeAsync();                         // throws if the error is still there
        }
        // </snippet:error-handling>
        Line($"open missing -> {openCode}, write to read-only -> {readOnlyCode}, background error: {live.BackgroundError?.Code.ToString() ?? "none"}");
        Check(openCode == StatusCode.InvalidArgument && readOnlyCode == StatusCode.NotSupported, "error codes");
    }
}
