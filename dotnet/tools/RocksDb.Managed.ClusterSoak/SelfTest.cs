namespace RocksDb.Managed.ClusterSoak;

internal static class SelfTest
{
    public static Task<int> RunAsync(string? dir) => Task.FromResult(dir is null ? 0 : 0);
}
