using Microsoft.Testing.Platform.Builder;
using RocksDb.Managed.RoundTrip.Harness;

namespace RocksDb.Managed.RoundTrip;

/// <summary>
/// Entry point of the round-trip test executable. Normally it hands control to the Microsoft Testing Platform (MSTest
/// runner). When started with <see cref="ChildHost.ChildArgument"/> as the first argument it instead runs as a
/// <em>child</em>: it opens a database with the requested engine (native or managed), executes an op script and exits.
/// The tests launch this child mode (see <see cref="ChildLauncher"/>) so crashes and hangs are isolated and captured.
/// </summary>
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == ChildHost.ChildArgument) return ChildHost.Run(args);

        ITestApplicationBuilder builder = await TestApplication.CreateBuilderAsync(args);
        SelfRegisteredExtensions.AddSelfRegisteredExtensions(builder, args);
        using ITestApplication app = await builder.BuildAsync();
        return await app.RunAsync();
    }
}
