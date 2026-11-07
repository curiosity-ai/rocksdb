using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.TestShared;

/// <summary>
/// Makes the system <c>libstdc++.so.6</c> globally visible before the native librocksdb (RocksDB NuGet package) is
/// loaded into a .NET 11 process on Linux.
/// </summary>
/// <remarks>
/// <para>The packaged <c>librocksdb.so</c> links libstdc++ statically but still exports its <c>std::</c> symbols. Its
/// iostream code only works when those exports are interposed consistently: either all bound to its own copy or all
/// to a <c>libstdc++.so.6</c> in the global symbol scope. The .NET 10 apphost (and the <c>dotnet</c> muxer) depend
/// on <c>libstdc++.so.6</c>, so it is global and everything binds to it. The .NET 11 apphost no longer does: the
/// system library is only loaded locally (as a dependency of the runtime), librocksdb's bindings end up mixed, and
/// every numeric insertion into a <c>std::ostream</c> throws <c>std::bad_cast</c> inside librocksdb (use_facet
/// fails). The visible effects: BackupEngine writes empty <c>meta/&lt;id&gt;</c> files (StoreToFile formats the meta
/// with an ostringstream that silently goes bad), and EventLogger / stats lines in the native LOG lose their
/// numbers. Running the same test under the <c>dotnet</c> muxer, or under .NET 10, works.</para>
/// <para>Promoting <c>libstdc++.so.6</c> to RTLD_GLOBAL before librocksdb is loaded restores the .NET 10 behaviour.
/// This is a property of the host process, not of the managed port; applications hosting the native library in a
/// .NET 11 apphost need the same workaround (or a librocksdb built with <c>-Wl,--exclude-libs,ALL</c>).</para>
/// </remarks>
internal static class NativeStdCxxPreload
{
    private const int RtldNow = 0x2;
    private const int RtldGlobal = 0x100;

    [DllImport("libc", EntryPoint = "dlopen")]
    private static extern IntPtr DlOpenLibc(string file, int mode);

    [DllImport("libdl.so.2", EntryPoint = "dlopen")]
    private static extern IntPtr DlOpenLibdl(string file, int mode);

#pragma warning disable CA2255 // test/benchmark executables: must run before any test touches the native library
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void Initialize()
    {
        if (!OperatingSystem.IsLinux()) return;
        try
        {
            _ = DlOpenLibc("libstdc++.so.6", RtldNow | RtldGlobal);
        }
        catch (Exception e) when (e is EntryPointNotFoundException or DllNotFoundException)
        {
            try
            {
                _ = DlOpenLibdl("libstdc++.so.6", RtldNow | RtldGlobal);
            }
            catch (Exception e2) when (e2 is EntryPointNotFoundException or DllNotFoundException)
            {
                // No dynamic loader API (static musl); nothing to fix.
            }
        }
    }
}
