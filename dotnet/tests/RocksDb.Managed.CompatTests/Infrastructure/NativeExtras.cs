using System.Runtime.InteropServices;

namespace RocksDb.Managed.CompatTests.Infrastructure;

/// <summary>
/// C API functions of the native librocksdb that rocksdb-sharp's <c>Native</c> class does not bind. They are resolved from
/// the library rocksdb-sharp already loaded (found through /proc/self/maps, so the same instance is used).
/// </summary>
internal static unsafe class NativeExtras
{
    private static readonly Lazy<IntPtr> s_library = new(LoadLibrary);

    private static IntPtr LoadLibrary()
    {
        _ = RocksDbSharp.Native.Instance; // make sure rocksdb-sharp loaded its library
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash < 0) continue;
            string path = line[slash..];
            if (Path.GetFileName(path).StartsWith("librocksdb", StringComparison.Ordinal)) return NativeLibrary.Load(path);
        }
        throw new InvalidOperationException("librocksdb is not loaded");
    }

    private static IntPtr Export(string name) => NativeLibrary.GetExport(s_library.Value, name);

    /// <summary>rocksdb_options_set_best_efforts_recovery.</summary>
    public static void SetBestEffortsRecovery(RocksDbSharp.DbOptions options, bool value) =>
        ((delegate* unmanaged<IntPtr, byte, void>)Export("rocksdb_options_set_best_efforts_recovery"))(options.Handle, value ? (byte)1 : (byte)0);
}
