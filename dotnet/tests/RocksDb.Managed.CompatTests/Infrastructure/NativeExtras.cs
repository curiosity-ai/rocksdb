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

    /// <summary>rocksdb_file_checksum_gen_crc32c_factory_create + rocksdb_options_set_file_checksum_gen_factory (the factory is leaked).</summary>
    public static void SetCrc32cFileChecksums(RocksDbSharp.DbOptions options)
    {
        IntPtr factory = ((delegate* unmanaged<IntPtr>)Export("rocksdb_file_checksum_gen_crc32c_factory_create"))();
        ((delegate* unmanaged<IntPtr, IntPtr, void>)Export("rocksdb_options_set_file_checksum_gen_factory"))(options.Handle, factory);
    }

    /// <summary>rocksdb_verify_file_checksums: null when OK, else the error message.</summary>
    public static string? VerifyFileChecksums(RocksDbSharp.RocksDb db)
    {
        IntPtr err = IntPtr.Zero;
        ((delegate* unmanaged<IntPtr, IntPtr*, void>)Export("rocksdb_verify_file_checksums"))(db.Handle, &err);
        if (err == IntPtr.Zero) return null;
        string msg = Marshal.PtrToStringUTF8(err)!;
        ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_free"))(err);
        return msg;
    }

    /// <summary>rocksdb_options_set_best_efforts_recovery.</summary>
    public static void SetBestEffortsRecovery(RocksDbSharp.DbOptions options, bool value) =>
        ((delegate* unmanaged<IntPtr, byte, void>)Export("rocksdb_options_set_best_efforts_recovery"))(options.Handle, value ? (byte)1 : (byte)0);
}
