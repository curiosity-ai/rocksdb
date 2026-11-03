using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace RocksDb.Managed.CompatTests.Infrastructure;

/// <summary>
/// C API functions of native librocksdb 11.12 for remote compaction and column family export/import that rocksdb-sharp
/// does not bind (rocksdb_compactionservice_*, rocksdb_open_and_compact, rocksdb_checkpoint_export_column_family,
/// rocksdb_create_column_family_with_import, rocksdb_export_import_files_metadata_*, rocksdb_livefile_*).
/// </summary>
internal static unsafe class NativeRemoteCompaction
{
    private static readonly Lazy<IntPtr> s_library = new(LoadLibrary);

    private static IntPtr LoadLibrary()
    {
        _ = RocksDbSharp.Native.Instance;
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

    private static void Check(IntPtr err)
    {
        if (err == IntPtr.Zero) return;
        string msg = Marshal.PtrToStringUTF8(err)!;
        ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_free"))(err);
        throw new InvalidOperationException("native: " + msg);
    }

    // ---- compaction service ----------------------------------------------------------------------------

    /// <summary>What a native primary's compaction service does with each job (managed callback).</summary>
    public interface IService
    {
        /// <summary>Schedule: returns (status, job id) for the serialized input.</summary>
        (int Status, string JobId) Schedule(byte[] input, int outputLevel);
        /// <summary>Wait: returns (status, serialized result).</summary>
        (int Status, byte[]? Result) Wait(string jobId);
        void OnInstallation(string jobId, int status);
    }

    /// <summary>Creates a rocksdb_compactionservice_t forwarding to <paramref name="service"/> and sets it on the options.</summary>
    public static void SetCompactionService(RocksDbSharp.DbOptions options, IService service)
    {
        GCHandle h = GCHandle.Alloc(service);
        IntPtr name = Marshal.StringToCoTaskMemUTF8("ManagedTestService");
        IntPtr cs = ((delegate* unmanaged<IntPtr, delegate* unmanaged<IntPtr, void>, delegate* unmanaged<IntPtr, IntPtr, IntPtr, nuint, IntPtr>, IntPtr,
            delegate* unmanaged<IntPtr, IntPtr, IntPtr*, nuint*, int>, delegate* unmanaged<IntPtr, void>, delegate* unmanaged<IntPtr, IntPtr, int, void>, IntPtr>)
            Export("rocksdb_compactionservice_create"))(GCHandle.ToIntPtr(h), &Destroy, &Schedule, name, &Wait, &Cancel, &OnInstalled);
        ((delegate* unmanaged<IntPtr, IntPtr, void>)Export("rocksdb_options_set_compaction_service"))(options.Handle, cs);
    }

    [UnmanagedCallersOnly]
    private static void Destroy(IntPtr state) => GCHandle.FromIntPtr(state).Free();

    [UnmanagedCallersOnly]
    private static IntPtr Schedule(IntPtr state, IntPtr info, IntPtr input, nuint len)
    {
        var svc = (IService)GCHandle.FromIntPtr(state).Target!;
        byte[] data = new ReadOnlySpan<byte>((void*)input, (int)len).ToArray();
        int outputLevel = ((delegate* unmanaged<IntPtr, int>)Export("rocksdb_compactionservice_jobinfo_t_get_output_level"))(info);
        (int status, string jobId) = svc.Schedule(data, outputLevel);
        IntPtr id = Marshal.StringToCoTaskMemUTF8(jobId);
        IntPtr err = IntPtr.Zero;
        IntPtr resp = ((delegate* unmanaged<IntPtr, int, IntPtr*, IntPtr>)Export("rocksdb_compactionservice_scheduleresponse_create"))(id, status, &err);
        Marshal.FreeCoTaskMem(id);
        return resp;
    }

    [UnmanagedCallersOnly]
    private static int Wait(IntPtr state, IntPtr jobId, IntPtr* result, nuint* resultLen)
    {
        var svc = (IService)GCHandle.FromIntPtr(state).Target!;
        (int status, byte[]? r) = svc.Wait(Marshal.PtrToStringUTF8(jobId)!);
        if (r is not null)
        {
            // Freed by native with free().
            IntPtr p = (IntPtr)NativeMemory.Alloc((nuint)Math.Max(1, r.Length));
            r.CopyTo(new Span<byte>((void*)p, r.Length));
            *result = p;
            *resultLen = (nuint)r.Length;
        }
        return status;
    }

    [UnmanagedCallersOnly]
    private static void Cancel(IntPtr state)
    {
    }

    [UnmanagedCallersOnly]
    private static void OnInstalled(IntPtr state, IntPtr jobId, int status)
    {
        var svc = (IService)GCHandle.FromIntPtr(state).Target!;
        svc.OnInstallation(Marshal.PtrToStringUTF8(jobId)!, status);
    }

    /// <summary>
    /// rocksdb_open_and_compact with an override built from <paramref name="options"/> (comparator, merge operator,
    /// table factory, ...). Returns the serialized result or throws with the native error.
    /// </summary>
    public static byte[] OpenAndCompact(string dbPath, string outputDirectory, byte[] input, RocksDbSharp.DbOptions options)
    {
        IntPtr ov = ((delegate* unmanaged<IntPtr, IntPtr>)Export("rocksdb_compaction_service_options_override_create_from_options"))(options.Handle);
        try
        {
            IntPtr db = Marshal.StringToCoTaskMemUTF8(dbPath), od = Marshal.StringToCoTaskMemUTF8(outputDirectory);
            try
            {
                IntPtr err = IntPtr.Zero;
                nuint len = 0;
                IntPtr r;
                fixed (byte* p = input)
                {
                    r = ((delegate* unmanaged<IntPtr, IntPtr, byte*, nuint, nuint*, IntPtr, IntPtr*, IntPtr>)Export("rocksdb_open_and_compact"))(
                        db, od, p, (nuint)input.Length, &len, ov, &err);
                }
                Check(err);
                byte[] result = new ReadOnlySpan<byte>((void*)r, (int)len).ToArray();
                ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_free"))(r);
                return result;
            }
            finally
            {
                Marshal.FreeCoTaskMem(db);
                Marshal.FreeCoTaskMem(od);
            }
        }
        finally
        {
            ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_compaction_service_options_override_destroy"))(ov);
        }
    }

    // ---- export / import ------------------------------------------------------------------------------

    /// <summary>A native export: comparator name plus the files (name, directory, level, size, user keys, seqnos).</summary>
    public sealed record NativeFile(string Name, string Directory, int Level, ulong Size, byte[] SmallestKey, byte[] LargestKey, ulong SmallestSeqno,
        ulong LargestSeqno);

    /// <summary>rocksdb_checkpoint_export_column_family, converted to managed records.</summary>
    public static (string Comparator, List<NativeFile> Files) ExportColumnFamily(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyHandle cf, string exportDir)
    {
        IntPtr err = IntPtr.Zero;
        IntPtr cp = ((delegate* unmanaged<IntPtr, IntPtr*, IntPtr>)Export("rocksdb_checkpoint_object_create"))(db.Handle, &err);
        Check(err);
        IntPtr dir = Marshal.StringToCoTaskMemUTF8(exportDir);
        try
        {
            IntPtr meta = ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, IntPtr*, IntPtr>)Export("rocksdb_checkpoint_export_column_family"))(cp, cf.Handle, dir, &err);
            Check(err);
            try
            {
                IntPtr cmp = ((delegate* unmanaged<IntPtr, IntPtr>)Export("rocksdb_export_import_files_metadata_get_db_comparator_name"))(meta);
                string comparator = Marshal.PtrToStringUTF8(cmp)!;
                ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_free"))(cmp);
                IntPtr lf = ((delegate* unmanaged<IntPtr, IntPtr>)Export("rocksdb_export_import_files_metadata_get_files"))(meta);
                var files = new List<NativeFile>();
                RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
                int count = n.rocksdb_livefiles_count(lf);
                for (int i = 0; i < count; i++)
                {
                    IntPtr sk = n.rocksdb_livefiles_smallestkey(lf, i, out UIntPtr sl);
                    IntPtr lk = n.rocksdb_livefiles_largestkey(lf, i, out UIntPtr ll);
                    files.Add(new NativeFile(Marshal.PtrToStringUTF8(n.rocksdb_livefiles_name(lf, i))!,
                        Marshal.PtrToStringUTF8(((delegate* unmanaged<IntPtr, int, IntPtr>)Export("rocksdb_livefiles_directory"))(lf, i))!,
                        n.rocksdb_livefiles_level(lf, i), (ulong)n.rocksdb_livefiles_size(lf, i),
                        new ReadOnlySpan<byte>((void*)sk, (int)sl).ToArray(), new ReadOnlySpan<byte>((void*)lk, (int)ll).ToArray(),
                        n.rocksdb_livefiles_smallest_seqno(lf, i), n.rocksdb_livefiles_largest_seqno(lf, i)));
                }
                n.rocksdb_livefiles_destroy(lf);
                return (comparator, files);
            }
            finally
            {
                ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_export_import_files_metadata_destroy"))(meta);
            }
        }
        finally
        {
            Marshal.FreeCoTaskMem(dir);
            ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_checkpoint_object_destroy"))(cp);
        }
    }

    /// <summary>rocksdb_create_column_family_with_import from managed export metadata; returns the native handle pointer.</summary>
    public static IntPtr CreateColumnFamilyWithImport(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyOptions cfOptions, string name, bool moveFiles,
        ExportImportFilesMetadata metadata)
    {
        IntPtr meta = ((delegate* unmanaged<IntPtr>)Export("rocksdb_export_import_files_metadata_create"))();
        IntPtr io = ((delegate* unmanaged<IntPtr>)Export("rocksdb_import_column_family_options_create"))();
        IntPtr cname = Marshal.StringToCoTaskMemUTF8(metadata.DbComparatorName), cfName = Marshal.StringToCoTaskMemUTF8(name);
        try
        {
            ((delegate* unmanaged<IntPtr, IntPtr, void>)Export("rocksdb_export_import_files_metadata_set_db_comparator_name"))(meta, cname);
            ((delegate* unmanaged<IntPtr, byte, void>)Export("rocksdb_import_column_family_options_set_move_files"))(io, moveFiles ? (byte)1 : (byte)0);
            IntPtr lf = ((delegate* unmanaged<IntPtr>)Export("rocksdb_livefiles_create"))();
            foreach (LiveFileMetadata f in metadata.Files)
            {
                IntPtr one = ((delegate* unmanaged<IntPtr>)Export("rocksdb_livefile_create"))();
                SetString("rocksdb_livefile_set_column_family_name", one, f.ColumnFamilyName);
                SetString("rocksdb_livefile_set_name", one, f.Name);
                SetString("rocksdb_livefile_set_directory", one, f.Directory);
                ((delegate* unmanaged<IntPtr, int, void>)Export("rocksdb_livefile_set_level"))(one, f.Level);
                ((delegate* unmanaged<IntPtr, nuint, void>)Export("rocksdb_livefile_set_size"))(one, (nuint)f.Size);
                fixed (byte* s = f.SmallestKey) ((delegate* unmanaged<IntPtr, byte*, nuint, void>)Export("rocksdb_livefile_set_smallest_key"))(one, s, (nuint)f.SmallestKey.Length);
                fixed (byte* l = f.LargestKey) ((delegate* unmanaged<IntPtr, byte*, nuint, void>)Export("rocksdb_livefile_set_largest_key"))(one, l, (nuint)f.LargestKey.Length);
                ((delegate* unmanaged<IntPtr, ulong, void>)Export("rocksdb_livefile_set_smallest_seqno"))(one, f.SmallestSeqno);
                ((delegate* unmanaged<IntPtr, ulong, void>)Export("rocksdb_livefile_set_largest_seqno"))(one, f.LargestSeqno);
                ((delegate* unmanaged<IntPtr, IntPtr, void>)Export("rocksdb_livefiles_add"))(lf, one);
            }
            ((delegate* unmanaged<IntPtr, IntPtr, void>)Export("rocksdb_export_import_files_metadata_set_files"))(meta, lf);
            IntPtr err = IntPtr.Zero;
            IntPtr h = ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr, IntPtr*, IntPtr>)Export("rocksdb_create_column_family_with_import"))(
                db.Handle, cfOptions.Handle, cfName, io, meta, &err);
            Check(err);
            return h;
        }
        finally
        {
            Marshal.FreeCoTaskMem(cname);
            Marshal.FreeCoTaskMem(cfName);
            ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_import_column_family_options_destroy"))(io);
            ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_export_import_files_metadata_destroy"))(meta);
        }
    }

    private static void SetString(string fn, IntPtr target, string value)
    {
        IntPtr p = Marshal.StringToCoTaskMemUTF8(value);
        try
        {
            ((delegate* unmanaged<IntPtr, IntPtr, void>)Export(fn))(target, p);
        }
        finally
        {
            Marshal.FreeCoTaskMem(p);
        }
    }

    /// <summary>rocksdb_get_cf on a raw handle pointer (UTF-8 key and value).</summary>
    public static string? Get(RocksDbSharp.RocksDb db, IntPtr cf, string key)
    {
        byte[] k = Encoding.UTF8.GetBytes(key);
        IntPtr ro = RocksDbSharp.Native.Instance.rocksdb_readoptions_create();
        try
        {
            IntPtr err = IntPtr.Zero;
            nuint len = 0;
            IntPtr v;
            fixed (byte* kp = k)
            {
                v = ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, byte*, nuint, nuint*, IntPtr*, IntPtr>)Export("rocksdb_get_cf"))(db.Handle, ro, cf, kp, (nuint)k.Length,
                    &len, &err);
            }
            Check(err);
            if (v == IntPtr.Zero) return null;
            string s = Encoding.UTF8.GetString(new ReadOnlySpan<byte>((void*)v, (int)len));
            ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_free"))(v);
            return s;
        }
        finally
        {
            RocksDbSharp.Native.Instance.rocksdb_readoptions_destroy(ro);
        }
    }
}
