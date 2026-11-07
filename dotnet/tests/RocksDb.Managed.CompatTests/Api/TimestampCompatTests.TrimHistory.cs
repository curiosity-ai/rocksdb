using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// DB::OpenAndTrimHistory: the same native-written history (files, range tombstones, WAL tail) trimmed by native
/// rocksdb_open_and_trim_history and by <see cref="RocksDatabase.OpenAndTrimHistory"/> reads identically afterwards.
/// </summary>
public sealed unsafe partial class TimestampCompatTests
{
    private static void CopyDbDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string f in Directory.GetFiles(from))
        {
            string name = Path.GetFileName(f);
            if (name == "LOCK") continue;
            File.Copy(f, Path.Combine(to, name));
        }
    }

    private static void NativeOpenAndTrimHistory(string path, ulong trimTs)
    {
        IntPtr lib = IntPtr.Zero;
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int slash = line.IndexOf('/', StringComparison.Ordinal);
            if (slash >= 0 && Path.GetFileName(line[slash..]).StartsWith("librocksdb", StringComparison.Ordinal))
            {
                lib = NativeLibrary.Load(line[slash..]);
                break;
            }
        }
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        N.rocksdb_options_set_comparator(cf.Handle, s_comparator.Value);
        cf.SetDisableAutoCompactions(1);
        var o = new RocksDbSharp.DbOptions();
        s_keepAlive.Add(cf);
        s_keepAlive.Add(o);
        IntPtr name = Marshal.StringToHGlobalAnsi("default");
        IntPtr dbPath = Marshal.StringToHGlobalAnsi(path);
        byte[] ts = Ts(trimTs);
        try
        {
            IntPtr* names = stackalloc IntPtr[1];
            names[0] = name;
            IntPtr* opts = stackalloc IntPtr[1];
            opts[0] = cf.Handle;
            IntPtr* handles = stackalloc IntPtr[1];
            IntPtr err = IntPtr.Zero;
            IntPtr db;
            fixed (byte* t = ts)
            {
                db = ((delegate* unmanaged<IntPtr, IntPtr, int, IntPtr*, IntPtr*, IntPtr*, byte*, nuint, IntPtr*, IntPtr>)
                    NativeLibrary.GetExport(lib, "rocksdb_open_and_trim_history"))(o.Handle, dbPath, 1, names, opts, handles, t, (nuint)ts.Length, &err);
            }
            Check(err);
            N.rocksdb_column_family_handle_destroy(handles[0]);
            N.rocksdb_close(db);
        }
        finally
        {
            Marshal.FreeHGlobal(name);
            Marshal.FreeHGlobal(dbPath);
        }
    }

    [TestMethod]
    public void OpenAndTrimHistoryMatchesNative()
    {
        using var dir = new TempDir("udt-trim");
        string src = Path.Combine(dir.Path, "src"), nativeTrim = Path.Combine(dir.Path, "native"), managedTrim = Path.Combine(dir.Path, "managed");
        const int Keys = 80;
        using (RocksDbSharp.RocksDb native = OpenNative(src))
        {
            WriteHistory(Keys, 0, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            WriteRangeDeletions(Keys, 10, (a, b, t) => NativeDeleteRange(native, a, b, t));
            WriteHistory(Keys, 30, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            // Left in the WAL: flushed and trimmed at open.
            WriteHistory(Keys, 60, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
        }
        CopyDbDirectory(src, nativeTrim);
        CopyDbDirectory(src, managedTrim);
        const ulong TrimTs = 45;
        NativeOpenAndTrimHistory(nativeTrim, TrimTs);
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true };
        using (RocksDatabase m = RocksDatabase.OpenAndTrimHistory(managedTrim, new DbOptions(), [new ColumnFamilyDescriptor("default", cfo)], Ts(TrimTs)))
        {
        }
        List<string> expected, actual;
        using (RocksDbSharp.RocksDb n = OpenNative(nativeTrim)) expected = NativeDump(n, Keys, 0);
        using (RocksDbSharp.RocksDb n = OpenNative(managedTrim)) actual = NativeDump(n, Keys, 0);
        // Nothing newer than the trim timestamp survived (the entries' timestamps follow the last '@').
        foreach (string l in expected)
        {
            if (l.EndsWith(" -", StringComparison.Ordinal)) continue;
            int at = l.LastIndexOf('@');
            string ts = l[(at + 1)..].Replace("(del)", "", StringComparison.Ordinal);
            Assert.IsTrue(ulong.Parse(ts, System.Globalization.CultureInfo.InvariantCulture) <= TrimTs, l);
        }
        AssertSameDump(expected, actual, "trimmed at 45");
    }
}
