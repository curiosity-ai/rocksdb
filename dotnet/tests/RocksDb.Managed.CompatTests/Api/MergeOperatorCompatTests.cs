using System.Buffers;
using System.Runtime.InteropServices;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// The built-in merge operators (utilities/merge_operators/*) against native RocksDB. The NuGet librocksdb creates them
/// by name through its object registry (rocksdb_get_options_from_string with "merge_operator=max" etc., i.e.
/// MergeOperator::CreateFromString): for every id, native writes merge operands (memtable/WAL and SST), managed opens the
/// directory with the operator recreated from native's OPTIONS file, reads the same merged values, adds operands and
/// compacts; native reopens and reads managed's results, and both OPTIONS files name the operator identically.
/// AggMergeOperator is not registered by name upstream (and has no C API), so it has no native counterpart here.
/// </summary>
[TestClass]
public sealed unsafe class MergeOperatorCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();
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

    /// <summary>rocksdb_get_options_from_string(base, text, new); returns the error message or null.</summary>
    private static string? NativeOptionsFromString(RocksDbSharp.DbOptions baseOptions, string text, RocksDbSharp.DbOptions newOptions)
    {
        IntPtr err = IntPtr.Zero;
        byte[] t = System.Text.Encoding.UTF8.GetBytes(text + "\0");
        fixed (byte* p = t)
        {
            ((delegate* unmanaged<IntPtr, byte*, IntPtr, IntPtr*, void>)Export("rocksdb_get_options_from_string"))(baseOptions.Handle, p, newOptions.Handle, &err);
        }
        if (err == IntPtr.Zero) return null;
        string msg = Marshal.PtrToStringUTF8(err)!;
        RocksDbSharp.Native.Instance.rocksdb_free(err);
        return msg;
    }

    /// <summary>Native options with create_if_missing and the merge operator created from <paramref name="id"/>.</summary>
    private static RocksDbSharp.DbOptions NativeOptions(string id)
    {
        var baseOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        var options = new RocksDbSharp.DbOptions();
        s_keepAlive.Add(baseOptions);
        s_keepAlive.Add(options);
        string? err = NativeOptionsFromString(baseOptions, "merge_operator=" + id, options);
        Assert.IsNull(err, "native merge_operator=" + id + ": " + err);
        return options;
    }

    private static OptionsFile Latest(string dir) =>
        OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir)!, preserveUnknownOptions: true);

    /// <summary>Id given to native, managed operator name, OPTIONS text both sides write.</summary>
    public static IEnumerable<object[]> Operators()
    {
        yield return ["put", "PutOperator", "PutOperator"];
        yield return ["put_v1", "PutOperator", "PutOperator"];
        yield return ["PutOperator", "PutOperator", "PutOperator"];
        yield return ["max", "MaxOperator", "MaxOperator"];
        yield return ["MaxOperator", "MaxOperator", "MaxOperator"];
        yield return ["uint64add", "UInt64AddOperator", "UInt64AddOperator"];
        yield return ["bytesxor", "BytesXOR", "BytesXOR"];
        yield return ["BytesXOR", "BytesXOR", "BytesXOR"];
        yield return ["stringappend", "StringAppendOperator", "{id=StringAppendOperator;delimiter=,;}"];
        yield return ["{id=stringappend;delimiter=<>;}", "StringAppendOperator", "{id=StringAppendOperator;delimiter=<>;}"];
        yield return ["stringappendtest", "StringAppendTESTOperator", "{id=StringAppendTESTOperator;delimiter=,;}"];
        yield return ["{id=StringAppendTESTOperator;delimiter=|;}", "StringAppendTESTOperator", "{id=StringAppendTESTOperator;delimiter=|;}"];
    }

    private const int Keys = 24;

    private static byte[] Key(int i) => System.Text.Encoding.UTF8.GetBytes("key" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));

    private static byte[] Operand(string name, Random rnd)
    {
        switch (name)
        {
            case "UInt64AddOperator":
            {
                byte[] b = new byte[8];
                Util.Coding.EncodeFixed64(b, (ulong)rnd.Next(0, 1 << 20));
                return b;
            }
            default:
            {
                byte[] b = new byte[rnd.Next(0, 10)];
                rnd.NextBytes(b);
                return b;
            }
        }
    }

    private static string? Hex(byte[]? b) => b is null ? null : Convert.ToHexString(b);

    [TestMethod]
    [DynamicData(nameof(Operators))]
    public void NativeAndManagedMergeIdentically(string id, string name, string optionsText)
    {
        MergeOperator managedOp = MergeOperator.CreateBuiltin(id)!;
        Assert.IsNotNull(managedOp, id);
        Assert.AreEqual(name, managedOp.Name);
        int seed = Environment.TickCount;
        string trace = id + " seed=" + seed;
        var rnd = new Random(seed);
        using var dir = new TempDir("mergeop");

        // 1. Native writes operands (some flushed to SST, the rest in the WAL), puts and deletes.
        var nativeValues = new string?[Keys];
        RocksDbSharp.DbOptions nativeOptions = NativeOptions(id);
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(nativeOptions, dir.Path))
        {
            for (int step = 0; step < 300; step++)
            {
                byte[] key = Key(rnd.Next(Keys));
                int a = rnd.Next(100);
                if (a < 80) db.Merge(key, Operand(name, rnd));
                else if (a < 92) db.Put(key, Operand(name, rnd));
                else db.Remove(key);
                if (step % 70 == 69) db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            for (int i = 0; i < Keys; i++) nativeValues[i] = Hex(db.Get(Key(i)));
        }
        Assert.AreEqual(optionsText, Latest(dir.Path).FindColumnFamily("default")!.GetOption("merge_operator"), trace);

        // 2. Managed opens with the operator recreated from native's OPTIONS file and reads the same values.
        ColumnFamilyOptions cfo = Latest(dir.Path).FindColumnFamily("default")!;
        Assert.IsNotNull(cfo.MergeOperator, trace);
        // put_v1 is persisted as "PutOperator" and reloads as PutOperatorV2 on both sides (same results).
        Assert.AreEqual(managedOp is PutOperator ? typeof(PutOperatorV2) : managedOp.GetType(), cfo.MergeOperator.GetType(), trace);
        var managedValues = new string?[Keys];
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cfo))
        {
            for (int i = 0; i < Keys; i++) Assert.AreEqual(nativeValues[i], Hex(db.Get(Key(i))), trace + " key " + i);
            // Managed adds operands and compacts native's operands together with its own.
            for (int step = 0; step < 200; step++)
            {
                byte[] key = Key(rnd.Next(Keys));
                if (rnd.Next(100) < 85) db.Merge(key, Operand(name, rnd));
                else db.Put(key, Operand(name, rnd));
                if (step == 100) db.Flush();
            }
            for (int i = 0; i < Keys; i++) managedValues[i] = Hex(db.Get(Key(i)));
            db.Flush();
            db.CompactRange();
            for (int i = 0; i < Keys; i++) Assert.AreEqual(managedValues[i], Hex(db.Get(Key(i))), trace + " compacted key " + i);
            // Unflushed operands stay in the WAL for native.
            for (int i = 0; i < Keys; i += 3) db.Merge(Key(i), Operand(name, rnd));
            for (int i = 0; i < Keys; i++) managedValues[i] = Hex(db.Get(Key(i)));
        }
        Assert.AreEqual(optionsText, Latest(dir.Path).FindColumnFamily("default")!.GetOption("merge_operator"), trace);

        // 3. Native reopens (also from managed's OPTIONS file) and reads managed's results, then compacts.
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(optionsText.StartsWith('{') ? optionsText : id), dir.Path))
        {
            for (int i = 0; i < Keys; i++) Assert.AreEqual(managedValues[i], Hex(db.Get(Key(i))), trace + " native key " + i);
            db.CompactRange((byte[]?)null, null);
            for (int i = 0; i < Keys; i++) Assert.AreEqual(managedValues[i], Hex(db.Get(Key(i))), trace + " native compacted key " + i);
        }
        Assert.AreEqual(optionsText, NativeLoadMergeOperatorText(dir.Path), trace);
    }

    /// <summary>
    /// The merge operator text native writes when it loads managed's OPTIONS file (rocksdb_load_latest_options) and
    /// writes it back on open: proves native resolves the name managed persisted.
    /// </summary>
    private static string? NativeLoadMergeOperatorText(string dir)
    {
        IntPtr env = RocksDbSharp.Native.Instance.rocksdb_create_default_env();
        IntPtr dbOptions, names, cfOptions, err = IntPtr.Zero;
        nuint numCfs;
        byte[] path = System.Text.Encoding.UTF8.GetBytes(dir + "\0");
        fixed (byte* p = path)
        {
            ((delegate* unmanaged<byte*, IntPtr, byte, IntPtr, IntPtr*, nuint*, IntPtr*, IntPtr*, IntPtr*, void>)Export("rocksdb_load_latest_options"))(
                p, env, 0, IntPtr.Zero, &dbOptions, &numCfs, &names, &cfOptions, &err);
        }
        if (err != IntPtr.Zero) Assert.Fail("rocksdb_load_latest_options: " + Marshal.PtrToStringUTF8(err));
        try
        {
            // Open with the loaded (default CF) options: native recreates the operator from the file, writes a new
            // OPTIONS file and must still be able to read merges.
            IntPtr cfOpts = Marshal.ReadIntPtr(cfOptions);
            IntPtr dbErr = IntPtr.Zero;
            IntPtr db;
            fixed (byte* p = path)
            {
                db = ((delegate* unmanaged<IntPtr, byte*, IntPtr*, IntPtr>)Export("rocksdb_open"))(cfOpts, p, &dbErr);
            }
            if (dbErr != IntPtr.Zero) Assert.Fail("rocksdb_open: " + Marshal.PtrToStringUTF8(dbErr));
            ((delegate* unmanaged<IntPtr, void>)Export("rocksdb_close"))(db);
        }
        finally
        {
            ((delegate* unmanaged<IntPtr, IntPtr, IntPtr, nuint, void>)Export("rocksdb_load_latest_options_destroy"))(dbOptions, names, cfOptions, numCfs);
            RocksDbSharp.Native.Instance.rocksdb_env_destroy(env);
        }
        return Latest(dir).FindColumnFamily("default")!.GetOption("merge_operator");
    }

    /// <summary>
    /// SortList ("sortlist" / "MergeSortOperator"): names and OPTIONS text agree in both directions, and managed merges
    /// operands of a DB native created with the operator. Merged values are not compared with native: upstream's
    /// SortList::MakeVector scans each operand up to a NUL terminator, which slices of memtable / block memory lack
    /// (it reads past the operand), and its PartialMergeMulti (run by native flushes) replaces the operands by an
    /// empty one; so native is only asked to resolve the operator and to read plain values.
    /// </summary>
    [TestMethod]
    [DataRow("sortlist")]
    [DataRow("MergeSortOperator")]
    public void SortListNamesAgreeWithNative(string id)
    {
        using var dir = new TempDir("sortlist");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(id), dir.Path))
        {
            db.Put(Key(0), "5,9"u8.ToArray());
            db.Put(Key(1), "1"u8.ToArray());
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            CollectionAssert.AreEqual("5,9"u8.ToArray(), db.Get(Key(0)));
        }
        ColumnFamilyOptions cfo = Latest(dir.Path).FindColumnFamily("default")!;
        Assert.AreEqual("MergeSortOperator", cfo.GetOption("merge_operator"));
        Assert.IsInstanceOfType<SortListOperator>(cfo.MergeOperator);
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions(), cfo))
        {
            Assert.AreEqual("1", System.Text.Encoding.UTF8.GetString(db.Get(Key(1))!));
            db.Merge(Key(2), "3,8"u8);
            db.Flush();
            db.Merge(Key(2), "-1,4"u8);
            db.Merge(Key(2), "9"u8);
            Assert.AreEqual("-1,3,4,8,9", System.Text.Encoding.UTF8.GetString(db.Get(Key(2))!));
            db.Flush();
            Assert.AreEqual("-1,3,4,8,9", System.Text.Encoding.UTF8.GetString(db.Get(Key(2))!));
        }
        Assert.AreEqual("MergeSortOperator", Latest(dir.Path).FindColumnFamily("default")!.GetOption("merge_operator"));
        Assert.AreEqual("MergeSortOperator", NativeLoadMergeOperatorText(dir.Path));
    }

    /// <summary>
    /// Ids native's registry does not know: GetOptionsFromString (ignore_unsupported_options) leaves the merge operator
    /// unset, so its OPTIONS file says "nullptr"; managed's resolver returns null for them too. AggMergeOperator.v1 is
    /// one of them natively (upstream does not register it); managed resolves it as an extension.
    /// </summary>
    [TestMethod]
    [DataRow("NoSuchOperator")]
    [DataRow("AggMergeOperator.v1")]
    public void UnregisteredIdsLeaveNativeWithoutOperator(string id)
    {
        using var dir = new TempDir("mergeop-unknown");
        using (RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(id), dir.Path)) db.Put(Key(0), new byte[] { 1 });
        Assert.AreEqual("nullptr", Latest(dir.Path).FindColumnFamily("default")!.GetOption("merge_operator"));
        Assert.AreEqual(id == "NoSuchOperator", MergeOperator.CreateBuiltin(id) is null);
    }
}
