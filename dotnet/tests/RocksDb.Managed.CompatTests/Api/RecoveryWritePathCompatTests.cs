using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.CompatTests.Wal;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Recovery and write-path behaviour checked against native 11.12: avoid_flush_during_recovery with a memtable that
/// fills up during WAL replay (db_impl_open.cc MaybeFlushFinalMemtableOrRestoreActiveLogFiles), and the
/// disallow_memtable_writes column family option (OPTIONS text, write rejection).
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class RecoveryWritePathCompatTests
{
    private const int Merges = 3000;
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static int Operands(byte[]? value) => value is null ? 0 : Encoding.UTF8.GetString(value).Split(',').Length;

    private static RocksDbSharp.RocksDb OpenNative(string path, ulong writeBufferSize, bool avoidFlushDuringRecovery)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        RocksDbSharp.Native.Instance.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, avoidFlushDuringRecovery ? (byte)1 : (byte)0);
        RocksDbSharp.ColumnFamilyOptions cf = NativeDbHelper.CfOptions(NativeDbHelper.CreateConcatMergeOperator());
        cf.SetWriteBufferSize(writeBufferSize);
        return RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies(cf));
    }

    private static RocksDatabase OpenManaged(string path, ulong writeBufferSize, bool avoidFlushDuringRecovery) =>
        RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, AvoidFlushDuringRecovery = avoidFlushDuringRecovery },
            new ColumnFamilyOptions { MergeOperator = new StringAppendOperator(), WriteBufferSize = writeBufferSize });

    /// <summary>
    /// <paramref name="nativeWriter"/> writes <see cref="Merges"/> merge operands into the WAL (64 MiB memtable); the
    /// recovering engine reopens twice with a 64 KiB memtable and avoid_flush_during_recovery, so the replay fills a
    /// memtable and flushes mid-recovery; then the other engine opens the DB. Every open must see exactly
    /// <see cref="Merges"/> operands: a recovery that flushed must also flush the rest and advance the log numbers, or
    /// the next open replays the WAL again on top of the flushed operands.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void AvoidFlushDuringRecoveryWithFullMemtableIsNotReplayedTwice(bool nativeWriter)
    {
        using var dir = new TempDir("avoid-flush-replay");
        const ulong Big = 64UL << 20, Small = 64UL << 10;
        if (nativeWriter)
        {
            using RocksDbSharp.RocksDb db = OpenNative(dir.Path, Big, false);
            for (int i = 0; i < Merges; i++) db.Merge(U("k"), U("o" + i));
        }
        else
        {
            using RocksDatabase db = OpenManaged(dir.Path, Big, false);
            for (int i = 0; i < Merges; i++) db.Merge(U("k"), U("o" + i));
        }
        for (int open = 0; open < 2; open++)
        {
            if (nativeWriter)
            {
                using RocksDbSharp.RocksDb db = OpenNative(dir.Path, Small, true);
                Assert.AreEqual(Merges, Operands(db.Get(U("k"))), "native recovery, open " + open);
            }
            else
            {
                using RocksDatabase db = OpenManaged(dir.Path, Small, true);
                Assert.AreEqual(Merges, Operands(db.Get(U("k"))), "managed recovery, open " + open);
            }
        }
        // The other engine opens what the recovering engine left.
        if (nativeWriter)
        {
            using RocksDatabase db = OpenManaged(dir.Path, Small, true);
            Assert.AreEqual(Merges, Operands(db.Get(U("k"))), "managed after native recovery");
        }
        else
        {
            using RocksDbSharp.RocksDb db = OpenNative(dir.Path, Small, true);
            Assert.AreEqual(Merges, Operands(db.Get(U("k"))), "native after managed recovery");
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, Big, false)) Assert.AreEqual(Merges, Operands(db.Get(U("k"))), "native, final");
        using (RocksDatabase db = OpenManaged(dir.Path, Big, false)) Assert.AreEqual(Merges, Operands(db.Get(U("k"))), "managed, final");
    }

    /// <summary>
    /// disallow_memtable_writes: native writes the option to its OPTIONS file and managed parses it (and vice versa,
    /// rocksdb_load_latest_options); both engines reject a write to such a column family with the same InvalidArgument
    /// message, and both refuse to set it on the default column family.
    /// </summary>
    [TestMethod]
    public void DisallowMemtableWritesAgreesWithNative()
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        using var nativeDir = new TempDir("disallow-native");
        string nativeMessage;
        {
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
            var ing = new RocksDbSharp.ColumnFamilyOptions();
            native.rocksdb_options_set_disallow_memtable_writes(ing.Handle, 1);
            var cfs = new RocksDbSharp.ColumnFamilies(new RocksDbSharp.ColumnFamilyOptions()) { { "ing", ing } };
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(o, nativeDir.Path, cfs);
            db.Put("a", "1");
            var e = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => db.Put(U("b"), U("2"), db.GetColumnFamily("ing")));
            nativeMessage = e.Message;
        }
        StringAssert.Contains(nativeMessage, "This column family has disallow_memtable_writes=true");
        CollectionAssert.AreEquivalent(new[] { "default=False", "ing=True" }, Flatten(NativeLoadDisallow(nativeDir.Path)));

        // Managed reads native's OPTIONS file.
        OptionsFile parsed = OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, nativeDir.Path)!, preserveUnknownOptions: true);
        Assert.IsFalse(parsed.ColumnFamilies.Single(c => c.Name == "ing").Options.UnknownOptions.ContainsKey("disallow_memtable_writes"));
        Assert.IsFalse(parsed.ColumnFamilies.Single(c => c.Name == "default").Options.DisallowMemtableWrites);
        Assert.IsTrue(parsed.ColumnFamilies.Single(c => c.Name == "ing").Options.DisallowMemtableWrites);

        // Managed opens the native DB with the option and rejects writes with the same message.
        using (RocksDatabase db = RocksDatabase.Open(nativeDir.Path, new DbOptions(),
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions()), new ColumnFamilyDescriptor("ing", new ColumnFamilyOptions { DisallowMemtableWrites = true })]))
        {
            Assert.AreEqual("1", Encoding.UTF8.GetString(db.Get(U("a"))!));
            var e = Assert.ThrowsExactly<RocksDbException>(() => db.GetColumnFamily("ing").Put(U("b"), U("2")));
            Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
            Assert.AreEqual(nativeMessage, e.Message);
        }
        // The managed OPTIONS file is read back by native.
        CollectionAssert.AreEquivalent(new[] { "default=False", "ing=True" }, Flatten(NativeLoadDisallow(nativeDir.Path)));

        // Neither engine allows it on the default column family.
        using var defDir = new TempDir("disallow-default");
        var nd = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        native.rocksdb_options_set_disallow_memtable_writes(nd.Handle, 1);
        var ne = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => RocksDbSharp.RocksDb.Open(nd, defDir.Path).Dispose());
        var me = Assert.ThrowsExactly<RocksDbException>(() =>
            RocksDatabase.Open(defDir.Path, new DbOptions { CreateIfMissing = true }, new ColumnFamilyOptions { DisallowMemtableWrites = true }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, me.Code);
        Assert.AreEqual(ne.Message, me.Message);
    }

    private static string[] Flatten(Dictionary<string, bool> d) => [.. d.Select(kv => kv.Key + "=" + kv.Value)];

    /// <summary>rocksdb_load_latest_options: disallow_memtable_writes of every column family.</summary>
    private static Dictionary<string, bool> NativeLoadDisallow(string dbPath)
    {
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr path = Marshal.StringToHGlobalAnsi(dbPath);
        IntPtr env = native.rocksdb_create_default_env();
        IntPtr cache = native.rocksdb_cache_create_lru((UIntPtr)(1 << 20));
        IntPtr outs = Marshal.AllocHGlobal(IntPtr.Size * 4);
        try
        {
            for (int i = 0; i < 4; i++) Marshal.WriteIntPtr(outs, i * IntPtr.Size, IntPtr.Zero);
            IntPtr dbOptsOut = outs, numOut = outs + IntPtr.Size, namesOut = outs + 2 * IntPtr.Size, cfOptsOut = outs + 3 * IntPtr.Size;
            native.rocksdb_load_latest_options(path, env, 0, cache, dbOptsOut, numOut, namesOut, cfOptsOut);
            int num = (int)Marshal.ReadIntPtr(numOut);
            IntPtr dbOpts = Marshal.ReadIntPtr(dbOptsOut);
            Assert.AreNotEqual(IntPtr.Zero, dbOpts);
            IntPtr names = Marshal.ReadIntPtr(namesOut), cfOpts = Marshal.ReadIntPtr(cfOptsOut);
            var result = new Dictionary<string, bool>(StringComparer.Ordinal);
            for (int i = 0; i < num; i++)
            {
                string name = Marshal.PtrToStringAnsi(Marshal.ReadIntPtr(names, i * IntPtr.Size))!;
                result[name] = native.rocksdb_options_get_disallow_memtable_writes(Marshal.ReadIntPtr(cfOpts, i * IntPtr.Size)) != 0;
            }
            native.rocksdb_load_latest_options_destroy(dbOpts, names, cfOpts, (UIntPtr)num);
            return result;
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            native.rocksdb_cache_destroy(cache);
            native.rocksdb_env_destroy(env);
        }
    }
}
