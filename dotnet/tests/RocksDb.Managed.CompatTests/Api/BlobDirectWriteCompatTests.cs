using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Blob direct write (enable_blob_direct_write) across implementations. librocksdb 11.12 has the feature (C API:
/// rocksdb_options_set_enable_blob_direct_write / _blob_direct_write_partitions): DBs written with direct write by either
/// side (blob files registered by the shutdown flush, entities with blob-backed columns, several partitions) are read,
/// extended and compacted by the other, and native parses the managed OPTIONS file with the direct write options.
/// </summary>
[TestClass]
public sealed class BlobDirectWriteCompatTests
{
    private const ulong MinBlobSize = 48;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string Key(int i) => "k" + i.ToString("D5", CultureInfo.InvariantCulture);

    private static string Val(int i, string tag) =>
        i % 4 == 0 ? tag + "-s" + i.ToString(CultureInfo.InvariantCulture) : tag + "-" + i.ToString(CultureInfo.InvariantCulture) + new string((char)('a' + i % 26), 60 + i % 200);

    private static RocksDbSharp.RocksDb OpenNative(string path, bool directWrite)
    {
        RocksDbSharp.MergeOperator op = CreateConcatMergeOperator();
        RocksDbSharp.ColumnFamilyOptions o = CfOptions(op);
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_enable_blob_files(o.Handle, 1);
        n.rocksdb_options_set_min_blob_size(o.Handle, MinBlobSize);
        n.rocksdb_options_set_blob_file_size(o.Handle, 32 * 1024);
        n.rocksdb_options_set_enable_blob_direct_write(o.Handle, directWrite ? (byte)1 : (byte)0);
        n.rocksdb_options_set_blob_direct_write_partitions(o.Handle, 3);
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        n.rocksdb_options_set_allow_concurrent_memtable_write(options.Handle, 0);
        GC.KeepAlive(options);
        return RocksDbSharp.RocksDb.Open(options, path, new RocksDbSharp.ColumnFamilies(o));
    }

    private static RocksDatabase OpenManaged(string path, bool directWrite) =>
        RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, AllowConcurrentMemtableWrite = false },
        [
            new("default", new ColumnFamilyOptions
            {
                EnableBlobFiles = true,
                MinBlobSize = MinBlobSize,
                BlobFileSize = 32 * 1024,
                EnableBlobDirectWrite = directWrite,
                BlobDirectWritePartitions = 3,
                MergeOperator = new StringAppend(),
            }),
        ]);

    private static void Verify(Func<string, string?> get, Dictionary<string, string?> model, string phase)
    {
        foreach ((string k, string? v) in model) Assert.AreEqual(v, get(k), phase + " " + k);
    }

    private static int CountBlobFiles(string dir) => Directory.GetFiles(dir, "*.blob").Length;

    [TestMethod]
    public void NativeDirectWriteDbReadAndExtendedByManaged()
    {
        using var dir = new TempDir("bdw-n");
        var model = new Dictionary<string, string?>();
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, directWrite: true))
        {
            for (int i = 0; i < 300; i++)
            {
                db.Put(U(Key(i)), U(Val(i, "n1")));
                model[Key(i)] = Val(i, "n1");
            }
            // Written to blob files at write time (before any flush), one file per partition.
            Assert.AreEqual(3, CountBlobFiles(dir.Path));
            db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            for (int i = 0; i < 300; i += 3)
            {
                db.Put(U(Key(i)), U(Val(i, "n2")));
                model[Key(i)] = Val(i, "n2");
            }
            db.Remove(U(Key(7)));
            model[Key(7)] = null;
            // Entity with a large default column (direct-written as a V2 entity by native).
            using (WriteBatch b = new())
            {
                b.PutEntity(U("entity"), [new(U(""), U(new string('E', 100))), new(U("c"), U("small"))]);
                using var nb = new RocksDbSharp.WriteBatch(b.Data.ToArray());
                db.Write(nb);
            }
            model["entity"] = new string('E', 100);
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "native");
            // Close: native flushes to register the direct-write blob files.
        }
        Assert.IsTrue(CountBlobFiles(dir.Path) >= 3);
        using (RocksDatabase db = OpenManaged(dir.Path, directWrite: true))
        {
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "managed reads native");
            Assert.AreEqual("=" + new string('E', 100) + ";c=small;", Describe(db.GetEntity(U("entity"))!));
            for (int i = 300; i < 400; i++)
            {
                db.Put(U(Key(i)), U(Val(i, "m1")));
                model[Key(i)] = Val(i, "m1");
            }
            db.PutEntity(U("entity2"), [new(U(""), U(new string('F', 90))), new(U("z"), U(new string('G', 70)))]);
            model["entity2"] = new string('F', 90);
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "managed memtable");
            // Closed without an explicit flush: the shutdown flush registers the managed direct-write files.
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, directWrite: true))
        {
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "native reads managed");
            db.CompactRange((byte[]?)null, null);
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "native compacted");
        }
        using (RocksDatabase db = OpenManaged(dir.Path, directWrite: false))
        {
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "managed final");
            Assert.AreEqual("=" + new string('F', 90) + ";z=" + new string('G', 70) + ";", Describe(db.GetEntity(U("entity2"))!));
        }
    }

    [TestMethod]
    public void ManagedDirectWriteDbReadByNativeAndOptionsParsed()
    {
        using var dir = new TempDir("bdw-m");
        var model = new Dictionary<string, string?>();
        using (RocksDatabase db = OpenManaged(dir.Path, directWrite: true))
        {
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 200; i++)
                {
                    if ((i + round) % 2 != 0) continue;
                    db.Put(U(Key(i)), U(Val(i, "r" + round)));
                    model[Key(i)] = Val(i, "r" + round);
                }
                if (round < 2) db.Flush();
            }
            db.CompactRange();
            for (int i = 200; i < 260; i++)
            {
                db.Put(new WriteOptions { Sync = true }, U(Key(i)), U(Val(i, "sync")));
                model[Key(i)] = Val(i, "sync");
            }
        }
        // Native parses the managed OPTIONS file (enable_blob_direct_write, blob_direct_write_partitions).
        (bool enabled, uint partitions) = NativeLoadDirectWriteOptions(dir.Path);
        Assert.IsTrue(enabled);
        Assert.AreEqual(3U, partitions);
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, directWrite: true))
        {
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "native reads managed");
            using (RocksDbSharp.Iterator it = db.NewIterator())
            {
                int n = 0;
                for (it.SeekToFirst(); it.Valid(); it.Next())
                {
                    Assert.AreEqual(model[Encoding.UTF8.GetString(it.Key())], Encoding.UTF8.GetString(it.Value()));
                    n++;
                }
                Assert.AreEqual(model.Count, n);
            }
            for (int i = 0; i < 50; i++)
            {
                db.Put(U(Key(i)), U(Val(i, "nat")));
                model[Key(i)] = Val(i, "nat");
            }
        }
        using (RocksDatabase db = OpenManaged(dir.Path, directWrite: true))
        {
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, model, "managed reads native writes");
        }
    }

    /// <summary>rocksdb_load_latest_options (ignore_unknown_options = false): the direct write options of the default CF.</summary>
    private static (bool Enabled, uint Partitions) NativeLoadDirectWriteOptions(string dbPath)
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
            Assert.AreEqual(1, num);
            IntPtr cfOpts = Marshal.ReadIntPtr(Marshal.ReadIntPtr(cfOptsOut));
            bool enabled = native.rocksdb_options_get_enable_blob_direct_write(cfOpts) != 0;
            uint partitions = native.rocksdb_options_get_blob_direct_write_partitions(cfOpts);
            native.rocksdb_load_latest_options_destroy(Marshal.ReadIntPtr(dbOptsOut), Marshal.ReadIntPtr(namesOut), Marshal.ReadIntPtr(cfOptsOut), (UIntPtr)num);
            return (enabled, partitions);
        }
        finally
        {
            Marshal.FreeHGlobal(outs);
            Marshal.FreeHGlobal(path);
            native.rocksdb_cache_destroy(cache);
            native.rocksdb_env_destroy(env);
        }
    }

    private static string Describe(WideColumns c)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < c.Count; i++) sb.Append(Encoding.UTF8.GetString(c.GetName(i))).Append('=').Append(Encoding.UTF8.GetString(c.GetValue(i))).Append(';');
        return sb.ToString();
    }

    private sealed class StringAppend : MergeOperator
    {
        public override string Name => "StringAppendOperator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            System.Buffers.IBufferWriter<byte> result)
        {
            bool first = true;
            if (hasExistingValue)
            {
                System.Buffers.BuffersExtensions.Write(result, existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) System.Buffers.BuffersExtensions.Write(result, ","u8);
                System.Buffers.BuffersExtensions.Write(result, operands[i]);
                first = false;
            }
            return true;
        }
    }
}
