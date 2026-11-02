using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.CompatTests.Wal.NativeDbHelper;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Whole-DB on-disk compatibility between native RocksDB (rocksdb-sharp) and <see cref="RocksDatabase"/>: a DB
/// directory written by either (WAL-only data, flushed L0 tables, column families, deletes, range deletes, merges)
/// is opened, read, extended and handed back to the other.
/// </summary>
[TestClass]
public sealed class DbCompatTests
{
    private static readonly string[] s_cfs = ["default", .. ExtraColumnFamilies]; // default, one, two

    /// <summary>Expected contents: (cf, key) -> value (null = must not exist).</summary>
    private sealed class Model
    {
        public readonly Dictionary<(string Cf, string Key), string?> Data = new();

        public void Put(string cf, string k, string v) => Data[(cf, k)] = v;
        public void Delete(string cf, string k) => Data[(cf, k)] = null;

        public void Merge(string cf, string k, string v) =>
            Data[(cf, k)] = Data.TryGetValue((cf, k), out string? old) && old is not null ? old + "," + v : v;

        public void DeleteRange(string cf, string begin, string end)
        {
            foreach ((string c, string key) in new List<(string, string)>(Data.Keys))
            {
                if (c == cf && string.CompareOrdinal(key, begin) >= 0 && string.CompareOrdinal(key, end) < 0) Data[(c, key)] = null;
            }
        }
    }

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static string Val(int i, string tag) => tag + "-" + i + "-" + new string((char)('a' + i % 26), i % 37);

    // ---------------------------------------------------------------------------------------------------
    // workloads

    /// <summary>Native workload: puts/deletes/merges/range deletes in all CFs, flushing in the middle so data ends up in L0 + WAL.</summary>
    private static void NativeWorkload(RocksDbSharp.RocksDb db, Model m, string tag, int n)
    {
        var flush = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
        for (int round = 0; round < 2; round++)
        {
            foreach (string cf in s_cfs)
            {
                RocksDbSharp.ColumnFamilyHandle h = db.GetColumnFamily(cf);
                for (int i = 0; i < n; i++)
                {
                    string k = "k" + i.ToString("D4");
                    switch ((i + round) % 5)
                    {
                        case 0:
                        case 1:
                            db.Put(U(k), U(Val(i, tag + round)), h);
                            m.Put(cf, k, Val(i, tag + round));
                            break;
                        case 2:
                            db.Remove(U(k), h);
                            m.Delete(cf, k);
                            break;
                        default:
                            db.Merge(U(k), U(tag + round + "m" + i), h);
                            m.Merge(cf, k, tag + round + "m" + i);
                            break;
                    }
                }
                using (var b = new RocksDbSharp.WriteBatch())
                {
                    byte[] begin = U("k" + (n / 3).ToString("D4")), end = U("k" + (n / 3 + 7).ToString("D4"));
                    b.DeleteRange(begin, (ulong)begin.Length, end, (ulong)end.Length, h);
                    db.Write(b);
                    m.DeleteRange(cf, "k" + (n / 3).ToString("D4"), "k" + (n / 3 + 7).ToString("D4"));
                }
            }
            if (round == 0)
            {
                // First round goes to L0 tables (default via DB::Flush, "one" via FlushCF); "two" stays in the WAL.
                db.Flush(flush);
                RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, flush.Handle, db.GetColumnFamily("one").Handle);
            }
        }
        GC.KeepAlive(flush);
    }

    private static async Task ManagedWorkload(RocksDatabase db, Model m, string tag, int n, bool flushSome)
    {
        for (int round = 0; round < 2; round++)
        {
            foreach (string cf in s_cfs)
            {
                ColumnFamily h = db.GetColumnFamily(cf);
                for (int i = 0; i < n; i++)
                {
                    string k = "k" + i.ToString("D4");
                    switch ((i + round + 1) % 5)
                    {
                        case 0:
                        case 1:
                            if (i % 2 == 0) h.Put(U(k), U(Val(i, tag + round)));
                            else await h.PutAsync(U(k), U(Val(i, tag + round)));
                            m.Put(cf, k, Val(i, tag + round));
                            break;
                        case 2:
                            h.Delete(U(k));
                            m.Delete(cf, k);
                            break;
                        default:
                            h.Merge(U(k), U(tag + round + "m" + i));
                            m.Merge(cf, k, tag + round + "m" + i);
                            break;
                    }
                }
                h.DeleteRange(U("k" + (n / 2).ToString("D4")), U("k" + (n / 2 + 5).ToString("D4")));
                m.DeleteRange(cf, "k" + (n / 2).ToString("D4"), "k" + (n / 2 + 5).ToString("D4"));
                using WriteBatch b = db.CreateWriteBatch();
                b.Put(h.Id, U("batch-" + tag + round), U("b")).SingleDelete(h.Id, U("never-written"));
                await db.WriteAsync(b);
                m.Put(cf, "batch-" + tag + round, "b");
            }
            if (round == 0 && flushSome)
            {
                db.Flush();
                await db.GetColumnFamily("two").FlushAsync();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // verification

    private static void VerifyNative(RocksDbSharp.RocksDb db, Model m)
    {
        foreach (KeyValuePair<(string Cf, string Key), string?> kv in m.Data)
        {
            byte[]? v = db.Get(U(kv.Key.Key), db.GetColumnFamily(kv.Key.Cf));
            Assert.AreEqual(kv.Value, v is null ? null : Encoding.UTF8.GetString(v), "native: " + kv.Key);
        }
    }

    private static async Task VerifyManaged(RocksDatabase db, Model m)
    {
        int i = 0;
        foreach (KeyValuePair<(string Cf, string Key), string?> kv in m.Data)
        {
            ColumnFamily cf = db.GetColumnFamily(kv.Key.Cf);
            byte[]? v = i++ % 2 == 0 ? cf.Get(U(kv.Key.Key)) : await cf.GetAsync(U(kv.Key.Key));
            Assert.AreEqual(kv.Value, v is null ? null : Encoding.UTF8.GetString(v), "managed: " + kv.Key);
        }
        // A full scan of each column family yields exactly the live keys, in order.
        foreach (string cfName in s_cfs)
        {
            var expected = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (KeyValuePair<(string Cf, string Key), string?> kv in m.Data)
            {
                if (kv.Key.Cf == cfName && kv.Value is not null) expected[kv.Key.Key] = kv.Value;
            }
            var actual = new List<string>();
            using (Iterator it = db.GetColumnFamily(cfName).NewIterator(null))
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) actual.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
            }
            var exp = new List<string>();
            foreach (KeyValuePair<string, string> kv in expected) exp.Add(kv.Key + "=" + kv.Value);
            CollectionAssert.AreEqual(exp, actual, "scan of " + cfName);
        }
    }

    private static RocksDbSharp.RocksDb OpenNative(string path, RocksDbSharp.MergeOperator merge) =>
        Open(path, new RocksDbSharp.DbOptions(), merge);

    private static ColumnFamilyDescriptor[] ManagedDescriptors()
    {
        var list = new ColumnFamilyDescriptor[s_cfs.Length];
        for (int i = 0; i < s_cfs.Length; i++) list[i] = new ColumnFamilyDescriptor(s_cfs[i], new ColumnFamilyOptions { MergeOperator = new StringAppend() });
        return list;
    }

    private static DbOptions ManagedOptions(bool create) => new() { CreateIfMissing = create, CreateMissingColumnFamilies = create };

    // ---------------------------------------------------------------------------------------------------
    // tests

    [TestMethod]
    public async Task NativeToManagedToNative()
    {
        using var dir = new TempDir("dbcompat-n2m");
        var model = new Model();
        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        ulong nativeSeq;
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, merge))
        {
            NativeWorkload(db, model, "n", 120);
            nativeSeq = db.GetLatestSequenceNumber();
        }

        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, ManagedOptions(false), ManagedDescriptors()))
        {
            Assert.AreEqual(nativeSeq, db.GetLatestSequenceNumber());
            // Native flushed default and "one"; WAL data was flushed to L0 during recovery.
            Assert.IsTrue(db.GetLiveFilesMetadata().Count >= 4);
            await VerifyManaged(db, model);
            await ManagedWorkload(db, model, "m", 100, flushSome: true);
            await VerifyManaged(db, model);
        }

        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, merge))
        {
            VerifyNative(db, model);
            // And once more after native flushes everything (native reads its own SSTs + ours).
            NativeWorkload(db, model, "n2", 60);
            VerifyNative(db, model);
        }
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, ManagedOptions(false), ManagedDescriptors()))
        {
            await VerifyManaged(db, model);
        }
    }

    [TestMethod]
    [DataRow(false, DisplayName = "WAL only")]
    [DataRow(true, DisplayName = "flushed + WAL")]
    public async Task ManagedToNativeToManaged(bool flushSome)
    {
        using var dir = new TempDir("dbcompat-m2n");
        var model = new Model();
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, ManagedOptions(true), ManagedDescriptors()))
        {
            await ManagedWorkload(db, model, "m", 150, flushSome);
            await VerifyManaged(db, model);
        }
        int ssts = Directory.GetFiles(dir.Path, "*.sst").Length;
        Assert.AreEqual(flushSome ? 2 : 0, ssts);
        Assert.AreEqual(flushSome ? 2 : 1, Directory.GetFiles(dir.Path, "*.log").Length); // CF "one" still needs the first WAL
        Assert.IsTrue(model.Data.Count > 400 && model.Data.ContainsValue(null));

        // OPTIONS written by the managed DB load natively with ignore_unknown_options = false.
        Assert.AreEqual(3, NativeLoadLatestOptionsCfCount(dir.Path));

        RocksDbSharp.MergeOperator merge = CreateConcatMergeOperator();
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, merge))
        {
            VerifyNative(db, model);
            NativeWorkload(db, model, "n", 80);
            VerifyNative(db, model);
        }

        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, ManagedOptions(false), ManagedDescriptors()))
        {
            await VerifyManaged(db, model);
            db.Put(U("final"), U("1"));
            model.Put("default", "final", "1");
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, merge)) VerifyNative(db, model);
    }

    [TestMethod]
    public async Task ManagedTablesWithFiltersAndCompressionReadNatively()
    {
        using var dir = new TempDir("dbcompat-tables");
        var cfo = new ColumnFamilyOptions { Compression = global::RocksDb.Managed.Compression.CompressionType.LZ4 };
        cfo.TableOptions.FilterPolicy = "bloomfilter:10:false";
        cfo.TableOptions.FormatVersion = 6;
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(dir.Path, ManagedOptions(true), cfo))
        {
            for (int i = 0; i < 5000; i++) db.Put(U("key" + i.ToString("D5")), U(Val(i, "t")));
            await db.FlushAsync();
            for (int i = 0; i < 5000; i += 10) db.Delete(U("key" + i.ToString("D5")));
            await db.FlushAsync();
        }
        var options = new RocksDbSharp.DbOptions();
        using (var db = RocksDbSharp.RocksDb.Open(options, dir.Path))
        {
            for (int i = 0; i < 5000; i += 7)
            {
                byte[]? v = db.Get(U("key" + i.ToString("D5")));
                Assert.AreEqual(i % 10 == 0 ? null : Val(i, "t"), v is null ? null : Encoding.UTF8.GetString(v), "key " + i);
            }
            Assert.AreEqual(2, db.GetLiveFilesMetadata().Count);
        }
    }

    // ---------------------------------------------------------------------------------------------------

    /// <summary>Managed StringAppend with ',' (same name and semantics as <see cref="CreateConcatMergeOperator"/>).</summary>
    private sealed class StringAppend : MergeOperator
    {
        public override string Name => "StringAppendOperator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }
    }

    /// <summary>rocksdb_load_latest_options with ignore_unknown_options = false; returns the column family count.</summary>
    private static int NativeLoadLatestOptionsCfCount(string dbPath)
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
            native.rocksdb_load_latest_options_destroy(Marshal.ReadIntPtr(dbOptsOut), Marshal.ReadIntPtr(namesOut), Marshal.ReadIntPtr(cfOptsOut), (UIntPtr)num);
            return num;
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
