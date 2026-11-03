using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// enable_pipelined_write against native RocksDB 11.12 (rocksdb_options_set_enable_pipelined_write): WALs written by
/// concurrent pipelined writers (sync and async API, parallel memtable inserts, WAL termination points) on either side
/// are recovered by the other, and native reads the option back from a managed OPTIONS file.
/// </summary>
[TestClass]
public sealed class PipelinedWriteCompatTests
{
    private const int Writers = 6;
    private const int PerWriter = 300;

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string Key(string side, int t, int i) => side + t + "-" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
    private static string Val(string side, int t, int i) => "v-" + Key(side, t, i) + new string('x', i % 40);

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        RocksDbSharp.Native.Instance.rocksdb_options_set_enable_pipelined_write(options.Handle, 1);
        // Keep the data in the WALs so that the other side has to recover it.
        RocksDbSharp.Native.Instance.rocksdb_options_set_avoid_flush_during_shutdown(options.Handle, 1);
        return RocksDbSharp.RocksDb.Open(options, path);
    }

    private static RocksDatabase OpenManaged(string path) =>
        RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, EnablePipelinedWrite = true, AvoidFlushDuringShutdown = true });

    [TestMethod]
    [Timeout(120_000, CooperativeCancellation = true)]
    public void PipelinedWalsRecoverBothWays()
    {
        using var dir = new TempDir("pipelined");
        // Managed: concurrent pipelined writers; every 4th writer adds a part after a WAL termination point, which the
        // memtables get but the WAL record does not.
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            RunWriters(t => (k, v, sync) =>
            {
                using WriteBatch b = db.CreateWriteBatch();
                b.Put(U(k), U(v));
                if (t % 4 == 0)
                {
                    b.MarkWalTerminationPoint();
                    b.Put(U("memonly-" + k), U(v));
                }
                var wo = new WriteOptions { Sync = sync };
                if (t % 2 == 0) db.Write(wo, b);
                else db.WriteAsync(wo, b).AsTask().GetAwaiter().GetResult();
            }, "m");
            Assert.AreEqual((ulong)(Writers * PerWriter + ((Writers + 3) / 4) * PerWriter), db.GetLatestSequenceNumber());
        }
        Assert.IsTrue(NativeLoadPipelined(dir.Path), "native reads enable_pipelined_write from the managed OPTIONS file");
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            Verify(k => native.Get(k), "m");
            for (int i = 0; i < PerWriter; i++) Assert.IsNull(native.Get("memonly-" + Key("m", 0, i)));
            var wo = new RocksDbSharp.WriteOptions();
            RunWriters(_ => (k, v, sync) =>
            {
                RocksDbSharp.WriteOptions o = sync ? new RocksDbSharp.WriteOptions().SetSync(true) : wo;
                native.Put(k, v, writeOptions: o);
            }, "n");
            Verify(k => native.Get(k), "n");
        }
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, "m");
            Verify(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, "n");
            for (int i = 0; i < PerWriter; i++) Assert.IsNull(db.Get(U("memonly-" + Key("m", 0, i))));
        }
    }

    private static void RunWriters(Func<int, Action<string, string, bool>> writerFor, string side)
    {
        var threads = new Thread[Writers];
        Exception? failure = null;
        for (int t = 0; t < Writers; t++)
        {
            int id = t;
            Action<string, string, bool> write = writerFor(id);
            threads[t] = new Thread(() =>
            {
                try
                {
                    for (int i = 0; i < PerWriter; i++) write(Key(side, id, i), Val(side, id, i), (i + id) % 25 == 0);
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            });
            threads[t].Start();
        }
        foreach (Thread th in threads) th.Join();
        if (failure is not null) throw new AssertFailedException("writer failed: " + failure);
    }

    private static void Verify(Func<string, string?> get, string side)
    {
        for (int t = 0; t < Writers; t++)
        {
            for (int i = 0; i < PerWriter; i++) Assert.AreEqual(Val(side, t, i), get(Key(side, t, i)), Key(side, t, i));
        }
    }

    /// <summary>rocksdb_load_latest_options: enable_pipelined_write of the DB options.</summary>
    private static bool NativeLoadPipelined(string dbPath)
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
            bool pipelined = native.rocksdb_options_get_enable_pipelined_write(dbOpts) != 0;
            native.rocksdb_load_latest_options_destroy(dbOpts, Marshal.ReadIntPtr(namesOut), Marshal.ReadIntPtr(cfOptsOut), (UIntPtr)num);
            return pipelined;
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
