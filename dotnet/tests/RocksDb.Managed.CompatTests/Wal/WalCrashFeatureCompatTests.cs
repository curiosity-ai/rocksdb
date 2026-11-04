using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>
/// Crash images of write features whose WAL records point outside the WAL, recovered by both engines (native 11.12):
/// blob direct write, whose blob indexes in the WAL reference blob files no flush registered yet.
/// </summary>
[TestClass]
[DoNotParallelize]
public sealed class WalCrashFeatureCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static RocksDbSharp.DbOptions NativeOptions(bool avoidFlushDuringRecovery)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_enable_blob_files(o.Handle, 1);
        n.rocksdb_options_set_enable_blob_direct_write(o.Handle, 1);
        n.rocksdb_options_set_min_blob_size(o.Handle, 64);
        n.rocksdb_options_set_allow_concurrent_memtable_write(o.Handle, 0);
        n.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, avoidFlushDuringRecovery ? (byte)1 : (byte)0);
        return o;
    }

    private static DbOptions ManagedOptions(bool avoidFlushDuringRecovery) =>
        new() { CreateIfMissing = true, AllowConcurrentMemtableWrite = false, AvoidFlushDuringRecovery = avoidFlushDuringRecovery };

    private static ColumnFamilyOptions ManagedCf() => new() { EnableBlobFiles = true, EnableBlobDirectWrite = true, MinBlobSize = 64 };

    private static string Read(Func<string, string?> get, int count)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < count; i++) sb.Append(get("k" + i) ?? "<none>").Append('|');
        return sb.ToString();
    }

    private static string ManagedOutcome(string dir, bool avoidFlush, int count)
    {
        try
        {
            using RocksDatabase db = RocksDatabase.Open(dir, ManagedOptions(avoidFlush), ManagedCf());
            string first = Read(k => db.Get(U(k)) is { } v ? Encoding.UTF8.GetString(v) : null, count);
            return first;
        }
        catch (RocksDbException e)
        {
            return "error " + e.Code + ": " + e.Message;
        }
    }

    private static string NativeOutcome(string dir, bool avoidFlush, int count)
    {
        try
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(avoidFlush), dir);
            return Read(k => db.Get(k), count);
        }
        catch (RocksDbSharp.RocksDbException e)
        {
            return "error " + WalMatrixUtil.NativeCode(e.Message) + ": " + e.Message;
        }
    }

    /// <summary>
    /// Writes blob-direct-write values (some flushed, the rest only in the WAL and unregistered blob files), copies the open
    /// DB (a process-kill crash image) and opens copies with both engines, with and without avoid_flush_during_recovery,
    /// twice (the second open sees what the first one's recovery and obsolete-file purge left).
    /// </summary>
    [TestMethod]
    [DataRow(false, false)]
    [DataRow(false, true)]
    [DataRow(true, false)]
    [DataRow(true, true)]
    public void BlobDirectWriteCrashImageRecoversLikeNative(bool nativeWriter, bool avoidFlushDuringRecovery)
    {
        const int count = 40;
        using var src = new TempDir("bdw-crash-src");
        using var image = new TempDir("bdw-crash-img");
        string V(int i) => "value-" + i + new string((char)('a' + i % 26), 80 + i);
        if (nativeWriter)
        {
            using RocksDbSharp.RocksDb db = RocksDbSharp.RocksDb.Open(NativeOptions(false), src.Path);
            for (int i = 0; i < count; i++)
            {
                db.Put("k" + i, V(i));
                if (i == 15) db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            WalMatrixUtil.CopyTree(src.Path, image.Path);
        }
        else
        {
            using RocksDatabase db = RocksDatabase.Open(src.Path, ManagedOptions(false), ManagedCf());
            for (int i = 0; i < count; i++)
            {
                db.Put(U("k" + i), U(V(i)));
                if (i == 15) db.Flush();
            }
            WalMatrixUtil.CopyTree(src.Path, image.Path);
        }
        using var m = new TempDir("bdw-crash-m");
        using var n = new TempDir("bdw-crash-n");
        WalMatrixUtil.CopyTree(image.Path, m.Path);
        WalMatrixUtil.CopyTree(image.Path, n.Path);
        for (int round = 0; round < 2; round++)
        {
            string no = WalMatrixUtil.Normalize(NativeOutcome(n.Path, avoidFlushDuringRecovery, count), n.Path);
            string mo = WalMatrixUtil.Normalize(ManagedOutcome(m.Path, avoidFlushDuringRecovery, count), m.Path);
            Assert.AreEqual(no, mo, "open " + round);
        }
    }
}
