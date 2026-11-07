using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Wal;

/// <summary>
/// Writes WAL matrix images with either engine and opens damaged copies with both (managed in process through the port,
/// native through rocksdb-sharp's librocksdb 11.12).
/// </summary>
internal static unsafe class WalMatrixEngines
{
    private const int NativeZstd = 7; // CompressionType::kZSTDCompression

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    // ---- writers -----------------------------------------------------------------------------------------------

    private interface IWorkloadDb : IDisposable
    {
        void Put(int cf, string key, string value, bool sync);
        void Delete(int cf, string key, bool sync);
        void BothCfs(string key, string value, bool sync);
        void Flush(int cf);
        void WaitIdle();
        void FlushWal();
    }

    private sealed class ManagedWriter(RocksDatabase db) : IWorkloadDb
    {
        private ColumnFamily Cf(int i) => i == 0 ? db.DefaultColumnFamily : db.GetColumnFamily("cf1");
        private static WriteOptions W(bool sync) => sync ? WriteOptions.SyncWrites : new WriteOptions();
        public void Put(int cf, string key, string value, bool sync) => Cf(cf).Put(W(sync), U(key), U(value));
        public void Delete(int cf, string key, bool sync) => Cf(cf).Delete(W(sync), U(key));

        public void BothCfs(string key, string value, bool sync)
        {
            using WriteBatch b = db.CreateWriteBatch();
            b.Put(Cf(0), U(key), U(value)).Put(Cf(1), U(key), U(value));
            db.Write(W(sync), b);
        }

        public void Flush(int cf) => Cf(cf).Flush();

        public void WaitIdle()
        {
            db.WaitForCompact();
            db.DefaultColumnFamily.Db.WaitForBackgroundWorkForTest();
        }

        public void FlushWal() => db.FlushWal(sync: false);
        public void Dispose() => db.Dispose();
    }

    private sealed class NativeWriter(RocksDbSharp.RocksDb db) : IWorkloadDb
    {
        private RocksDbSharp.ColumnFamilyHandle Cf(int i) => db.GetColumnFamily(WalMatrixUtil.ColumnFamilies[i]);
        private static RocksDbSharp.WriteOptions W(bool sync) => new RocksDbSharp.WriteOptions().SetSync(sync);
        public void Put(int cf, string key, string value, bool sync) => db.Put(U(key), U(value), Cf(cf), W(sync));
        public void Delete(int cf, string key, bool sync) => db.Remove(U(key), Cf(cf), W(sync));

        public void BothCfs(string key, string value, bool sync)
        {
            using var b = new RocksDbSharp.WriteBatch();
            b.Put(U(key), U(value), Cf(0)).Put(U(key), U(value), Cf(1));
            db.Write(b, W(sync));
        }

        public void Flush(int cf) => RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().SetWaitForFlush(true).Handle, Cf(cf).Handle);

        public void WaitIdle()
        {
            IntPtr o = RocksDbSharp.Native.Instance.rocksdb_wait_for_compact_options_create();
            RocksDbSharp.Native.Instance.rocksdb_wait_for_compact(db.Handle, o);
            RocksDbSharp.Native.Instance.rocksdb_wait_for_compact_options_destroy(o);
        }

        public void FlushWal() => RocksDbSharp.Native.Instance.rocksdb_flush_wal(db.Handle, 0);
        public void Dispose() => db.Dispose();
    }

    /// <summary>
    /// Runs the workload with <paramref name="cfg"/>'s writer and copies the open DB (a process-kill crash image) to
    /// <paramref name="root"/>. The workload makes the oldest WALs obsolete (recycled with recycle_log_file_num), lets
    /// the column families' flush progress differ (cf1's data stays in an older WAL than the default CF's), writes
    /// fragmented records, a batch over both column families and a last sync write followed by unsynced ones. With
    /// recycling the latest WAL is a reused one with a stale tail.
    /// </summary>
    public static WalImage WriteImage(WalWriteConfig cfg, string root, int seed)
    {
        string work = root + "-work";
        Directory.CreateDirectory(Path.Combine(work, "db"));
        var image = new WalImage { Root = root, Config = cfg };
        var rnd = new Random(seed);
        string V(string tag, int i) => tag + "-" + i + "-" + new string((char)('a' + i % 26), 20 + rnd.Next(150));
        using (IWorkloadDb db = OpenWriter(cfg, work))
        {
            for (int i = 0; i < 40; i++) db.Put(i % 2, "a" + i, V("a", i), sync: i % 10 == 9);
            db.Flush(0);
            db.Flush(1);
            db.WaitIdle();
            for (int i = 0; i < 30; i++)
            {
                if (i % 7 == 3) db.Delete(i % 2, "a" + (i - 1), sync: false);
                else db.Put(i % 2, "b" + i, V("b", i), sync: i % 10 == 9);
            }
            db.BothCfs("both1", "x1", sync: false);
            db.Put(0, "big1", new string('B', 70_000), sync: false); // fragmented over three blocks
            db.Flush(1);
            db.WaitIdle();
            for (int i = 0; i < 30; i++)
            {
                if (i % 5 == 0) db.BothCfs("cb" + i, V("cb", i), sync: false);
                else db.Put(i % 2, "c" + i, V("c", i), sync: i % 10 == 9);
            }
            db.Flush(0);
            db.WaitIdle();
            for (int i = 0; i < 20; i++) db.Put(i % 2, "d" + i, V("d", i), sync: false);
            db.Put(1, "big2", new string('C', 40_000), sync: false);
            // cf1 flushes again: the switch reuses the WAL of the second phase (recycled once both column families
            // flushed past it), whose big record stays behind the new data as a stale tail; the default CF keeps data
            // in the previous WAL.
            db.Flush(1);
            db.WaitIdle();
            image.LastSyncedKey = "synced-last";
            image.LastSyncedValue = "s";
            db.Put(0, image.LastSyncedKey, image.LastSyncedValue, sync: true);
            List<ulong> wals = WalMatrixUtil.Wals(cfg.SeparateWalDir ? Path.Combine(work, "wal") : Path.Combine(work, "db"));
            image.SyncedWal = wals[^1];
            image.SyncedWalSize = new FileInfo(FileNames.LogFileName(cfg.SeparateWalDir ? Path.Combine(work, "wal") : Path.Combine(work, "db"), wals[^1])).Length;
            for (int i = 0; i < 4; i++) db.Put(i % 2, "e" + i, V("e", i), sync: false);
            db.BothCfs("both2", "x2", sync: false);
            if (cfg.ManualWalFlush) db.FlushWal();
            WalMatrixUtil.CopyTree(work, root);
        }
        try { Directory.Delete(work, recursive: true); }
        catch (IOException) { }
        return image;
    }

    private static IWorkloadDb OpenWriter(WalWriteConfig cfg, string work)
    {
        var open = new WalOpenConfig(WalRecoveryMode.PointInTimeRecovery, Paranoid: true, WalMatrixFilter.None);
        if (cfg.Writer == WalMatrixWriter.Managed)
        {
            DbOptions o = ManagedOptions(cfg, open, work);
            o.CreateIfMissing = true;
            o.CreateMissingColumnFamilies = true;
            return new ManagedWriter(RocksDatabase.Open(Path.Combine(work, "db"), o, ManagedCfs()));
        }
        RocksDbSharp.DbOptions n = NativeOptions(cfg, open, work);
        n.SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        return new NativeWriter(RocksDbSharp.RocksDb.Open(n, Path.Combine(work, "db"), NativeCfs()));
    }

    private static ColumnFamilyDescriptor[] ManagedCfs() => [new("default", new ColumnFamilyOptions()), new("cf1", new ColumnFamilyOptions())];

    private static RocksDbSharp.ColumnFamilies NativeCfs() => new() { { "cf1", new RocksDbSharp.ColumnFamilyOptions() } };

    private static DbOptions ManagedOptions(WalWriteConfig cfg, WalOpenConfig open, string root) => new()
    {
        WalRecoveryMode = open.Mode,
        ParanoidChecks = open.Paranoid,
        RecycleLogFileNum = cfg.Recycle ? 1UL : 0,
        TrackAndVerifyWals = cfg.TrackWals,
        TrackAndVerifyWalsInManifest = cfg.TrackInManifest,
        WalCompression = cfg.Zstd ? CompressionType.Zstd : CompressionType.None,
        ManualWalFlush = cfg.ManualWalFlush,
        WalDir = cfg.SeparateWalDir ? Path.Combine(root, "wal") : "",
        AvoidFlushDuringRecovery = cfg.AvoidFlushDuringRecovery,
        AvoidFlushDuringShutdown = true,
    };

    private static RocksDbSharp.DbOptions NativeOptions(WalWriteConfig cfg, WalOpenConfig open, string root)
    {
        var o = new RocksDbSharp.DbOptions();
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        n.rocksdb_options_set_wal_recovery_mode(o.Handle, (int)open.Mode);
        n.rocksdb_options_set_paranoid_checks(o.Handle, open.Paranoid ? (byte)1 : (byte)0);
        n.rocksdb_options_set_recycle_log_file_num(o.Handle, (UIntPtr)(cfg.Recycle ? 1u : 0u));
        n.rocksdb_options_set_track_and_verify_wals(o.Handle, cfg.TrackWals ? (byte)1 : (byte)0);
        n.rocksdb_options_set_track_and_verify_wals_in_manifest(o.Handle, cfg.TrackInManifest ? (byte)1 : (byte)0);
        n.rocksdb_options_set_wal_compression(o.Handle, cfg.Zstd ? NativeZstd : 0);
        n.rocksdb_options_set_manual_wal_flush(o.Handle, cfg.ManualWalFlush ? (byte)1 : (byte)0);
        if (cfg.SeparateWalDir) n.rocksdb_options_set_wal_dir(o.Handle, Path.Combine(root, "wal"));
        n.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, cfg.AvoidFlushDuringRecovery ? (byte)1 : (byte)0);
        n.rocksdb_options_set_avoid_flush_during_shutdown(o.Handle, 1);
        return o;
    }

    // ---- openers -----------------------------------------------------------------------------------------------

    private sealed class MatrixFilter(WalMatrixFilter f) : WalFilter
    {
        private int _index;
        public override string Name => "MatrixFilter";

        public override WalProcessingOption LogRecordFound(ulong logNumber, string logFileName, WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            return (WalProcessingOption)f.OptionFor(_index++);
        }
    }

    public static WalOpenOutcome OpenManaged(WalImage image, WalOpenConfig open)
    {
        DbOptions o = ManagedOptions(image.Config, open, image.Root);
        if (open.Filter.Kind != WalMatrixFilterKind.None) o.WalFilter = new MatrixFilter(open.Filter);
        RocksDatabase db;
        try
        {
            db = RocksDatabase.Open(image.DbDir, o, ManagedCfs());
        }
        catch (RocksDbException e)
        {
            return new WalOpenOutcome(e.Code, WalMatrixUtil.Normalize(e.Message, image.Root), "", 0);
        }
        using (db)
        {
            string dump = WalMatrixUtil.Dump(i => ManagedScan(i == 0 ? db.DefaultColumnFamily : db.GetColumnFamily("cf1")), 2);
            return new WalOpenOutcome(null, "", dump, db.GetLatestSequenceNumber());
        }
    }

    private static List<(byte[], byte[])> ManagedScan(ColumnFamily cf)
    {
        var r = new List<(byte[], byte[])>();
        using RocksDb.Managed.Iterator it = cf.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) r.Add((it.Key.ToArray(), it.Value.ToArray()));
        return r;
    }

    // Native WAL filter callbacks (one native open at a time: the compat tests of this class do not run in parallel).
    private static WalMatrixFilter s_nativeFilter;
    private static int s_nativeFilterIndex;
    private static readonly IntPtr s_filterName = Marshal.StringToHGlobalAnsi("MatrixFilter");

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void NativeDestructor(IntPtr state) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static IntPtr NativeName(IntPtr state) => s_filterName;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void NativeCfMap(IntPtr state, uint* ids, ulong* logNumbers, nuint count, byte** names, nuint* nameLengths, uint* nameIds, nuint nameCount) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int NativeRecordFound(IntPtr state, ulong logNumber, byte* fileName, nuint fileNameLength, IntPtr batch, IntPtr newBatch, byte* batchChanged)
    {
        *batchChanged = 0;
        return s_nativeFilter.OptionFor(s_nativeFilterIndex++);
    }

    private static readonly Lock s_nativeLock = new();

    public static WalOpenOutcome OpenNative(WalImage image, WalOpenConfig open)
    {
        lock (s_nativeLock)
        {
            RocksDbSharp.DbOptions o = NativeOptions(image.Config, open, image.Root);
            IntPtr filter = IntPtr.Zero;
            if (open.Filter.Kind != WalMatrixFilterKind.None)
            {
                s_nativeFilter = open.Filter;
                s_nativeFilterIndex = 0;
                filter = RocksDbSharp.Native.Instance.rocksdb_walfilter_create(IntPtr.Zero,
                    (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, void>)&NativeDestructor,
                    (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, uint*, ulong*, nuint, byte**, nuint*, uint*, nuint, void>)&NativeCfMap,
                    (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, ulong, byte*, nuint, IntPtr, IntPtr, byte*, int>)&NativeRecordFound,
                    (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr>)&NativeName);
                RocksDbSharp.Native.Instance.rocksdb_options_set_wal_filter(o.Handle, filter);
            }
            try
            {
                RocksDbSharp.RocksDb db;
                try
                {
                    db = RocksDbSharp.RocksDb.Open(o, image.DbDir, NativeCfs());
                }
                catch (RocksDbSharp.RocksDbException e)
                {
                    string m = e.Message;
                    return new WalOpenOutcome(WalMatrixUtil.NativeCode(m), WalMatrixUtil.Normalize(m, image.Root), "", 0);
                }
                using (db)
                {
                    string dump = WalMatrixUtil.Dump(i => NativeScan(db, db.GetColumnFamily(WalMatrixUtil.ColumnFamilies[i])), 2);
                    return new WalOpenOutcome(null, "", dump, db.GetLatestSequenceNumber());
                }
            }
            finally
            {
                if (filter != IntPtr.Zero)
                {
                    RocksDbSharp.Native.Instance.rocksdb_options_clear_wal_filter(o.Handle);
                    RocksDbSharp.Native.Instance.rocksdb_walfilter_destroy(filter);
                }
            }
        }
    }

    private static List<(byte[], byte[])> NativeScan(RocksDbSharp.RocksDb db, RocksDbSharp.ColumnFamilyHandle cf)
    {
        var r = new List<(byte[], byte[])>();
        using RocksDbSharp.Iterator it = db.NewIterator(cf);
        for (it.SeekToFirst(); it.Valid(); it.Next()) r.Add((it.Key(), it.Value()));
        return r;
    }
}
