using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// track_and_verify_wals and wal_filter against native RocksDB 11.12 (rocksdb_options_set_track_and_verify_wals,
/// rocksdb_walfilter_create): the predecessor-WAL-info records written by either implementation have the same contents
/// and are verified by the other (missing / truncated predecessor detected with the same messages), and a WAL filter sees
/// the same records and column family log numbers in both.
/// </summary>
[TestClass]
public sealed unsafe class WalPredecessorAndFilterCompatTests
{
    private const int TolerateCorruptedTailRecords = 0; // WALRecoveryMode::kTolerateCorruptedTailRecords
    private const int PointInTimeRecovery = 2; // WALRecoveryMode::kPointInTimeRecovery

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    private static RocksDbSharp.DbOptions NativeOptions(bool track, ulong recycle = 0)
    {
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetCreateMissingColumnFamilies(true);
        RocksDbSharp.Native.Instance.rocksdb_options_set_track_and_verify_wals(options.Handle, track ? (byte)1 : (byte)0);
        RocksDbSharp.Native.Instance.rocksdb_options_set_wal_recovery_mode(options.Handle, TolerateCorruptedTailRecords);
        RocksDbSharp.Native.Instance.rocksdb_options_set_avoid_flush_during_shutdown(options.Handle, 1);
        if (recycle != 0)
        {
            // SanitizeOptions drops WAL recycling with kTolerateCorruptedTailRecords / kAbsoluteConsistency.
            RocksDbSharp.Native.Instance.rocksdb_options_set_wal_recovery_mode(options.Handle, PointInTimeRecovery);
            RocksDbSharp.Native.Instance.rocksdb_options_set_recycle_log_file_num(options.Handle, (UIntPtr)recycle);
        }
        return options;
    }

    private static RocksDbSharp.RocksDb OpenNative(string path, RocksDbSharp.DbOptions options)
    {
        var cfs = new RocksDbSharp.ColumnFamilies { { "cf1", new RocksDbSharp.ColumnFamilyOptions() } };
        return RocksDbSharp.RocksDb.Open(options, path, cfs);
    }

    private static DbOptions ManagedOptions(bool track) => new()
    {
        CreateIfMissing = true,
        CreateMissingColumnFamilies = true,
        TrackAndVerifyWals = track,
        WalRecoveryMode = WalRecoveryMode.TolerateCorruptedTailRecords,
        AvoidFlushDuringShutdown = true,
    };

    private static ColumnFamilyDescriptor[] Cfs() => [new("default", new ColumnFamilyOptions()), new("cf1", new ColumnFamilyOptions())];

    private static RocksDatabase OpenManaged(string path, DbOptions options) => RocksDatabase.Open(path, options, Cfs());

    private static List<ulong> Wals(string dir)
    {
        var r = new List<ulong>();
        foreach (string f in Directory.GetFiles(dir, "*.log"))
        {
            if (FileNames.TryParseFileName(Path.GetFileName(f), out ulong n, out FileType t) && t == FileType.WalFile) r.Add(n);
        }
        r.Sort();
        return r;
    }

    private static string CopyDb(string src, string dst)
    {
        Directory.CreateDirectory(dst);
        foreach (string f in Directory.GetFiles(src))
        {
            if (Path.GetFileName(f) == "LOCK") continue;
            File.Copy(f, Path.Combine(dst, Path.GetFileName(f)));
        }
        return dst;
    }

    /// <summary>The physical records (type, payload) of a WAL; handles both header formats.</summary>
    private static List<(byte Type, byte[] Payload)> PhysicalRecords(byte[] wal)
    {
        var r = new List<(byte, byte[])>();
        int pos = 0;
        while (pos + LogFormat.HeaderSize <= wal.Length)
        {
            int blockLeft = LogFormat.BlockSize - pos % LogFormat.BlockSize;
            if (blockLeft < LogFormat.HeaderSize) { pos += blockLeft; continue; }
            int len = wal[pos + 4] | wal[pos + 5] << 8;
            byte type = wal[pos + 6];
            if (type == 0 && len == 0) break;
            int header = LogFormat.IsRecyclableType(type) ? LogFormat.RecyclableHeaderSize : LogFormat.HeaderSize;
            r.Add((type, wal.AsSpan(pos + header, len).ToArray()));
            pos += header + len;
        }
        return r;
    }

    /// <summary>The predecessor record at the start of WAL <paramref name="number"/>.</summary>
    private static PredecessorWalInfo PredecessorRecordOf(string dir, ulong number, out byte type)
    {
        foreach ((byte t, byte[] payload) in PhysicalRecords(File.ReadAllBytes(FileNames.LogFileName(dir, number))))
        {
            if (t is (byte)LogRecordType.PredecessorWalInfo or (byte)LogRecordType.RecyclePredecessorWalInfo)
            {
                type = t;
                Assert.IsTrue(PredecessorWalInfo.TryDecode(payload, out PredecessorWalInfo info));
                return info;
            }
        }
        throw new AssertFailedException("WAL " + number + " has no predecessor record");
    }

    /// <summary>What a reader observes for WAL <paramref name="number"/>: its size on disk and the sequence of its last batch.</summary>
    private static PredecessorWalInfo Observed(string dir, ulong number)
    {
        string path = FileNames.LogFileName(dir, number);
        ulong last = 0;
        using (var reader = new LogReader(LocalFileSystem.Instance.OpenSequential(path), null, checksum: true, number))
        {
            while (reader.TryReadRecord(out ReadOnlySpan<byte> record)) last = Util.Coding.DecodeFixed64(record);
        }
        return new PredecessorWalInfo(number, (ulong)new FileInfo(path).Length, last);
    }

    private static void TruncateTo0(string path)
    {
        using FileStream f = File.Open(path, FileMode.Truncate);
    }

    [TestMethod]
    public void ManagedPredecessorRecordsAreVerifiedByNative()
    {
        using var dir = new TempDir("walpred-m2n");
        using (RocksDatabase db = OpenManaged(dir.Path, ManagedOptions(track: true)))
        {
            ColumnFamily cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 50; i++)
            {
                db.Put(U("d" + i), U("v" + i));
                cf1.Put(U("c" + i), U("w" + i));
            }
            // A last batch of several keys: the recorded sequence is the batch's first one (WriteToWAL's `sequence`).
            using (WriteBatch b = db.CreateWriteBatch())
            {
                b.Put(U("b1"), U("1")).Put(cf1, U("b2"), U("2")).Put(U("b3"), U("3"));
                db.Write(b);
            }
            db.Flush(); // the default CF only: a new WAL; the old one stays for cf1
            db.Put(U("after"), U("x"));
            cf1.Put(U("after1"), U("y"));
        }
        List<ulong> wals = Wals(dir.Path);
        Assert.IsTrue(wals.Count >= 2);
        PredecessorWalInfo recorded = PredecessorRecordOf(dir.Path, wals[^1], out byte type);
        Assert.AreEqual((byte)LogRecordType.PredecessorWalInfo, type);
        Assert.AreEqual(Observed(dir.Path, wals[^2]), recorded);

        // Native detects a missing / truncated predecessor of a managed WAL.
        using (var copy = new TempDir("walpred-m2n-missing"))
        {
            CopyDb(dir.Path, copy.Path);
            File.Delete(FileNames.LogFileName(copy.Path, wals[^2]));
            var e = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => OpenNative(copy.Path, NativeOptions(track: true)).Dispose());
            StringAssert.Contains(e.Message, "Missing WAL of log number " + wals[^2]);
            var m = Assert.ThrowsExactly<RocksDbException>(() => OpenManaged(copy.Path, ManagedOptions(track: true)).Dispose());
            Assert.AreEqual(StatusCode.Corruption, m.Code);
            StringAssert.Contains(m.Message, "Missing WAL of log number " + wals[^2]);
        }
        using (var copy = new TempDir("walpred-m2n-trunc"))
        {
            CopyDb(dir.Path, copy.Path);
            TruncateTo0(FileNames.LogFileName(copy.Path, wals[^2]));
            var e = Assert.ThrowsExactly<RocksDbSharp.RocksDbException>(() => OpenNative(copy.Path, NativeOptions(track: true)).Dispose());
            StringAssert.Contains(e.Message, "Mismatched last sequence number recorded in the WAL of log number " + wals[^2]);
            var m = Assert.ThrowsExactly<RocksDbException>(() => OpenManaged(copy.Path, ManagedOptions(track: true)).Dispose());
            StringAssert.Contains(m.Message, "Mismatched last sequence number recorded in the WAL of log number " + wals[^2]);
        }
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, NativeOptions(track: true)))
        {
            Assert.AreEqual("w7", db.Get("c7", db.GetColumnFamily("cf1")));
            Assert.AreEqual("2", db.Get("b2", db.GetColumnFamily("cf1")));
            Assert.AreEqual("x", db.Get("after"));
            Assert.AreEqual("y", db.Get("after1", db.GetColumnFamily("cf1")));
        }
    }

    [TestMethod]
    public void NativePredecessorRecordsAreVerifiedByManaged()
    {
        using var dir = new TempDir("walpred-n2m");
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, NativeOptions(track: true)))
        {
            RocksDbSharp.ColumnFamilyHandle cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 50; i++)
            {
                db.Put("d" + i, "v" + i);
                db.Put("c" + i, "w" + i, cf1);
            }
            var b = new RocksDbSharp.WriteBatch();
            b.Put("b1", "1").Put(U("b2"), U("2"), cf1).Put("b3", "3");
            db.Write(b);
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().Handle, db.GetColumnFamily("default").Handle);
            db.Put("after", "x");
            db.Put("after1", "y", cf1);
        }
        List<ulong> wals = Wals(dir.Path);
        Assert.IsTrue(wals.Count >= 2);
        // Byte-identical contents: native records what the managed writer would (number, size, first seq of the last batch).
        PredecessorWalInfo recorded = PredecessorRecordOf(dir.Path, wals[^1], out byte type);
        Assert.AreEqual((byte)LogRecordType.PredecessorWalInfo, type);
        Assert.AreEqual(Observed(dir.Path, wals[^2]), recorded);

        using (var copy = new TempDir("walpred-n2m-missing"))
        {
            CopyDb(dir.Path, copy.Path);
            File.Delete(FileNames.LogFileName(copy.Path, wals[^2]));
            var m = Assert.ThrowsExactly<RocksDbException>(() => OpenManaged(copy.Path, ManagedOptions(track: true)).Dispose());
            Assert.AreEqual(StatusCode.Corruption, m.Code);
            StringAssert.Contains(m.Message, "Missing WAL of log number " + wals[^2]);
            // Without the option the record is ignored (the missing WAL only held flushed data of the default CF and cf1's).
            DbOptions noTrack = ManagedOptions(track: false);
            noTrack.WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery;
            using RocksDatabase lenient = OpenManaged(copy.Path, noTrack);
            Assert.AreEqual("x", Encoding.UTF8.GetString(lenient.Get(U("after"))!));
        }
        using (RocksDatabase db = OpenManaged(dir.Path, ManagedOptions(track: true)))
        {
            Assert.AreEqual("w7", Encoding.UTF8.GetString(db.GetColumnFamily("cf1").Get(U("c7"))!));
            Assert.AreEqual("y", Encoding.UTF8.GetString(db.GetColumnFamily("cf1").Get(U("after1"))!));
        }
    }

    /// <summary>
    /// With recycle_log_file_num native writes kRecyclePredecessorWALInfoType records (recyclable format); managed reads
    /// and verifies them. (The managed DB does not recycle WALs itself.)
    /// </summary>
    [TestMethod]
    public void NativeRecycledWalPredecessorRecordsOpenInManaged()
    {
        using var dir = new TempDir("walpred-recycle");
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, NativeOptions(track: true, recycle: 2)))
        {
            RocksDbSharp.ColumnFamilyHandle cf1 = db.GetColumnFamily("cf1");
            for (int round = 0; round < 4; round++)
            {
                for (int i = 0; i < 20; i++) db.Put("k" + i, "v" + round);
                db.Put("c" + round, "w" + round, cf1);
                RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().Handle, db.GetColumnFamily("default").Handle);
            }
            db.Put("last", "z");
        }
        bool sawRecycle = false;
        foreach (ulong n in Wals(dir.Path))
        {
            foreach ((byte t, _) in PhysicalRecords(File.ReadAllBytes(FileNames.LogFileName(dir.Path, n))))
            {
                sawRecycle |= t == (byte)LogRecordType.RecyclePredecessorWalInfo;
            }
        }
        Assert.IsTrue(sawRecycle, "native wrote recyclable predecessor records");
        DbOptions mo = ManagedOptions(track: true);
        mo.WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery;
        using RocksDatabase m = OpenManaged(dir.Path, mo);
        Assert.AreEqual("v3", Encoding.UTF8.GetString(m.Get(U("k5"))!));
        Assert.AreEqual("w2", Encoding.UTF8.GetString(m.GetColumnFamily("cf1").Get(U("c2"))!));
        Assert.AreEqual("z", Encoding.UTF8.GetString(m.Get(U("last"))!));
    }

    [TestMethod]
    public void TrackAndVerifyWalsInNativeOptionsFile()
    {
        using var dir = new TempDir("walpred-opts");
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, NativeOptions(track: true))) db.Put("a", "1");
        OptionsFile f = OptionsFile.Load(LocalFileSystem.Instance, OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir.Path)!, preserveUnknownOptions: true);
        Assert.IsTrue(f.DbOptions.TrackAndVerifyWals);
        // The managed OPTIONS file is read back by native (LoadLatestOptions through a strict open of the same DB).
        using (RocksDatabase m = OpenManaged(dir.Path, ManagedOptions(track: true))) m.Put(U("b"), U("2"));
        string text = File.ReadAllText(OptionsFile.FindLatestOptionsFile(LocalFileSystem.Instance, dir.Path)!);
        StringAssert.Contains(text, "track_and_verify_wals=true");
        using RocksDbSharp.RocksDb n = OpenNative(dir.Path, NativeOptions(track: true));
        Assert.AreEqual("2", n.Get("b"));
    }

    // ---- WAL filter ------------------------------------------------------------------------------------------------

    /// <summary>What a filter saw: (log number, batch bytes) per record, and the CF log number / name maps.</summary>
    private sealed class FilterLog
    {
        public readonly List<(ulong Log, byte[] Batch)> Records = [];
        public readonly SortedDictionary<uint, ulong> CfLogNumbers = new();
        public readonly SortedDictionary<string, uint> CfNameIds = new(StringComparer.Ordinal);
        public int IgnoreIndex = -1;
    }

    private sealed class RecordingFilter(FilterLog log) : WalFilter
    {
        public override string Name => "RecordingFilter";

        public override void ColumnFamilyLogNumberMap(IReadOnlyDictionary<uint, ulong> columnFamilyLogNumbers, IReadOnlyDictionary<string, uint> columnFamilyNameIds)
        {
            foreach (KeyValuePair<uint, ulong> kv in columnFamilyLogNumbers) log.CfLogNumbers[kv.Key] = kv.Value;
            foreach (KeyValuePair<string, uint> kv in columnFamilyNameIds) log.CfNameIds[kv.Key] = kv.Value;
        }

        public override WalProcessingOption LogRecordFound(ulong logNumber, string logFileName, WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            log.Records.Add((logNumber, batch.Data.ToArray()));
            return log.Records.Count - 1 == log.IgnoreIndex ? WalProcessingOption.IgnoreCurrentRecord : WalProcessingOption.ContinueProcessing;
        }
    }

    private static FilterLog? s_nativeLog;
    private static readonly IntPtr s_filterName = Marshal.StringToHGlobalAnsi("RecordingFilter");

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void NativeDestructor(IntPtr state) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static IntPtr NativeName(IntPtr state) => s_filterName;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void NativeCfMap(IntPtr state, uint* ids, ulong* logNumbers, nuint count, byte** names, nuint* nameLengths, uint* nameIds, nuint nameCount)
    {
        FilterLog log = s_nativeLog!;
        for (nuint i = 0; i < count; i++) log.CfLogNumbers[ids[i]] = logNumbers[i];
        for (nuint i = 0; i < nameCount; i++) log.CfNameIds[Encoding.UTF8.GetString(names[i], (int)nameLengths[i])] = nameIds[i];
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int NativeRecordFound(IntPtr state, ulong logNumber, byte* fileName, nuint fileNameLength, IntPtr batch, IntPtr newBatch, byte* batchChanged)
    {
        FilterLog log = s_nativeLog!;
        IntPtr data = RocksDbSharp.Native.Instance.rocksdb_writebatch_data(batch, out UIntPtr size);
        log.Records.Add((logNumber, new ReadOnlySpan<byte>((void*)data, (int)size).ToArray()));
        *batchChanged = 0;
        return log.Records.Count - 1 == log.IgnoreIndex ? (int)WalProcessingOption.IgnoreCurrentRecord : (int)WalProcessingOption.ContinueProcessing;
    }

    [TestMethod]
    public void WalFilterSeesTheSameRecordsAsNative()
    {
        using var dir = new TempDir("walfilter-src");
        using (RocksDbSharp.RocksDb db = OpenNative(dir.Path, NativeOptions(track: false)))
        {
            RocksDbSharp.ColumnFamilyHandle cf1 = db.GetColumnFamily("cf1");
            for (int i = 0; i < 3; i++)
            {
                var b = new RocksDbSharp.WriteBatch();
                b.Put("p" + i, "v" + i).Put(U("q" + i), U("w" + i), cf1);
                db.Write(b);
            }
            RocksDbSharp.Native.Instance.rocksdb_flush_cf(db.Handle, new RocksDbSharp.FlushOptions().Handle, db.GetColumnFamily("default").Handle);
            for (int i = 3; i < 6; i++)
            {
                var b = new RocksDbSharp.WriteBatch();
                b.Put("p" + i, "v" + i).Put(U("q" + i), U("w" + i), cf1);
                db.Write(b);
            }
        }
        const int ignore = 4;
        using var nativeCopy = new TempDir("walfilter-native");
        using var managedCopy = new TempDir("walfilter-managed");
        CopyDb(dir.Path, nativeCopy.Path);
        CopyDb(dir.Path, managedCopy.Path);

        var nativeLog = new FilterLog { IgnoreIndex = ignore };
        s_nativeLog = nativeLog;
        RocksDbSharp.DbOptions no = NativeOptions(track: false);
        IntPtr filter = RocksDbSharp.Native.Instance.rocksdb_walfilter_create(IntPtr.Zero,
            (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, void>)&NativeDestructor,
            (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, uint*, ulong*, nuint, byte**, nuint*, uint*, nuint, void>)&NativeCfMap,
            (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, ulong, byte*, nuint, IntPtr, IntPtr, byte*, int>)&NativeRecordFound,
            (IntPtr)(delegate* unmanaged[Cdecl]<IntPtr, IntPtr>)&NativeName);
        RocksDbSharp.Native.Instance.rocksdb_options_set_wal_filter(no.Handle, filter);
        var nativeResult = new List<string?>();
        using (RocksDbSharp.RocksDb db = OpenNative(nativeCopy.Path, no))
        {
            for (int i = 0; i < 6; i++)
            {
                nativeResult.Add(db.Get("p" + i));
                nativeResult.Add(db.Get("q" + i, db.GetColumnFamily("cf1")));
            }
        }
        RocksDbSharp.Native.Instance.rocksdb_options_clear_wal_filter(no.Handle);
        RocksDbSharp.Native.Instance.rocksdb_walfilter_destroy(filter);
        s_nativeLog = null;

        var managedLog = new FilterLog { IgnoreIndex = ignore };
        DbOptions mo = ManagedOptions(track: false);
        mo.WalFilter = new RecordingFilter(managedLog);
        var managedResult = new List<string?>();
        using (RocksDatabase db = OpenManaged(managedCopy.Path, mo))
        {
            for (int i = 0; i < 6; i++)
            {
                byte[]? p = db.Get(U("p" + i));
                byte[]? q = db.GetColumnFamily("cf1").Get(U("q" + i));
                managedResult.Add(p is null ? null : Encoding.UTF8.GetString(p));
                managedResult.Add(q is null ? null : Encoding.UTF8.GetString(q));
            }
        }

        Assert.AreEqual(6, nativeLog.Records.Count, "the WAL holding cf1's data is replayed whole");
        CollectionAssert.AreEqual(new List<KeyValuePair<uint, ulong>>(nativeLog.CfLogNumbers), new List<KeyValuePair<uint, ulong>>(managedLog.CfLogNumbers));
        CollectionAssert.AreEqual(new List<KeyValuePair<string, uint>>(nativeLog.CfNameIds), new List<KeyValuePair<string, uint>>(managedLog.CfNameIds));
        Assert.AreEqual(nativeLog.Records.Count, managedLog.Records.Count);
        for (int i = 0; i < nativeLog.Records.Count; i++)
        {
            Assert.AreEqual(nativeLog.Records[i].Log, managedLog.Records[i].Log, "record " + i);
            CollectionAssert.AreEqual(nativeLog.Records[i].Batch, managedLog.Records[i].Batch, "record " + i);
        }
        CollectionAssert.AreEqual(nativeResult, managedResult);
        Assert.IsNull(managedResult[2 * ignore], "the ignored record's keys are not recovered");
        Assert.IsNull(managedResult[2 * ignore + 1]);
        Assert.AreEqual("v1", managedResult[2], "records already flushed for the default CF are still recovered from SSTs");
    }
}
