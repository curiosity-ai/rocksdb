using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// User-defined timestamps across implementations. The native side uses a comparator created with
/// rocksdb_comparator_with_ts_create named "leveldb.BytewiseComparator.u64ts" (the same order as
/// BytewiseComparatorWithU64Ts), the managed side <see cref="Comparator.BytewiseWithU64Ts"/>. Both sides read the
/// same DB at several timestamps (point lookups with the returned timestamp, forward and reverse scans, iter_start_ts
/// scans) and must agree, after native writes (memtable/WAL, flushed and compacted files, full_history_ts_low GC) and
/// after managed writes on top.
/// </summary>
[TestClass]
public sealed unsafe class TimestampCompatTests
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private static readonly ulong[] s_readTimestamps = [0, 5, 10, 15, 20, 25, 30, 45, 60, 75, 100, 150, ulong.MaxValue];

    // ---- native comparator (kept alive for the process) ---------------------------------------------------------

    private static readonly RocksDbSharp.DestructorDelegate s_destructor = static _ => { };
    private static readonly RocksDbSharp.CompareDelegate s_compare = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.Compare(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly RocksDbSharp.CompareTsDelegate s_compareTs = static (_, a, alen, b, blen) =>
        Comparator.BytewiseWithU64Ts.CompareTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), new ReadOnlySpan<byte>((void*)b, (int)blen));
    private static readonly RocksDbSharp.CompareWithoutTsDelegate s_compareWithoutTs = static (_, a, alen, aHasTs, b, blen, bHasTs) =>
        Comparator.BytewiseWithU64Ts.CompareWithoutTimestamp(new ReadOnlySpan<byte>((void*)a, (int)alen), aHasTs != 0,
            new ReadOnlySpan<byte>((void*)b, (int)blen), bHasTs != 0);
    private static readonly IntPtr s_name = Marshal.StringToHGlobalAnsi("leveldb.BytewiseComparator.u64ts");
    private static readonly RocksDbSharp.NameDelegate s_nameDelegate = static _ => s_name;
    private static readonly Lazy<IntPtr> s_comparator = new(static () =>
        N.rocksdb_comparator_with_ts_create(IntPtr.Zero, s_destructor, s_compare, s_compareTs, s_compareWithoutTs, s_nameDelegate, (UIntPtr)8));
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] Ts(ulong t)
    {
        byte[] b = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(b, t);
        return b;
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static string Key(int i) => "key" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);

    private static RocksDbSharp.RocksDb OpenNative(string path)
    {
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        N.rocksdb_options_set_comparator(cf.Handle, s_comparator.Value);
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false)).SetFormatVersion(5);
        cf.SetBlockBasedTableFactory(bbto);
        cf.SetDisableAutoCompactions(1);
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        s_keepAlive.Add(cf);
        s_keepAlive.Add(o);
        s_keepAlive.Add(bbto);
        return RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies(cf));
    }

    private static RocksDatabase OpenManaged(string path)
    {
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true };
        cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        return RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfo);
    }

    private static void Check(IntPtr err)
    {
        if (err != IntPtr.Zero)
        {
            string msg = Marshal.PtrToStringAnsi(err) ?? "";
            N.rocksdb_free(err);
            Assert.Fail("native error: " + msg);
        }
    }

    private static readonly RocksDbSharp.WriteOptions s_nativeWrite = new();

    private static void NativePut(RocksDbSharp.RocksDb db, string key, ulong ts, string value)
    {
        byte[] k = B(key), v = B(value), t = Ts(ts);
        fixed (byte* pt = t)
        {
            N.rocksdb_put_with_ts(db.Handle, s_nativeWrite.Handle, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8, v, (UIntPtr)v.Length, out IntPtr err);
            Check(err);
        }
    }

    private static void NativeDelete(RocksDbSharp.RocksDb db, string key, ulong ts)
    {
        byte[] k = B(key), t = Ts(ts);
        fixed (byte* pt = t)
        {
            N.rocksdb_delete_with_ts(db.Handle, s_nativeWrite.Handle, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8, out IntPtr err);
            Check(err);
        }
    }

    /// <summary>
    /// DeleteRange with a timestamp (the C API has no such call): the batch is built by the managed WriteBatch, whose
    /// rep is byte-identical to native WriteBatch::DeleteRange(cf, begin, end, ts), and written as is.
    /// </summary>
    private static void NativeDeleteRange(RocksDbSharp.RocksDb db, string begin, string end, ulong ts)
    {
        using var mb = new WriteBatch();
        mb.DeleteRange(0, B(begin), B(end), Ts(ts));
        byte[] rep = mb.Data.ToArray();
        IntPtr wb = N.rocksdb_writebatch_create_from(rep, (UIntPtr)rep.Length);
        try
        {
            N.rocksdb_write(db.Handle, s_nativeWrite.Handle, wb, out IntPtr err);
            Check(err);
        }
        finally
        {
            N.rocksdb_writebatch_destroy(wb);
        }
    }

    /// <summary>A few range deletions over [0, keys) at timestamps around <paramref name="baseTs"/>.</summary>
    private static void WriteRangeDeletions(int keys, ulong baseTs, Action<string, string, ulong> deleteRange)
    {
        var rnd = new Random(4321 + (int)baseTs);
        for (int r = 0; r < 4; r++)
        {
            int a = rnd.Next(keys), b = a + rnd.Next(1, keys / 4);
            deleteRange(Key(a), Key(b), baseTs + (ulong)rnd.Next(5, 40));
        }
    }

    private static void NativeCompact(RocksDbSharp.RocksDb db, ulong? tsLow)
    {
        IntPtr opt = N.rocksdb_compactoptions_create();
        IntPtr ts = IntPtr.Zero;
        try
        {
            N.rocksdb_compactoptions_set_bottommost_level_compaction(opt, 2); // kForce
            if (tsLow is ulong low)
            {
                ts = Marshal.AllocHGlobal(8);
                Ts(low).CopyTo(new Span<byte>((void*)ts, 8));
                N.rocksdb_compactoptions_set_full_history_ts_low(opt, ts, (UIntPtr)8);
            }
            N.rocksdb_compact_range_opt(db.Handle, opt, (byte[]?)null, UIntPtr.Zero, (byte[]?)null, UIntPtr.Zero);
        }
        finally
        {
            N.rocksdb_compactoptions_destroy(opt);
            if (ts != IntPtr.Zero) Marshal.FreeHGlobal(ts);
        }
    }

    private static ulong? NativeFullHistoryTsLow(RocksDbSharp.RocksDb db)
    {
        IntPtr p = N.rocksdb_get_full_history_ts_low(db.Handle, db.GetDefaultColumnFamily().Handle, out UIntPtr len, out IntPtr err);
        Check(err);
        if (p == IntPtr.Zero || len == UIntPtr.Zero) return null;
        ulong v = BinaryPrimitives.ReadUInt64LittleEndian(new ReadOnlySpan<byte>((void*)p, 8));
        N.rocksdb_free(p);
        return v;
    }

    // ---- dumps --------------------------------------------------------------------------------------------------

    private static string Entry(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value, ReadOnlySpan<byte> ts, bool deletion) =>
        Encoding.UTF8.GetString(userKey) + "=" + Encoding.UTF8.GetString(value) + "@" + BinaryPrimitives.ReadUInt64LittleEndian(ts) + (deletion ? "(del)" : "");

    private static bool IsDeletion(ReadOnlySpan<byte> internalKey)
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out _, out EntryType t);
        return t is EntryType.Deletion or EntryType.DeletionWithTimestamp or EntryType.SingleDeletion;
    }

    private static List<string> NativeDump(RocksDbSharp.RocksDb db, int keys, ulong minReadTs)
    {
        var dump = new List<string>();
        IntPtr tsBuf = Marshal.AllocHGlobal(16);
        try
        {
            foreach (ulong readTs in s_readTimestamps)
            {
                if (readTs < minReadTs) continue; // reads below full_history_ts_low are rejected
                Ts(readTs).CopyTo(new Span<byte>((void*)tsBuf, 8));
                var ro = new RocksDbSharp.ReadOptions();
                N.rocksdb_readoptions_set_timestamp(ro.Handle, tsBuf, (UIntPtr)8);
                for (int i = 0; i < keys; i++)
                {
                    byte[] k = B(Key(i));
                    IntPtr tsOut = IntPtr.Zero;
                    IntPtr v = N.rocksdb_get_with_ts(db.Handle, ro.Handle, k, (UIntPtr)k.Length, out UIntPtr vlen, (IntPtr)(&tsOut), out UIntPtr tslen, out IntPtr err);
                    Check(err);
                    if (v == IntPtr.Zero)
                    {
                        dump.Add("get@" + readTs + " " + Key(i) + " -");
                        continue;
                    }
                    dump.Add("get@" + readTs + " " + Entry(k, new ReadOnlySpan<byte>((void*)v, (int)vlen), new ReadOnlySpan<byte>((void*)tsOut, (int)tslen), false));
                    N.rocksdb_free(v);
                    N.rocksdb_free(tsOut);
                }
                using (RocksDbSharp.Iterator it = db.NewIterator(null, ro))
                {
                    for (it.SeekToFirst(); it.Valid(); it.Next()) dump.Add("fwd@" + readTs + " " + Entry(it.Key(), it.Value(), NativeIterTs(it), false));
                    for (it.SeekToLast(); it.Valid(); it.Prev()) dump.Add("rev@" + readTs + " " + Entry(it.Key(), it.Value(), NativeIterTs(it), false));
                    it.Seek(B(Key(keys / 2)));
                    if (it.Valid()) dump.Add("seek@" + readTs + " " + Entry(it.Key(), it.Value(), NativeIterTs(it), false));
                    it.SeekForPrev(B(Key(keys / 3)));
                    if (it.Valid()) dump.Add("seekprev@" + readTs + " " + Entry(it.Key(), it.Value(), NativeIterTs(it), false));
                }
                if (readTs != ulong.MaxValue && readTs >= 20)
                {
                    // iter_start_ts: every version in [readTs - 20, readTs]; key() is the internal key.
                    Ts(readTs - 20).CopyTo(new Span<byte>((void*)(tsBuf + 8), 8));
                    var ro2 = new RocksDbSharp.ReadOptions();
                    N.rocksdb_readoptions_set_timestamp(ro2.Handle, tsBuf, (UIntPtr)8);
                    N.rocksdb_readoptions_set_iter_start_ts(ro2.Handle, tsBuf + 8, (UIntPtr)8);
                    using RocksDbSharp.Iterator it = db.NewIterator(null, ro2);
                    for (it.SeekToFirst(); it.Valid(); it.Next())
                    {
                        byte[] ik = it.Key();
                        ReadOnlySpan<byte> uk = DbFormat.ExtractUserKey(ik);
                        dump.Add("all@" + readTs + " " + Entry(uk[..^8], it.Value(), uk[^8..], IsDeletion(ik)));
                    }
                }
            }
        }
        finally
        {
            Marshal.FreeHGlobal(tsBuf);
        }
        return dump;
    }

    private static ReadOnlySpan<byte> NativeIterTs(RocksDbSharp.Iterator it)
    {
        IntPtr p = N.rocksdb_iter_timestamp(it.Handle, out UIntPtr len);
        return new ReadOnlySpan<byte>((void*)p, (int)len);
    }

    private static List<string> ManagedDump(RocksDatabase db, int keys, ulong minReadTs)
    {
        var dump = new List<string>();
        foreach (ulong readTs in s_readTimestamps)
        {
            if (readTs < minReadTs) continue; // reads below full_history_ts_low are rejected
            var ro = new ReadOptions { Timestamp = Ts(readTs) };
            for (int i = 0; i < keys; i++)
            {
                var v = new ArrayBufferWriter<byte>();
                var t = new ArrayBufferWriter<byte>();
                byte[] k = B(Key(i));
                dump.Add(db.TryGet(ro, k, v, t) ? "get@" + readTs + " " + Entry(k, v.WrittenSpan, t.WrittenSpan, false) : "get@" + readTs + " " + Key(i) + " -");
            }
            using (Iterator it = db.NewIterator(ro))
            {
                for (it.SeekToFirst(); it.Valid; it.Next()) dump.Add("fwd@" + readTs + " " + Entry(it.Key, it.Value, it.Timestamp, false));
                for (it.SeekToLast(); it.Valid; it.Prev()) dump.Add("rev@" + readTs + " " + Entry(it.Key, it.Value, it.Timestamp, false));
                it.Seek(B(Key(keys / 2)));
                if (it.Valid) dump.Add("seek@" + readTs + " " + Entry(it.Key, it.Value, it.Timestamp, false));
                it.SeekForPrev(B(Key(keys / 3)));
                if (it.Valid) dump.Add("seekprev@" + readTs + " " + Entry(it.Key, it.Value, it.Timestamp, false));
            }
            if (readTs != ulong.MaxValue && readTs >= 20)
            {
                using Iterator it = db.NewIterator(new ReadOptions { Timestamp = Ts(readTs), IterStartTs = Ts(readTs - 20) });
                for (it.SeekToFirst(); it.Valid; it.Next()) dump.Add("all@" + readTs + " " + Entry(it.Key, it.Value, it.Timestamp, IsDeletion(it.InternalKey)));
            }
        }
        return dump;
    }

    private static void AssertSameDump(List<string> expected, List<string> actual, string what)
    {
        for (int i = 0; i < Math.Min(expected.Count, actual.Count); i++)
        {
            if (expected[i] != actual[i])
            {
                var ctx = new StringBuilder();
                for (int j = Math.Max(0, i - 6); j < Math.Min(Math.Min(expected.Count, actual.Count), i + 6); j++) ctx.Append("\n  ").Append(expected[j]).Append("  |  ").Append(actual[j]);
                Assert.Fail(what + ": first difference at line " + i + ": expected '" + expected[i] + "', actual '" + actual[i] + "'" + ctx);
            }
        }
        Assert.AreEqual(expected.Count, actual.Count, what + ": line count");
    }

    /// <summary>Deterministic history: per key a few puts and deletes at increasing timestamps (offset by <paramref name="baseTs"/>).</summary>
    private static void WriteHistory(int keys, ulong baseTs, Action<string, ulong, string> put, Action<string, ulong> delete)
    {
        var rnd = new Random(1234 + (int)baseTs);
        for (int i = 0; i < keys; i++)
        {
            ulong ts = baseTs + (ulong)rnd.Next(1, 8);
            int versions = rnd.Next(1, 5);
            for (int v = 0; v < versions; v++)
            {
                if (rnd.Next(5) == 0) delete(Key(i), ts);
                else put(Key(i), ts, "v" + i + "-" + ts);
                ts += (ulong)rnd.Next(1, 12);
            }
        }
    }

    [TestMethod]
    public void NativeUdtDbReadsIdenticallyInManagedThenManagedWritesReadByNative()
    {
        using var dir = new TempDir("udt-n2m");
        const int Keys = 120;
        List<string> nativeDump;
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            WriteHistory(Keys, 0, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            WriteHistory(Keys, 30, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            // Compaction with full_history_ts_low = 25: older versions collapsed, seqno/timestamp zeroed.
            NativeCompact(native, 25);
            Assert.AreEqual(25UL, NativeFullHistoryTsLow(native));
            // More history left in the memtable / WAL.
            WriteHistory(Keys, 60, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            nativeDump = NativeDump(native, Keys, 25);
            // Sanity: the dump covers values, deletions (iter_start_ts) and versions at zeroed timestamps.
            Assert.IsTrue(nativeDump.Count > 3000, "dump size " + nativeDump.Count);
            Assert.IsTrue(nativeDump.Exists(static l => l.EndsWith("(del)", StringComparison.Ordinal)));
            Assert.IsTrue(nativeDump.Exists(static l => l.StartsWith("fwd@", StringComparison.Ordinal) && l.EndsWith("@0", StringComparison.Ordinal)));
        }

        List<string> managedDump;
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            Assert.AreEqual(25UL, BinaryPrimitives.ReadUInt64LittleEndian(db.GetFullHistoryTsLow()));
            managedDump = ManagedDump(db, Keys, 25);
            AssertSameDump(nativeDump, managedDump, "managed reading native DB (WAL + files)");
            db.Flush();
            AssertSameDump(nativeDump, ManagedDump(db, Keys, 25), "managed after flushing the recovered WAL");

            // Managed writes on top, flush, and a compaction raising full_history_ts_low.
            WriteHistory(Keys, 90, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            db.Flush();
            db.CompactRange(new CompactRangeOptions { FullHistoryTsLow = Ts(70), BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            WriteHistory(Keys, 120, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            managedDump = ManagedDump(db, Keys, 70);
        }

        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            Assert.AreEqual(70UL, NativeFullHistoryTsLow(native));
            AssertSameDump(managedDump, NativeDump(native, Keys, 70), "native reading managed writes (WAL + files)");
            NativeCompact(native, null);
            AssertSameDump(managedDump, NativeDump(native, Keys, 70), "native after compacting managed files");
        }
    }

    [TestMethod]
    public void ManagedUdtDbReadsIdenticallyInNative()
    {
        using var dir = new TempDir("udt-m2n");
        const int Keys = 150;
        List<string> managedDump;
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            WriteHistory(Keys, 0, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            db.Flush();
            WriteHistory(Keys, 30, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            db.CompactRange();
            WriteHistory(Keys, 60, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            db.Flush();
            db.IncreaseFullHistoryTsLow(Ts(40));
            WriteHistory(Keys, 100, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            managedDump = ManagedDump(db, Keys, 40);
        }
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            Assert.AreEqual(40UL, NativeFullHistoryTsLow(native));
            AssertSameDump(managedDump, NativeDump(native, Keys, 40), "native reading managed DB");
            // Native compaction with GC of the managed-written files, then managed reads the result again.
            NativeCompact(native, 60);
            managedDump = NativeDump(native, Keys, 60);
        }
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            AssertSameDump(managedDump, ManagedDump(db, Keys, 60), "managed reading native-compacted DB");
        }
    }

    [TestMethod]
    public void NativeRangeDeletionsWithTimestampsReadIdenticallyBothWays()
    {
        using var dir = new TempDir("udt-rdel");
        const int Keys = 120;
        List<string> nativeDump;
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            WriteHistory(Keys, 0, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            WriteRangeDeletions(Keys, 0, (b, e, t) => NativeDeleteRange(native, b, e, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            WriteHistory(Keys, 30, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            WriteRangeDeletions(Keys, 30, (b, e, t) => NativeDeleteRange(native, b, e, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            // Compaction keeping every version (no full_history_ts_low): the tombstones are rewritten.
            NativeCompact(native, null);
            WriteHistory(Keys, 60, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            WriteRangeDeletions(Keys, 60, (b, e, t) => NativeDeleteRange(native, b, e, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            WriteRangeDeletions(Keys, 90, (b, e, t) => NativeDeleteRange(native, b, e, t)); // memtable / WAL only
            nativeDump = NativeDump(native, Keys, 0);
        }
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            AssertSameDump(nativeDump, ManagedDump(db, Keys, 0), "managed reading native range deletions (WAL + files)");
            db.Flush();
            AssertSameDump(nativeDump, ManagedDump(db, Keys, 0), "managed after flushing the recovered range deletions");
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            AssertSameDump(nativeDump, ManagedDump(db, Keys, 0), "managed after compacting native range deletions");
        }
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            AssertSameDump(nativeDump, NativeDump(native, Keys, 0), "native reading managed-rewritten range deletions");
            // GC with full_history_ts_low: tombstones and covered versions below it may be dropped by native.
            NativeCompact(native, 70);
            nativeDump = NativeDump(native, Keys, 70);
        }
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            AssertSameDump(nativeDump, ManagedDump(db, Keys, 70), "managed reading native GC of range deletions");
        }
    }

    [TestMethod]
    public void ManagedRangeDeletionsWithTimestampsReadIdenticallyInNative()
    {
        using var dir = new TempDir("udt-rdel-m");
        const int Keys = 120;
        List<string> managedDump;
        using (RocksDatabase db = OpenManaged(dir.Path))
        {
            WriteHistory(Keys, 0, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            WriteRangeDeletions(Keys, 0, (b, e, t) => db.DeleteRange(B(b), B(e), Ts(t)));
            db.Flush();
            WriteHistory(Keys, 30, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            WriteRangeDeletions(Keys, 30, (b, e, t) => db.DeleteRange(B(b), B(e), Ts(t)));
            db.Flush();
            db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            WriteHistory(Keys, 60, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            WriteRangeDeletions(Keys, 60, (b, e, t) => db.DeleteRange(B(b), B(e), Ts(t)));
            db.Flush();
            db.CompactRange(new CompactRangeOptions { FullHistoryTsLow = Ts(50), BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
            WriteRangeDeletions(Keys, 90, (b, e, t) => db.DeleteRange(B(b), B(e), Ts(t)));
            managedDump = ManagedDump(db, Keys, 50);
            Assert.IsTrue(managedDump.Exists(static l => l.EndsWith(" -", StringComparison.Ordinal)));
        }
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            AssertSameDump(managedDump, NativeDump(native, Keys, 50), "native reading managed range deletions (WAL + files)");
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            NativeCompact(native, null);
            AssertSameDump(managedDump, NativeDump(native, Keys, 50), "native after compacting managed range deletions");
        }
    }

    /// <summary>
    /// DeleteRangeGetIteratorWithSnapshot across implementations: keys k0..k3 at ts 0..3, a tombstone [k1, k3) at ts 1
    /// written between k1 and k2, a snapshot before and after it, a flush in the middle. Native and managed must
    /// return the same Get / scan results for every (snapshot, read timestamp) pair.
    /// </summary>
    [TestMethod]
    public void RangeDeletionWithTimestampAndSnapshotsMatchesNative()
    {
        using var dir = new TempDir("udt-rdel-snap");
        var nativeResults = new List<string>();
        var managedResults = new List<string>();
        using (RocksDbSharp.RocksDb native = OpenNative(dir.Path))
        {
            IntPtr before = IntPtr.Zero;
            for (int i = 0; i < 4; i++)
            {
                NativePut(native, Key(i), (ulong)i, "val" + i);
                if (i == 1)
                {
                    before = N.rocksdb_create_snapshot(native.Handle);
                    NativeDeleteRange(native, Key(1), Key(3), 1);
                }
                if (i == 2) native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            }
            IntPtr after = N.rocksdb_create_snapshot(native.Handle);
            IntPtr tsBuf = Marshal.AllocHGlobal(8);
            try
            {
                foreach (IntPtr snap in new[] { before, after })
                {
                    foreach (ulong readTs in new ulong[] { 0, 1, 2, 4 })
                    {
                        Ts(readTs).CopyTo(new Span<byte>((void*)tsBuf, 8));
                        var ro = new RocksDbSharp.ReadOptions();
                        N.rocksdb_readoptions_set_timestamp(ro.Handle, tsBuf, (UIntPtr)8);
                        N.rocksdb_readoptions_set_snapshot(ro.Handle, snap);
                        string tag = (snap == before ? "before" : "after") + "@" + readTs + " ";
                        for (int i = 0; i < 4; i++)
                        {
                            byte[] k = B(Key(i));
                            IntPtr tsOut = IntPtr.Zero;
                            IntPtr v = N.rocksdb_get_with_ts(native.Handle, ro.Handle, k, (UIntPtr)k.Length, out UIntPtr vlen, (IntPtr)(&tsOut), out UIntPtr tslen, out IntPtr err);
                            Check(err);
                            nativeResults.Add(tag + "get " + Key(i) + " " + (v == IntPtr.Zero ? "-" : Encoding.UTF8.GetString(new ReadOnlySpan<byte>((void*)v, (int)vlen))));
                            if (v != IntPtr.Zero) N.rocksdb_free(v);
                            if (tsOut != IntPtr.Zero) N.rocksdb_free(tsOut);
                        }
                        using RocksDbSharp.Iterator it = native.NewIterator(null, ro);
                        for (it.SeekToFirst(); it.Valid(); it.Next()) nativeResults.Add(tag + "fwd " + Entry(it.Key(), it.Value(), NativeIterTs(it), false));
                        for (it.SeekToLast(); it.Valid(); it.Prev()) nativeResults.Add(tag + "rev " + Entry(it.Key(), it.Value(), NativeIterTs(it), false));
                    }
                }
            }
            finally
            {
                Marshal.FreeHGlobal(tsBuf);
                N.rocksdb_release_snapshot(native.Handle, before);
                N.rocksdb_release_snapshot(native.Handle, after);
            }
        }
        using (var dir2 = new TempDir("udt-rdel-snap-m"))
        using (RocksDatabase db = OpenManaged(dir2.Path))
        {
            Snapshot? before = null;
            for (int i = 0; i < 4; i++)
            {
                db.Put(B(Key(i)), Ts((ulong)i), B("val" + i));
                if (i == 1)
                {
                    before = db.GetSnapshot();
                    db.DeleteRange(B(Key(1)), B(Key(3)), Ts(1));
                }
                if (i == 2) db.Flush();
            }
            using Snapshot after = db.GetSnapshot();
            using Snapshot b = before!;
            foreach (Snapshot snap in new[] { b, after })
            {
                foreach (ulong readTs in new ulong[] { 0, 1, 2, 4 })
                {
                    var ro = new ReadOptions { Snapshot = snap, Timestamp = Ts(readTs) };
                    string tag = (snap == b ? "before" : "after") + "@" + readTs + " ";
                    for (int i = 0; i < 4; i++)
                    {
                        byte[]? v = db.Get(ro, B(Key(i)));
                        managedResults.Add(tag + "get " + Key(i) + " " + (v is null ? "-" : Encoding.UTF8.GetString(v)));
                    }
                    using Iterator it = db.NewIterator(ro);
                    for (it.SeekToFirst(); it.Valid; it.Next()) managedResults.Add(tag + "fwd " + Entry(it.Key, it.Value, it.Timestamp, false));
                    for (it.SeekToLast(); it.Valid; it.Prev()) managedResults.Add(tag + "rev " + Entry(it.Key, it.Value, it.Timestamp, false));
                }
            }
        }
        AssertSameDump(nativeResults, managedResults, "range deletion with timestamps and snapshots");
    }

    /// <summary>
    /// Timestamped external SST files both ways: the managed SstFileWriter (Put/Delete/DeleteRange with timestamps) is
    /// ingested by native, the native one (rocksdb_sstfilewriter_put_with_ts / delete_with_ts) by managed; both sides
    /// then read the same versions at every timestamp.
    /// </summary>
    [TestMethod]
    public void TimestampedSstFilesIngestedBothWays()
    {
        using var dir = new TempDir("udt-sst");
        const int Keys = 140;
        string managedSst = Path.Combine(dir.Path, "managed.sst"), nativeSst = Path.Combine(dir.Path, "native.sst");
        string dbPath = Path.Combine(dir.Path, "db");

        // Managed writer: keys 60..99, two versions each, some deletions and a range deletion, ordered key asc / ts desc.
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts };
        using (var w = new SstFileWriter(cfo))
        {
            w.Open(managedSst);
            for (int i = 60; i < 100; i++)
            {
                if (i % 7 == 0) w.Delete(B(Key(i)), Ts(40));
                w.Put(B(Key(i)), Ts(35), B("m" + i + "-35"));
                w.Put(B(Key(i)), Ts(22), B("m" + i + "-22"));
            }
            w.DeleteRange(B(Key(80)), B(Key(90)), Ts(30));
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => w.Put(B("z"), B("v"))).Code);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => w.Put(B("z"), Ts(1)[..4], B("v"))).Code);
            w.Finish();
        }

        List<string> nativeDump;
        using (RocksDbSharp.RocksDb native = OpenNative(dbPath))
        {
            for (int i = 0; i < 60; i++) NativePut(native, Key(i), (ulong)(i % 20 + 1), "n" + i);
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            native.IngestExternalFiles([managedSst], new RocksDbSharp.IngestExternalFileOptions());

            // Native writer: keys 100..139.
            var nopts = new RocksDbSharp.ColumnFamilyOptions();
            N.rocksdb_options_set_comparator(nopts.Handle, s_comparator.Value);
            s_keepAlive.Add(nopts);
            using (var nw = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), nopts))
            {
                nw.Open(nativeSst);
                for (int i = 100; i < Keys; i++)
                {
                    byte[] k = B(Key(i)), v = B("n" + i + "-45"), t = Ts(45), dts = Ts(50);
                    fixed (byte* pk = k)
                    fixed (byte* pv = v)
                    fixed (byte* pt = t)
                    fixed (byte* pd = dts)
                    {
                        if (i % 5 == 0)
                        {
                            N.rocksdb_sstfilewriter_delete_with_ts(nw.Handle, (IntPtr)pk, (UIntPtr)k.Length, (IntPtr)pd, (UIntPtr)8, out IntPtr derr);
                            Check(derr);
                        }
                        N.rocksdb_sstfilewriter_put_with_ts(nw.Handle, (IntPtr)pk, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8, (IntPtr)pv, (UIntPtr)v.Length, out IntPtr err);
                        Check(err);
                    }
                }
                nw.Finish();
            }
            nativeDump = NativeDump(native, Keys, 0);
        }

        using (RocksDatabase db = OpenManaged(dbPath))
        {
            // Managed reads the native ingestion of the managed SST.
            List<string> before = NativeDumpFilter(nativeDump, 100);
            AssertSameDump(before, NativeDumpFilter(ManagedDump(db, Keys, 0), 100), "managed reading native-ingested managed SST");
            db.IngestExternalFiles([nativeSst], new IngestExternalFileOptions());
            AssertSameDump(nativeDump.FindAll(static l => !l.Contains("key01", StringComparison.Ordinal)),
                ManagedDump(db, Keys, 0).FindAll(static l => !l.Contains("key01", StringComparison.Ordinal)), "managed after ingesting native SST");
            nativeDump = ManagedDump(db, Keys, 0);
            Assert.IsTrue(nativeDump.Exists(static l => l.Contains("key0105=@50(del)", StringComparison.Ordinal)), "native SST deletion visible");
        }
        using (RocksDbSharp.RocksDb native = OpenNative(dbPath))
        {
            AssertSameDump(nativeDump, NativeDump(native, Keys, 0), "native reading managed-ingested native SST");
        }
    }

    /// <summary>Lines of a dump that do not concern keys &gt;= <paramref name="limit"/>.</summary>
    private static List<string> NativeDumpFilter(List<string> dump, int limit) =>
        dump.FindAll(l =>
        {
            int k = l.IndexOf(" key", StringComparison.Ordinal);
            return k < 0 || int.Parse(l.AsSpan(k + 4, 4), System.Globalization.CultureInfo.InvariantCulture) < limit;
        });

    [TestMethod]
    public void WriteBatchWithTimestampsMatchesNativeBytes()
    {
        using var dir = new TempDir("udt-wb");
        using RocksDbSharp.RocksDb native = OpenNative(dir.Path);
        using var nb = new RocksDbSharp.WriteBatch();
        IntPtr cf = native.GetDefaultColumnFamily().Handle;
        byte[] t = Ts(77);
        byte[] k = B("key"), v = B("value");
        fixed (byte* pt = t)
        {
            N.rocksdb_writebatch_put_cf_with_ts(nb.Handle, cf, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8, v, (UIntPtr)v.Length);
            N.rocksdb_writebatch_delete_cf_with_ts(nb.Handle, cf, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8);
            N.rocksdb_writebatch_singledelete_cf_with_ts(nb.Handle, cf, k, (UIntPtr)k.Length, (IntPtr)pt, (UIntPtr)8);
        }
        using var mb = new WriteBatch();
        mb.Put(0, k, t, v).Delete(0, k, t).SingleDelete(0, k, t);
        CollectionAssert.AreEqual(nb.ToBytes(), mb.Data.ToArray());
    }
}
