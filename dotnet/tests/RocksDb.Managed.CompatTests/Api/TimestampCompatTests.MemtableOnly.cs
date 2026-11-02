using System.Buffers.Binary;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// User-defined timestamps in memtable only (persist_user_defined_timestamps = false) across implementations: the
/// native side sets the option through rocksdb_options_set_persist_user_defined_timestamps (and
/// allow_concurrent_memtable_write = false, which upstream requires). Tables are written without timestamps
/// ("rocksdb.user.defined.timestamps.persisted" = 0), MANIFEST boundaries without them, the WAL with them; flushes move
/// full_history_ts_low above the flushed timestamps. Both sides must read the same DB identically, in both directions.
/// </summary>
public sealed unsafe partial class TimestampCompatTests
{
    private static RocksDbSharp.RocksDb OpenNativeMemtableOnly(string path)
    {
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        N.rocksdb_options_set_comparator(cf.Handle, s_comparator.Value);
        N.rocksdb_options_set_persist_user_defined_timestamps(cf.Handle, 0);
        var bbto = new RocksDbSharp.BlockBasedTableOptions().SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false)).SetFormatVersion(5)
            .SetBlockSize(256);
        cf.SetBlockBasedTableFactory(bbto);
        cf.SetDisableAutoCompactions(1);
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetAllowConcurrentMemtableWrite(false);
        N.rocksdb_options_set_avoid_flush_during_recovery(o.Handle, 1);
        s_keepAlive.Add(cf);
        s_keepAlive.Add(o);
        s_keepAlive.Add(bbto);
        return RocksDbSharp.RocksDb.Open(o, path, new RocksDbSharp.ColumnFamilies(cf));
    }

    private static RocksDatabase OpenManagedMemtableOnly(string path)
    {
        var cfo = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, DisableAutoCompactions = true, PersistUserDefinedTimestamps = false };
        cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        cfo.TableOptions.BlockSize = 256;
        return RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, AllowConcurrentMemtableWrite = false, AvoidFlushDuringRecovery = true }, cfo);
    }

    private static ulong? ManagedFullHistoryTsLow(RocksDatabase db) =>
        db.DefaultColumnFamily.GetFullHistoryTsLow() is { } low ? BinaryPrimitives.ReadUInt64LittleEndian(low) : null;

    private static void AssertAllTablesStripTimestamps(string path)
    {
        // Every table file must be readable by a plain reader as a table without timestamps.
        foreach (string f in Directory.GetFiles(path, "*.sst"))
        {
            using BlockBasedTableReader r = BlockBasedTableReader.Open(RocksDb.Managed.Env.LocalFileSystem.Instance.OpenRandomAccess(f),
                new TableReaderOptions { Comparator = Comparator.BytewiseWithU64Ts });
            Assert.AreEqual(0UL, r.Properties.UserDefinedTimestampsPersisted, f);
            Assert.AreEqual(8, r.PadTimestampSize, f);
        }
    }

    [TestMethod]
    public void MemtableOnlyTimestampsNativeToManagedAndBack()
    {
        using var dir = new TempDir("udt-memonly-n2m");
        const int Keys = 120;
        List<string> nativeDump;
        ulong low;
        using (RocksDbSharp.RocksDb native = OpenNativeMemtableOnly(dir.Path))
        {
            WriteHistory(Keys, 0, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            WriteRangeDeletions(Keys, 0, (b, e, t) => NativeDeleteRange(native, b, e, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            low = NativeFullHistoryTsLow(native) ?? 0;
            Assert.IsTrue(low > 30, "native full_history_ts_low " + low);
            WriteHistory(Keys / 2, 50, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            NativeCompact(native, null);
            low = NativeFullHistoryTsLow(native) ?? 0;
            // Left in the memtable / WAL with their timestamps.
            WriteHistory(Keys, 110, (k, t, v) => NativePut(native, k, t, v), (k, t) => NativeDelete(native, k, t));
            nativeDump = NativeDump(native, Keys, low);
        }
        AssertAllTablesStripTimestamps(dir.Path);

        List<string> managedDump;
        using (RocksDatabase db = OpenManagedMemtableOnly(dir.Path))
        {
            Assert.AreEqual(low, ManagedFullHistoryTsLow(db));
            AssertSameDump(nativeDump, ManagedDump(db, Keys, low), "managed read of native memtable-only DB");
            // Managed flush (stripping), compaction and more WAL data, then native reads it.
            db.Flush();
            ulong managedLow = ManagedFullHistoryTsLow(db)!.Value;
            Assert.IsTrue(managedLow > low);
            low = managedLow;
            WriteHistory(Keys / 3, 200, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            db.Flush();
            db.CompactRange();
            low = ManagedFullHistoryTsLow(db)!.Value;
            WriteHistory(Keys, 260, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            managedDump = ManagedDump(db, Keys, low);
        }
        AssertAllTablesStripTimestamps(dir.Path);
        using (RocksDbSharp.RocksDb native = OpenNativeMemtableOnly(dir.Path))
        {
            Assert.AreEqual(low, NativeFullHistoryTsLow(native));
            AssertSameDump(managedDump, NativeDump(native, Keys, low), "native read of managed memtable-only DB");
        }
    }

    [TestMethod]
    public void MemtableOnlyTimestampsManagedFirstIdenticalTablesProperties()
    {
        using var dir = new TempDir("udt-memonly-m2n");
        const int Keys = 80;
        List<string> managedDump;
        ulong low;
        using (RocksDatabase db = OpenManagedMemtableOnly(dir.Path))
        {
            WriteHistory(Keys, 0, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            WriteRangeDeletions(Keys, 0, (b, e, t) => db.DeleteRange(B(b), B(e), Ts(t)));
            db.Flush();
            low = ManagedFullHistoryTsLow(db)!.Value;
            WriteHistory(Keys, 60, (k, t, v) => db.Put(B(k), Ts(t), B(v)), (k, t) => db.Delete(B(k), Ts(t)));
            managedDump = ManagedDump(db, Keys, low);
        }
        AssertAllTablesStripTimestamps(dir.Path);
        List<string> nativeDump;
        using (RocksDbSharp.RocksDb native = OpenNativeMemtableOnly(dir.Path))
        {
            Assert.AreEqual(low, NativeFullHistoryTsLow(native));
            nativeDump = NativeDump(native, Keys, low);
            AssertSameDump(managedDump, nativeDump, "native read of managed memtable-only DB");
            // Native flush of the managed WAL data + compaction, read back by managed.
            native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            NativeCompact(native, null);
            low = NativeFullHistoryTsLow(native)!.Value;
            nativeDump = NativeDump(native, Keys, low);
        }
        using (RocksDatabase db = OpenManagedMemtableOnly(dir.Path))
        {
            Assert.AreEqual(low, ManagedFullHistoryTsLow(db));
            AssertSameDump(nativeDump, ManagedDump(db, Keys, low), "managed read after native flush/compaction");
        }
    }
}
