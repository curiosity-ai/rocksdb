using System.Buffers.Binary;
using RocksDb.Managed.Env;
using RocksDb.Managed.TestShared;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #15, Mosaik's flows through the engine code changed since audit #6 (the native differentials are in
/// tests/RocksDb.Managed.CompatTests/Audit15/Audit15MosaikFlowsCompatTests.cs). Mosaik's database is
/// <see cref="MosaikProfile"/> on a memory file system.
/// </summary>
[TestClass]
public sealed class Audit15MosaikFlowsTests
{
    private static byte[] Uid(int i)
    {
        var k = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(k, (ulong)i * 0x9E3779B97F4A7C15UL);
        BinaryPrimitives.WriteUInt64LittleEndian(k.AsSpan(8), (ulong)i);
        return k;
    }

    private static ulong SuperVersionNumber(ColumnFamily cf) => ulong.Parse(cf.GetProperty("rocksdb.current-super-version-number")!, System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// "rocksdb.current-super-version-number" is ColumnFamilyData::super_version_number_ (db/column_family.cc): per column
    /// family, 1 after the open's install, and a flush of one family (switch + install) moves only that family's number.
    /// RocksDbBase.UpdateStats sums the property over Mosaik's families; one DB-wide counter made every family report the
    /// installs of all of them.
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void SuperVersionNumberIsPerColumnFamily()
    {
        var fs = new MemoryFileSystem();
        var profile = new MosaikProfile("/journal", lowMemoryMode: true, processorCount: 2, fs, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        using (RocksDatabase db = profile.Open("/db"))
        {
            Dictionary<string, ColumnFamily> cf = MosaikProfile.Handles(db);
            foreach (MosaikProfile.Family f in MosaikProfile.Families) Assert.AreEqual(1UL, SuperVersionNumber(cf[f.Name]), f.Name);
            Assert.AreEqual(1UL, SuperVersionNumber(db.DefaultColumnFamily));

            cf["nodes"].Put(Uid(1), "v"u8);
            cf["nodes"].Flush();
            db.WaitForCompact();
            Assert.AreEqual(3UL, SuperVersionNumber(cf["nodes"]), "the memtable switch and the flush's install");
            foreach (MosaikProfile.Family f in MosaikProfile.Families)
            {
                if (f.Name != "nodes") Assert.AreEqual(1UL, SuperVersionNumber(cf[f.Name]), f.Name);
            }
            // Left in the WAL: the reopen replays it and flushes it (WriteLevel0TableForRecovery).
            cf["content"].Put(Uid(2), "w"u8);
        }
        // A reopen installs one super version per family (DB::Open), whatever recovery did; a new column family starts
        // from 0 too.
        using (RocksDatabase db = profile.Open("/db"))
        {
            Dictionary<string, ColumnFamily> cf = MosaikProfile.Handles(db);
            foreach (MosaikProfile.Family f in MosaikProfile.Families) Assert.AreEqual(1UL, SuperVersionNumber(cf[f.Name]), "reopened " + f.Name);
            Assert.AreEqual(1UL, SuperVersionNumber(db.DefaultColumnFamily), "reopened default");
            ColumnFamily added = db.CreateColumnFamily("audit15-new", new ColumnFamilyOptions());
            Assert.AreEqual(1UL, SuperVersionNumber(added));
        }
    }

    /// <summary>
    /// DBImpl::SwitchWAL marks the oldest alive WAL as getting flushed even when no column family needs it any more
    /// (outside 2PC), so the writes after it return at the mark. With file deletions disabled (RocksDbBase.RegisterReplica)
    /// such an obsolete WAL stays alive; the info LOG names the switch once, as native's does
    /// (Audit15MosaikFlowsCompatTests.SwitchWalWithFileDeletionsDisabledMarksTheOldestWalOnceLikeNative).
    /// </summary>
    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public void SwitchWalMarksAnObsoleteOldestWalOnce()
    {
        var fs = new MemoryFileSystem();
        var profile = new MosaikProfile("/journal", lowMemoryMode: true, processorCount: 2, fs, cacheBytes: 8L << 20, writeBufferBytes: 32L << 20);
        var log = new List<string>();
        profile.DbOptions.InfoLogLevel = InfoLogLevel.Info;
        profile.DbOptions.InfoLog = new CapturingLogger(log);
        using RocksDatabase db = profile.Open("/db");
        ColumnFamily content = db.GetColumnFamily("content"), nodes = db.GetColumnFamily("nodes");
        db.DisableFileDeletions();
        var value = new byte[4096];
        // Eight flushed rounds of the content family: every family's log number is past the first WALs, which stay alive.
        for (int round = 0; round < 8; round++)
        {
            for (int i = 0; i < 60; i++) content.Put(Uid(round * 1000 + i), value);
            content.Flush();
            db.WaitForCompact();
        }
        for (int i = 0; i < 200; i++) nodes.Put(Uid(100_000 + i), "v"u8);
        db.WaitForCompact();
        int switches;
        lock (log) switches = log.Count(static l => l.Contains("Flushing all column families with data in WAL number", StringComparison.Ordinal));
        Assert.AreEqual(1, switches);
        Assert.AreEqual("0", nodes.GetProperty("rocksdb.num-files-at-level0"), "nothing was switched for the obsolete WAL");
    }

    private sealed class CapturingLogger(List<string> lines) : InfoLogger(InfoLogLevel.Info)
    {
        protected override void LogCore(InfoLogLevel level, string message)
        {
            lock (lines) lines.Add(message);
        }
    }
}
