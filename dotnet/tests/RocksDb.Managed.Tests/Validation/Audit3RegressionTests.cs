using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Regression tests for discrepancies found by audit #3 (see dotnet/docs/validation.md, "Audit #3").
/// </summary>
[TestClass]
public sealed class Audit3RegressionTests
{
    private const string DbPath = "/audit3";

    [TestMethod]
    public void FlushAndCompactionRecordNewestKeyTime()
    {
        // FlushJob::WriteLevel0Table passes current_time as newest_key_time; compaction outputs carry
        // Compaction::MaxInputFileNewestKeyTime. The managed port wrote 0 (unknown) for both, so FIFO TTL fell back to
        // the oldest ancestor time.
        MemoryFileSystem fs = NewFs();
        ulong before = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        using RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs },
            [new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { DisableAutoCompactions = true })]);
        db.Put(B("a"), B("1"));
        db.Flush();
        db.Put(B("a"), B("2")); // overlaps the first file (no trivial move)
        db.Flush();
        Dictionary<string, TableProperties> flushed = db.GetPropertiesOfAllTables();
        Assert.AreEqual(2, flushed.Count);
        ulong maxFlushed = 0;
        foreach (TableProperties p in flushed.Values)
        {
            Assert.IsTrue(p.NewestKeyTime >= before, "flush newest_key_time " + p.NewestKeyTime);
            maxFlushed = Math.Max(maxFlushed, p.NewestKeyTime);
        }
        db.CompactRange();
        Dictionary<string, TableProperties> compacted = db.GetPropertiesOfAllTables();
        Assert.AreEqual(1, compacted.Count);
        foreach (TableProperties p in compacted.Values) Assert.AreEqual(maxFlushed, p.NewestKeyTime);
    }

    [TestMethod]
    public void WalRecoveryVerifiesChecksumsWithoutParanoidChecks()
    {
        // DBImpl::InitializeLogReader: "We intentionally make log::Reader do checksumming even if paranoid_checks==false
        // so that corruptions cause entire commits to be skipped instead of propagating bad information". The port
        // passed checksum = paranoid_checks, so a corrupted record was replayed into the memtable.
        MemoryFileSystem fs = NewFs();
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs, ParanoidChecks = false };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("value-of-b"));
        }
        string? wal = null;
        foreach (string child in fs.GetChildren(DbPath))
        {
            if (child.EndsWith(".log", StringComparison.Ordinal)) wal = DbPath + "/" + child;
        }
        Assert.IsNotNull(wal);
        byte[] data = fs.ReadAllBytes(wal);
        int at = data.AsSpan().IndexOf("value-of-b"u8);
        Assert.IsTrue(at > 0);
        data[at] ^= 0x20; // "Value-of-b": same length, wrong CRC
        using (IWritableFile f = fs.CreateWritable(wal, truncate: true))
        {
            f.Append(data);
            f.Sync();
        }
        using RocksDatabase db2 = RocksDatabase.Open(DbPath, options);
        Assert.AreEqual("1", GetString(db2, "a"));
        // Point-in-time recovery stops at the corrupted record: the corrupted value must never be served.
        Assert.IsNull(GetString(db2, "b"));
    }

    [TestMethod]
    public void CompactionStyleOptionsArePersistedAndMutable()
    {
        // options/cf_options.cc: compaction_options_fifo / compaction_options_universal are mutable struct options
        // written to the OPTIONS file; the port neither persisted nor accepted them in SetOptions.
        MemoryFileSystem fs = NewFs();
        var cf = new ColumnFamilyOptions { CompactionStyle = CompactionStyle.Fifo };
        cf.CompactionOptionsFifo.MaxTableFilesSize = 12345;
        cf.CompactionOptionsUniversal.SizeRatio = 7;
        cf.CompactionOptionsUniversal.StopStyle = CompactionStopStyle.SimilarSize;
        using RocksDatabase db = RocksDatabase.Open(DbPath, new DbOptions { CreateIfMissing = true, FileSystem = fs },
            [new ColumnFamilyDescriptor("default", cf)]);
        OptionsFile f = OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: true);
        ColumnFamilyOptions persisted = f.FindColumnFamily("default")!;
        Assert.AreEqual(12345UL, persisted.CompactionOptionsFifo.MaxTableFilesSize);
        Assert.AreEqual(7u, persisted.CompactionOptionsUniversal.SizeRatio);
        Assert.AreEqual(CompactionStopStyle.SimilarSize, persisted.CompactionOptionsUniversal.StopStyle);
        Assert.AreEqual("{max_table_files_size=12345;allow_compaction=false;}", persisted.GetOption("compaction_options_fifo"));

        // Native struct text (with members the port does not model) and the single-scalar FIFO form.
        db.SetOptions([new("compaction_options_fifo", "{max_table_files_size=777;allow_compaction=true;age_for_warm=0;}")]);
        Assert.AreEqual(777UL, db.DefaultColumnFamily.Options.CompactionOptionsFifo.MaxTableFilesSize);
        Assert.IsTrue(db.DefaultColumnFamily.Options.CompactionOptionsFifo.AllowCompaction);
        db.SetOptions([new("compaction_options_fifo", "4096")]);
        Assert.AreEqual(4096UL, db.DefaultColumnFamily.Options.CompactionOptionsFifo.MaxTableFilesSize);
        f = OptionsFile.Load(fs, OptionsFile.FindLatestOptionsFile(fs, DbPath)!, preserveUnknownOptions: true);
        Assert.AreEqual(4096UL, f.FindColumnFamily("default")!.CompactionOptionsFifo.MaxTableFilesSize);
    }
}
