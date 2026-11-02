using RocksDb.Managed.Db;
using ValueType = RocksDb.Managed.Db.ValueType;
using static RocksDb.Managed.Tests.Db.VersionEditTests;

namespace RocksDb.Managed.Tests.Db;

/// <summary>In-memory MANIFEST record seam used by unit tests (no log framing).</summary>
internal sealed class InMemoryManifest : IManifestRecordWriter, IManifestRecordReader
{
    public List<byte[]> Records { get; } = new();
    private int _next;
    public int Syncs { get; private set; }

    public ValueTask AddRecordAsync(ReadOnlyMemory<byte> record, CancellationToken cancellationToken)
    {
        Records.Add(record.ToArray());
        return ValueTask.CompletedTask;
    }

    public ValueTask SyncAsync(CancellationToken cancellationToken)
    {
        Syncs++;
        return ValueTask.CompletedTask;
    }

    public bool TryReadRecord(out ReadOnlySpan<byte> record)
    {
        if (_next < Records.Count)
        {
            record = Records[_next++];
            return true;
        }
        record = default;
        return false;
    }

    public void Rewind() => _next = 0;
}

/// <summary>Tests for the VersionBuilder-lite replay and the ManifestWriter/ManifestReader pair.</summary>
[TestClass]
public sealed class VersionEditReplayTests
{
    private static FileMetaData F(ulong number, string smallest, string largest, ulong seq, ulong epoch) =>
        File(number, 0, 1000 + number, IKey(smallest, seq, ValueType.Value), IKey(largest, seq + 1, ValueType.Value), seq, seq + 1, epoch);

    private static VersionEdit Base()
    {
        var e = new VersionEdit { ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true, LogNumber = 0, NextFileNumber = 2, LastSequence = 0 };
        return e;
    }

    [TestMethod]
    public void ReplayBuildsLevelsAndColumnFamilies()
    {
        var r = new ManifestReplayer(null);
        r.Apply(Base());
        var addCf = new VersionEdit { ColumnFamily = 1, ComparatorName = "leveldb.BytewiseComparator" };
        addCf.AddColumnFamily("cf1");
        r.Apply(addCf);
        r.Apply(new VersionEdit { MaxColumnFamily = 1 });

        var flush1 = new VersionEdit { LogNumber = 5, NextFileNumber = 8 };
        flush1.AddFile(0, F(6, "a", "m", 1, 1));
        r.Apply(flush1);
        var flush2 = new VersionEdit { LogNumber = 7, NextFileNumber = 10 };
        flush2.AddFile(0, F(9, "b", "z", 10, 2));
        r.Apply(flush2);

        // Compaction: L0 -> L1 two files.
        var compact = new VersionEdit { NextFileNumber = 13 };
        compact.DeleteFile(0, 6);
        compact.DeleteFile(0, 9);
        compact.AddFile(1, F(12, "n", "z", 1, 1));
        compact.AddFile(1, F(11, "a", "m", 1, 1));
        r.Apply(compact);

        var cfFlush = new VersionEdit { ColumnFamily = 1, LogNumber = 7 };
        cfFlush.AddFile(0, F(14, "c", "d", 20, 1));
        cfFlush.AddFile(0, F(15, "c", "d", 30, 2));
        r.Apply(cfFlush);

        RecoveredVersionState s = r.Finish();
        Assert.AreEqual(13UL, s.NextFileNumber);
        Assert.AreEqual(31UL, s.LastSequence);
        Assert.AreEqual(1u, s.MaxColumnFamily);
        Assert.AreEqual(2, s.ColumnFamilies.Count);
        ColumnFamilyVersion def = s.ColumnFamilies[0];
        Assert.AreEqual(0, def.Levels[0].Count);
        Assert.AreEqual(2, def.Levels[1].Count);
        Assert.AreEqual(11UL, def.Levels[1][0].FileNumber); // ordered by smallest key
        Assert.AreEqual(12UL, def.Levels[1][1].FileNumber);
        Assert.AreEqual(7UL, def.LogNumber);
        ColumnFamilyVersion cf1 = s.FindColumnFamily("cf1")!;
        Assert.AreEqual(15UL, cf1.Levels[0][0].FileNumber); // newest epoch first
        Assert.AreEqual(14UL, cf1.Levels[0][1].FileNumber);
        Assert.AreEqual(7UL, s.MinLogNumberWithUnflushedData);
    }

    [TestMethod]
    public void DropColumnFamily()
    {
        var r = new ManifestReplayer(null);
        r.Apply(Base());
        var add = new VersionEdit { ColumnFamily = 3 };
        add.AddColumnFamily("x");
        r.Apply(add);
        var drop = new VersionEdit { ColumnFamily = 3 };
        drop.DropColumnFamily();
        r.Apply(drop);
        Assert.AreEqual(1, r.Finish().ColumnFamilies.Count);

        var r2 = new ManifestReplayer(null);
        r2.Apply(Base());
        var bad = new VersionEdit { ColumnFamily = 9 };
        bad.DropColumnFamily();
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => r2.Apply(bad)).Code);
    }

    [TestMethod]
    public void InvalidFileOperationsAreCorruption()
    {
        var r = new ManifestReplayer(null);
        r.Apply(Base());
        var del = new VersionEdit();
        del.DeleteFile(1, 99);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => r.Apply(del)).Code);

        var add = new VersionEdit();
        add.AddFile(1, F(5, "a", "b", 1, 1));
        r.Apply(add);
        var add2 = new VersionEdit();
        add2.AddFile(2, F(5, "a", "b", 1, 1));
        StringAssert.Contains(Assert.ThrowsExactly<RocksDbException>(() => r.Apply(add2)).Message, "already in the LSM tree on level 1");

        var unknownCf = new VersionEdit { ColumnFamily = 7, LogNumber = 3 };
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => r.Apply(unknownCf)).Code);
    }

    [TestMethod]
    public void MissingRequiredFieldsIsCorruption()
    {
        var r = new ManifestReplayer(null);
        r.Apply(new VersionEdit { ComparatorName = "leveldb.BytewiseComparator" });
        var ex = Assert.ThrowsExactly<RocksDbException>(() => r.Finish());
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "no log_file_number, next_file_number, last_sequence entry in MANIFEST");
    }

    [TestMethod]
    public void AtomicGroupAppliedOnlyWhenComplete()
    {
        var r = new ManifestReplayer(null);
        r.Apply(Base());
        var g1 = new VersionEdit();
        g1.AddFile(0, F(20, "a", "b", 1, 1));
        g1.MarkAtomicGroup(1);
        var g2 = new VersionEdit();
        g2.AddFile(0, F(21, "c", "d", 2, 2));
        g2.MarkAtomicGroup(0);
        r.Apply(g1);
        int before = r.EditsApplied;
        r.Apply(g2);
        Assert.AreEqual(before + 2, r.EditsApplied);
        Assert.AreEqual(2, r.Finish().ColumnFamilies[0].Levels[0].Count);

        // Incomplete group followed by a normal edit is corruption.
        var r2 = new ManifestReplayer(null);
        r2.Apply(Base());
        var g = new VersionEdit();
        g.MarkAtomicGroup(2);
        r2.Apply(g);
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => r2.Apply(new VersionEdit { LogNumber = 1 })).Code);
    }

    [TestMethod]
    public void WalTracking()
    {
        var r = new ManifestReplayer(null);
        r.Apply(Base());
        var adds = new VersionEdit();
        adds.WalAdditions.Add(new WalAddition(1, null));
        adds.WalAdditions.Add(new WalAddition(2, 100));
        r.Apply(adds);
        var sync = new VersionEdit();
        sync.WalAdditions.Add(new WalAddition(2, 200));
        r.Apply(sync);
        r.Apply(new VersionEdit { WalDeletionBefore = 2 });
        RecoveredVersionState s = r.Finish();
        Assert.AreEqual(1, s.Wals.Count);
        Assert.AreEqual(200UL, s.Wals[2]);
        Assert.AreEqual(2UL, s.MinWalNumberToKeep);
    }

    [TestMethod]
    public void BlobFiles()
    {
        var r = new ManifestReplayer(null);
        r.Apply(Base());
        var e = new VersionEdit();
        e.BlobFileAdditions.Add(new BlobFileAddition(30, 10, 1000, "", []));
        e.BlobFileAdditions.Add(new BlobFileAddition(31, 10, 1000, "", []));
        r.Apply(e);
        var g = new VersionEdit();
        g.BlobFileGarbages.Add(new BlobFileGarbage(30, 10, 1000)); // fully garbage
        g.BlobFileGarbages.Add(new BlobFileGarbage(31, 3, 300));
        r.Apply(g);
        RecoveredVersionState s = r.Finish();
        Assert.AreEqual(1, s.ColumnFamilies[0].BlobFiles.Count);
        Assert.AreEqual(3UL, s.ColumnFamilies[0].BlobFiles[31].GarbageBlobCount);

        var r2 = new ManifestReplayer(null);
        r2.Apply(Base());
        var over = new VersionEdit();
        over.BlobFileGarbages.Add(new BlobFileGarbage(99, 1, 1));
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => r2.Apply(over)).Code);
    }

    [TestMethod]
    public async Task WriterSnapshotRoundTrip()
    {
        // Build a state by replay, snapshot it into a new manifest, recover again, compare.
        var r = new ManifestReplayer(null);
        r.Apply(new VersionEdit { DbId = "db-id-1" });
        r.Apply(Base());
        var addCf = new VersionEdit { ColumnFamily = 1, ComparatorName = "leveldb.BytewiseComparator", PersistUserDefinedTimestamps = true };
        addCf.AddColumnFamily("users");
        r.Apply(addCf);
        var e = new VersionEdit { LogNumber = 9, NextFileNumber = 20, LastSequence = 500, MaxColumnFamily = 1, MinLogNumberToKeep = 9 };
        e.AddFile(0, F(10, "a", "k", 100, 3));
        e.AddFile(2, F(11, "l", "z", 50, 1));
        r.Apply(e);
        var e2 = new VersionEdit { ColumnFamily = 1, LogNumber = 9 };
        e2.AddFile(1, F(12, "u1", "u9", 400, 2));
        r.Apply(e2);
        RecoveredVersionState original = r.Finish();

        var manifest = new InMemoryManifest();
        var writer = new ManifestWriter(manifest);
        await writer.WriteSnapshotAsync(original, static _ => 0, CancellationToken.None);
        Assert.AreEqual(1, manifest.Syncs);

        var extra = new VersionEdit { ColumnFamily = 1, LastSequence = 600, NextFileNumber = 22 };
        extra.DeleteFile(1, 12);
        extra.AddFile(1, F(21, "u1", "u9", 400, 2));
        await writer.AddEditAsync(extra, 0, sync: true, CancellationToken.None);

        RecoveredVersionState recovered = ManifestReader.Recover(manifest, null);
        Assert.AreEqual("db-id-1", recovered.DbId);
        Assert.AreEqual(22UL, recovered.NextFileNumber);
        Assert.AreEqual(600UL, recovered.LastSequence);
        Assert.AreEqual(9UL, recovered.MinLogNumberToKeep);
        Assert.AreEqual(1u, recovered.MaxColumnFamily);
        Assert.AreEqual(2, recovered.ColumnFamilies.Count);
        Assert.AreEqual(10UL, recovered.ColumnFamilies[0].Levels[0][0].FileNumber);
        Assert.AreEqual(11UL, recovered.ColumnFamilies[0].Levels[2][0].FileNumber);
        Assert.AreEqual("users", recovered.ColumnFamilies[1].Name);
        Assert.AreEqual(21UL, recovered.ColumnFamilies[1].Levels[1][0].FileNumber);
        Assert.AreEqual(9UL, recovered.ColumnFamilies[1].LogNumber);
    }
}
