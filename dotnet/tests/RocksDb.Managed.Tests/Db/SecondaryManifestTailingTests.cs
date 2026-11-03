using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// Secondary instances tailing the primary's MANIFEST incrementally (ReactiveVersionSet / ManifestTailer,
/// db/version_set.cc, db/version_edit_handler.h; scenarios after db/db_secondary_test.cc).
/// </summary>
[TestClass]
public sealed class SecondaryManifestTailingTests
{
    private static ColumnFamilyOptions SmallCf(bool autoCompactions) => new()
    {
        WriteBufferSize = 64 * 1024,
        ArenaBlockSize = 16 * 1024,
        Level0FileNumCompactionTrigger = 3,
        DisableAutoCompactions = !autoCompactions,
    };

    private static void AssertSameContents(RocksDatabase primary, RocksDatabase secondary, int keys, string context)
    {
        for (int i = 0; i < keys; i++) Assert.AreEqual(GetString(primary, S(Key(i))!), GetString(secondary, S(Key(i))!), context + " key " + i);
        Assert.AreEqual(primary.GetLatestSequenceNumber(), secondary.GetLatestSequenceNumber(), context);
    }

    [TestMethod]
    public async Task CatchUpAppliesOnlyNewEditsAfterManyFlushesAndCompactions()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase primary = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), SmallCf(autoCompactions: true));
        for (int i = 0; i < 100; i++) primary.Put(Key(i), Value(i, 0));
        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(DbPath, "/sec-many", new DbOptions { FileSystem = fs });
        (int opens, long records) = sec.SecondaryManifestTailStats;
        Assert.AreEqual(1, opens);
        Assert.IsGreaterThan(0L, records);

        // No new edits: nothing re-read.
        await sec.TryCatchUpWithPrimaryAsync();
        Assert.AreEqual((opens, records), sec.SecondaryManifestTailStats);

        for (int round = 1; round <= 12; round++)
        {
            for (int i = 0; i < 400; i++) primary.Put(Key(i), Value(i, round));
            primary.Delete(Key(round));
            await primary.FlushAsync();
            if (round % 4 == 0) primary.WaitForCompact();
            await sec.TryCatchUpWithPrimaryAsync();
            AssertSameContents(primary, sec, 400, "round " + round);
            (int o, long r) = sec.SecondaryManifestTailStats;
            Assert.AreEqual(1, o, "the MANIFEST must not be re-opened while CURRENT is unchanged");
            Assert.IsGreaterThan(records, r, "round " + round + " applied no new edits");
            records = r;
        }
        // An idle catch-up applies nothing.
        await sec.TryCatchUpWithPrimaryAsync();
        Assert.AreEqual((1, records), sec.SecondaryManifestTailStats);
    }

    [TestMethod]
    public async Task ManifestRolloverSwitchesToTheNewManifest()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.MaxManifestFileSize = 512; // every few edits the primary starts a new MANIFEST
        await using RocksDatabase primary = await RocksDatabase.OpenAsync(DbPath, options, SmallCf(autoCompactions: true));
        for (int i = 0; i < 50; i++) primary.Put(Key(i), Value(i, 0));
        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(DbPath, "/sec-roll", new DbOptions { FileSystem = fs });
        int opens = sec.SecondaryManifestTailStats.ManifestOpens;
        for (int round = 1; round <= 10; round++)
        {
            for (int i = 0; i < 300; i++) primary.Put(Key(i), Value(i, round));
            await primary.FlushAsync();
            // Catch up only every other round, so that sometimes several rollovers happened in between.
            if (round % 2 == 1) continue;
            await sec.TryCatchUpWithPrimaryAsync();
            AssertSameContents(primary, sec, 300, "round " + round);
        }
        Assert.IsGreaterThan(opens, sec.SecondaryManifestTailStats.ManifestOpens, "the primary never rolled its MANIFEST over");
    }

    [TestMethod]
    public async Task CatchUpWhilePrimaryWritesFlushesAndCompacts()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions options = NewOptions(fs);
        options.MaxManifestFileSize = 2048;
        await using RocksDatabase primary = await RocksDatabase.OpenAsync(DbPath, options, SmallCf(autoCompactions: true));
        // Keep the files of old versions so that the secondary can read between its catch-ups (upstream caveat).
        primary.DisableFileDeletions();
        const int Keys = 300;
        for (int i = 0; i < Keys; i++) primary.Put(Key(i), Value(i, 0));
        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(DbPath, "/sec-conc", new DbOptions { FileSystem = fs });

        using var stop = new CancellationTokenSource();
        Task writer = Task.Run(async () =>
        {
            for (int round = 1; round <= 30; round++)
            {
                for (int i = 0; i < Keys; i++) await primary.PutAsync(Key(i), Value(i, round));
                if (round % 3 == 0) await primary.FlushAsync();
                if (round % 10 == 0) await primary.CompactRangeAsync(null, null);
            }
        });
        int catchUps = 0;
        while (!writer.IsCompleted)
        {
            await sec.TryCatchUpWithPrimaryAsync();
            catchUps++;
            // Every key was written before the secondary opened: it must always be visible with some round's value.
            int k = catchUps * 7 % Keys;
            string? v = GetString(sec, S(Key(k))!);
            Assert.IsNotNull(v, "key " + k + " vanished after catch-up " + catchUps);
            StringAssert.StartsWith(v, "value-" + k + "-");
        }
        await writer;
        await sec.TryCatchUpWithPrimaryAsync();
        AssertSameContents(primary, sec, Keys, "final");
        Assert.IsGreaterThan(1, catchUps);
    }

    [TestMethod]
    public void IncompleteAtomicGroupStaysBufferedAcrossCurrentState()
    {
        // ManifestReplayer.CurrentState (used by the tailer) must keep a partially read atomic group, unlike Finish.
        var replayer = new ManifestReplayer(null);
        replayer.Apply(new VersionEdit { LogNumber = 1, NextFileNumber = 10, LastSequence = 5 });
        var add = new VersionEdit { ColumnFamily = 1 };
        add.AddColumnFamily("cf1");
        add.MarkAtomicGroup(1);
        replayer.Apply(add);
        RecoveredVersionState state = replayer.CurrentState();
        Assert.IsFalse(state.ColumnFamilies.ContainsKey(1), "an incomplete atomic group must not be visible");
        var second = new VersionEdit { ColumnFamily = 1, LogNumber = 2 };
        second.MarkAtomicGroup(0);
        replayer.Apply(second);
        state = replayer.CurrentState();
        Assert.IsTrue(state.ColumnFamilies.ContainsKey(1));
        Assert.AreEqual(2UL, state.ColumnFamilies[1].LogNumber);
    }

    [TestMethod]
    public async Task SecondaryOnLocalFileSystemTailsManifest()
    {
        string root = Path.Combine(Path.GetTempPath(), "rocksdb-managed-sectail-" + Guid.NewGuid().ToString("N"));
        try
        {
            var options = new DbOptions { CreateIfMissing = true, MaxManifestFileSize = 1024 };
            await using RocksDatabase primary = await RocksDatabase.OpenAsync(root + "/db", options, SmallCf(autoCompactions: true));
            for (int i = 0; i < 100; i++) primary.Put(Key(i), Value(i, 0));
            await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(root + "/db", root + "/sec", new DbOptions());
            for (int round = 1; round <= 6; round++)
            {
                for (int i = 0; i < 300; i++) primary.Put(Key(i), Value(i, round));
                await primary.FlushAsync();
                await sec.TryCatchUpWithPrimaryAsync();
                AssertSameContents(primary, sec, 300, "round " + round);
            }
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { }
        }
    }
    /// <summary>
    /// ManifestTailer::VerifyFile: a version whose new table file is missing (or has the wrong size) is not installed;
    /// the column family keeps serving its current version (and its WALs), and the next catch-up after the file appears
    /// installs it.
    /// </summary>
    [TestMethod]
    public async Task VersionWithMissingTableFileIsNotInstalled()
    {
        MemoryFileSystem fs = NewFs();
        await using RocksDatabase primary = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), SmallCf(autoCompactions: false));
        for (int i = 0; i < 50; i++) primary.Put(Key(i), Value(i, 0));
        await primary.FlushAsync();
        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(DbPath, "/sec-missing", new DbOptions { FileSystem = fs });
        Assert.AreEqual(1, sec.GetLiveFilesMetadata().Count);

        for (int i = 50; i < 100; i++) primary.Put(Key(i), Value(i, 1));
        await primary.FlushAsync();
        LiveFileMetadata newest = primary.GetLiveFilesMetadata().MaxBy(f => f.FileNumber)!;
        string path = newest.Directory + newest.Name;
        byte[] bytes = fs.ReadAllBytes(path);
        fs.DeleteFile(path);
        await sec.TryCatchUpWithPrimaryAsync();
        Assert.AreEqual(1, sec.GetLiveFilesMetadata().Count, "the version adding the missing file is not installed");
        Assert.AreEqual(S(Value(5, 0)), GetString(sec, S(Key(5))!));

        // A file with the wrong size is not installed either.
        using (IWritableFile w = fs.CreateWritable(path))
        {
            w.Append(bytes.AsSpan(0, bytes.Length - 1));
            w.Sync();
        }
        await sec.TryCatchUpWithPrimaryAsync();
        Assert.AreEqual(1, sec.GetLiveFilesMetadata().Count);

        // Restored: installed by the next catch-up.
        fs.DeleteFile(path);
        using (IWritableFile w = fs.CreateWritable(path))
        {
            w.Append(bytes);
            w.Sync();
        }
        for (int i = 0; i < 10; i++) primary.Put(Key(i), Value(i, 2));
        await sec.TryCatchUpWithPrimaryAsync();
        Assert.AreEqual(2, sec.GetLiveFilesMetadata().Count);
        AssertSameContents(primary, sec, 100, "after restore");
    }
}
