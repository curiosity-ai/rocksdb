using System.Buffers;
using RocksDb.Managed;
using RocksDb.Managed.Utilities.Backup;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Flush/compaction control, checkpoints, backups and SST ingestion.</summary>
internal static class Maintenance
{
    // <snippet:compaction-filter-class>
    /// <summary>Drops every key starting with "tmp:" when compaction rewrites it.</summary>
    private sealed class DropTemporaryKeys : CompactionFilter
    {
        public override string Name => "DropTemporaryKeys";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) =>
            key.StartsWith("tmp:"u8) ? CompactionFilterDecision.Remove : CompactionFilterDecision.Keep;
    }
    // </snippet:compaction-filter-class>

    public static async Task Compaction(string path)
    {
        // <snippet:compaction>
        var cfOptions = new ColumnFamilyOptions
        {
            CompactionStyle = CompactionStyle.Level,          // also Universal and Fifo
            Level0FileNumCompactionTrigger = 4,
            TargetFileSizeBase = 64UL << 20,
            CompactionFilter = new DropTemporaryKeys(),
        };
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
        for (int i = 0; i < 1000; i++)
            db.Put(System.Text.Encoding.UTF8.GetBytes((i % 10 == 0 ? "tmp:" : "key:") + i), "value"u8);
        db.Flush();

        // Compact everything (null, null) or a key range; Force rewrites the bottommost level too.
        db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
        await db.CompactRangeAsync("key:0"u8.ToArray(), "key:5"u8.ToArray());
        await db.WaitForCompactAsync();                       // until no flush/compaction is pending

        // Compact chosen table files into a level (files a valid compaction needs are added); returns the outputs.
        db.Put("key:x"u8, "value"u8);
        db.Flush();
        List<string> l0 = db.GetLiveFilesMetadata().FindAll(f => f.Level == 0).ConvertAll(f => f.Name);
        IReadOnlyList<string> outputs = db.CompactFiles(new CompactFilesOptions { MaxSubcompactions = 2 }, l0, 6);

        Console.WriteLine(db.GetProperty(DbProperties.NumFilesAtLevelPrefix + "6"));   // files at L6
        string? stats = db.GetProperty(DbProperties.Stats);                             // native-format stats dump
        // </snippet:compaction>
        Line($"tmp:0 after compaction = {Utf8(db.Get("tmp:0"u8))}, files: {db.GetLiveFilesMetadata().Count}, CompactFiles outputs {outputs.Count}, stats {stats?.Length} chars");
        Check(db.Get("tmp:0"u8) is null && db.Get("key:1"u8) is not null, "compaction filter dropped tmp keys");
    }

    public static async Task CheckpointBackupRestore(string root)
    {
        string path = Path.Combine(root, "db");
        string checkpointDir = Path.Combine(root, "checkpoint");
        string backupDir = Path.Combine(root, "backups");
        string restoreDir = Path.Combine(root, "restored");

        // <snippet:checkpoint-backup>
        using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }))
        {
            db.Put("k"u8, "v"u8);

            // Checkpoint: an openable copy of the DB (SST files hard-linked when on the same file system).
            await db.CreateCheckpointAsync(checkpointDir);

            // Backups: incremental, shared SST files, native-identical layout (native BackupEngine can read them).
            using BackupEngine backups = BackupEngine.Open(new BackupEngineOptions(backupDir) { MaxBackgroundOperations = 4 });
            uint id = backups.CreateNewBackup(db, flushBeforeBackup: true);
            backups.VerifyBackup(id, verifyWithChecksum: true);
            foreach (BackupInfo info in backups.GetBackupInfo())
                Console.WriteLine($"backup {info.BackupId}: {info.NumberFiles} files, {info.Size} bytes");
            backups.PurgeOldBackups(numBackupsToKeep: 5);
        }

        using (BackupEngineReadOnly backups = BackupEngineReadOnly.Open(new BackupEngineOptions(backupDir)))
            backups.RestoreDbFromLatestBackup(restoreDir, restoreDir, new RestoreOptions());

        using RocksDatabase restored = RocksDatabase.Open(restoreDir, new DbOptions());
        using RocksDatabase fromCheckpoint = RocksDatabase.Open(checkpointDir, new DbOptions());
        // </snippet:checkpoint-backup>
        Line($"restored k={Utf8(restored.Get("k"u8))}, checkpoint k={Utf8(fromCheckpoint.Get("k"u8))}");
        Check(Utf8(restored.Get("k"u8)) == "v" && Utf8(fromCheckpoint.Get("k"u8)) == "v", "backup + checkpoint");
    }

    public static async Task SstFileIngest(string root)
    {
        string path = Path.Combine(root, "db");
        string sstPath = Path.Combine(root, "bulk.sst");

        // <snippet:sst-ingest>
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

        // Build an SST file offline (keys must be added in comparator order)...
        using (SstFileWriter writer = db.CreateSstFileWriter(db.DefaultColumnFamily))   // or new SstFileWriter(cfOptions)
        {
            writer.Open(sstPath);
            for (int i = 0; i < 100; i++)
                writer.Put(System.Text.Encoding.UTF8.GetBytes($"bulk:{i:D4}"), "v"u8);
            ExternalSstFileInfo info = await writer.FinishAsync();
            Console.WriteLine($"{info.NumEntries} entries");
        }

        // ...and ingest it atomically (the file is placed at the lowest level it fits; also works with native SSTs).
        // Several files may overlap: later files in the list win. IngestBehind = true back-fills below existing data.
        db.IngestExternalFiles([sstPath], new IngestExternalFileOptions { MoveFiles = true });

        // Replace a whole key range atomically: existing keys in ["bulk:", "bulk;") disappear in the same step as the new
        // file appears (EXPERIMENTAL atomic_replace_range; requires SnapshotConsistency = false).
        using (SstFileWriter writer = db.CreateSstFileWriter(db.DefaultColumnFamily))
        {
            writer.Open(sstPath);
            writer.Put("bulk:0001"u8, "v2"u8);
            await writer.FinishAsync();
        }
        db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg
        {
            ExternalFiles = [sstPath],
            Options = new IngestExternalFileOptions { SnapshotConsistency = false },
            AtomicReplaceRange = new IngestReplaceRange("bulk:"u8.ToArray(), "bulk;"u8.ToArray()),
        });
        // </snippet:sst-ingest>
        Line($"bulk:0042 = {Utf8(db.Get("bulk:0042"u8))}, bulk:0001 = {Utf8(db.Get("bulk:0001"u8))}");
        Check(db.Get("bulk:0042"u8) is null && Utf8(db.Get("bulk:0001"u8)) == "v2", "ingested and replaced");
    }
}
