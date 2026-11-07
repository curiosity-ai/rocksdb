using System.Buffers;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Utilities.Backup;

namespace RocksDb.Managed.AllocationProfile;

internal static partial class Scenarios
{
    private sealed class DropEveryTenth : CompactionFilter
    {
        public override string Name => "DropEveryTenth";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) =>
            key[^1] == (byte)'0' ? CompactionFilterDecision.Remove : CompactionFilterDecision.Keep;
    }

    private const int FlushEntries = 20_000;

    private static void AddBackground(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(100);

        list.Add(S("flush", "background", PathKind.Cold, $"{FlushEntries} Put (no WAL) + Flush (one ~2 MB L0 file)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Shuffled(ScenarioContext.Keys(FlushEntries));
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    foreach (byte[] k in keys) db.Put(wo, k, value);
                    db.Flush();
                }
            }));
        }, chunk: 1, notes: "Includes the Puts (put.nowal is ~0 B/op); divide by 20000 for B/entry."));
        foreach ((string name, CompactionStyle style, CompressionType comp, uint dict, bool filter) in
                 ((string, CompactionStyle, CompressionType, uint, bool)[])
                 [
                     ("compaction.leveled", CompactionStyle.Level, CompressionType.None, 0, false),
                     ("compaction.leveled-lz4", CompactionStyle.Level, CompressionType.LZ4, 0, false),
                     ("compaction.leveled-zstd-dict", CompactionStyle.Level, CompressionType.Zstd, 16 * 1024, false),
                     ("compaction.universal", CompactionStyle.Universal, CompressionType.None, 0, false),
                     ("compaction.filter", CompactionStyle.Level, CompressionType.None, 0, true),
                 ])
        {
            list.Add(S(name, "background", PathKind.Cold, $"4 x ({FlushEntries} Put + Flush) + CompactRange ({style}, {comp}{(dict > 0 ? " + dict" : "")}{(filter ? ", CompactionFilter" : "")})", ctx =>
            {
                ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
                o.WriteBufferSize = 64UL << 20;
                o.DisableAutoCompactions = true;
                o.CompactionStyle = style;
                o.Compression = comp;
                o.BottommostCompression = comp;
                if (dict > 0)
                {
                    o.CompressionOpts.MaxDictBytes = dict;
                    o.CompressionOpts.ZstdMaxTrainBytes = dict * 8;
                    o.BottommostCompressionOpts.MaxDictBytes = dict;
                    o.BottommostCompressionOpts.ZstdMaxTrainBytes = dict * 8;
                    o.BottommostCompressionOpts.Enabled = true;
                }
                if (filter) o.CompactionFilter = new DropEveryTenth();
                RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
                byte[][] keys = ScenarioContext.Shuffled(ScenarioContext.Keys(FlushEntries * 4));
                var wo = new WriteOptions { DisableWal = true };
                return Done(Runner.Sync(n =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        for (int f = 0; f < 4; f++)
                        {
                            for (int k = f * FlushEntries; k < (f + 1) * FlushEntries; k++) db.Put(wo, keys[k], value);
                            db.Flush();
                        }
                        db.CompactRange();
                    }
                }));
            }, chunk: 1, notes: "Includes 4 flushes (see `flush`)."));
        }
        list.Add(S("compaction.merge", "background", PathKind.Cold, "4 x (20000 UInt64Add Merge + Flush) over 5000 keys + CompactRange", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            o.MergeOperator = MergeOperator.UInt64Add;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(5_000);
            byte[] one = new byte[8];
            one[0] = 1;
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int f = 0; f < 4; f++)
                    {
                        for (int k = 0; k < FlushEntries; k++) db.Merge(wo, keys[k % keys.Length], one);
                        db.Flush();
                    }
                    db.CompactRange();
                }
            }));
        }, chunk: 1));
        list.Add(S("open-close.empty", "lifecycle", PathKind.Cold, "RocksDatabase.Open + Dispose of an empty DB", ctx =>
        {
            string dir = ctx.NewDirectory("db");
            RocksDatabase.Open(dir, ScenarioContext.DefaultDbOptions()).Dispose();
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) RocksDatabase.Open(dir, ScenarioContext.DefaultDbOptions()).Dispose(); }));
        }, chunk: 2, notes: "Options objects are created per open in the loop (a caller would too)."));
        list.Add(S("open-close.recovery", "lifecycle", PathKind.Cold, "Open (replays a 20000-record WAL, ~2.4 MB) + Dispose; WAL rewritten each op", ctx =>
        {
            string dir = ctx.NewDirectory("db");
            byte[][] keys = ScenarioContext.Keys(FlushEntries);
            var o = new ColumnFamilyOptions { WriteBufferSize = 64UL << 20, DisableAutoCompactions = true };
            var dbo = new DbOptions { CreateIfMissing = true, AvoidFlushDuringShutdown = true, AvoidFlushDuringRecovery = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
                    using (RocksDatabase w = RocksDatabase.Open(dir, dbo, o))
                        foreach (byte[] k in keys) w.Put(k, value);
                    RocksDatabase.Open(dir, dbo, o).Dispose();
                }
            }));
        }, chunk: 1, notes: "Includes writing the WAL (put.sync share ~0) and one empty open."));
        list.Add(S("ingest.sstfilewriter", "lifecycle", PathKind.Cold, "SstFileWriter 10000 entries + IngestExternalFiles(move)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            string dir = ctx.NewDirectory("sst");
            long round = 0;
            var keySets = new byte[50][][];
            for (int s = 0; s < keySets.Length; s++) keySets[s] = ScenarioContext.Keys(10_000, s * 10_000);
            string path = Path.Combine(dir, "bulk.sst");
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    byte[][] keys = keySets[round++ % keySets.Length];
                    using (SstFileWriter w = db.CreateSstFileWriter(db.DefaultColumnFamily))
                    {
                        w.Open(path);
                        foreach (byte[] k in keys) w.Put(k, value);
                        w.Finish();
                    }
                    db.IngestExternalFiles([path], new IngestExternalFileOptions { MoveFiles = true });
                }
            }));
        }, chunk: 1));
        list.Add(S("checkpoint", "lifecycle", PathKind.Cold, "CreateCheckpoint (8 SST files, hard links) + delete the directory", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            string root = ctx.NewDirectory("cp");
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    string dir = Path.Combine(root, "c");
                    db.CreateCheckpoint(dir);
                    Directory.Delete(dir, recursive: true);
                }
            }));
        }, chunk: 2));
        list.Add(S("backup", "lifecycle", PathKind.Cold, "CreateNewBackup (incremental, 1 new L0 file) + PurgeOldBackups(2)", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            BackupEngine engine = ctx.Own(BackupEngine.Open(new BackupEngineOptions(ctx.NewDirectory("backup"))));
            byte[][] keys = ScenarioContext.Keys(1000, 900_000);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    foreach (byte[] k in keys) db.Put(k, value);
                    engine.CreateNewBackup(db, flushBeforeBackup: true);
                    engine.PurgeOldBackups(2);
                }
            }));
        }, chunk: 1));
        list.Add(S("flush.async", "background", PathKind.Cold, $"{FlushEntries} PutAsync (no WAL) + await FlushAsync", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Shuffled(ScenarioContext.Keys(FlushEntries));
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Async(async n =>
            {
                for (int i = 0; i < n; i++)
                {
                    foreach (byte[] k in keys) await db.PutAsync(wo, k, value);
                    await db.FlushAsync();
                }
            }));
        }, chunk: 1));
    }
}
