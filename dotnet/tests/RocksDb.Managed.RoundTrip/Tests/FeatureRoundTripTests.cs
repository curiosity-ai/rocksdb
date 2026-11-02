using RocksDb.Managed.Compression;
using System.Globalization;
using System.Text;
using RocksDb.Managed.RoundTrip.Engines;
using RocksDb.Managed.RoundTrip.Harness;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Tests;

/// <summary>
/// Deterministic and targeted round trips: edge-case keys/values, many keys across levels, external SST files,
/// checkpoints, TTL databases and OPTIONS files, each handed from one engine to the other and back.
/// </summary>
[TestClass]
public sealed class FeatureRoundTripTests : RoundTripTestBase
{
    private static EngineKind Other(EngineKind e) => e == EngineKind.Native ? EngineKind.Managed : EngineKind.Native;

    private static byte[] A(string s) => Encoding.ASCII.GetBytes(s);

    private static byte[] Filled(int n, byte seed)
    {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte)(seed + i * 13 + (i >> 7));
        return b;
    }

    // ---------------------------------------------------------------------------------------------------------
    // edge cases

    private static IEnumerable<(byte[] Key, byte[] Value)> EdgeEntries(string tag)
    {
        byte t = (byte)tag[0];
        yield return ([], A("empty-key-" + tag));
        yield return ([0x00], []);
        yield return ([0x00, 0x00], [0x00]);
        yield return ([0x00, 0xff, 0x00], Filled(4095, t));
        yield return ([0x7f], Filled(4096, t));
        yield return ([0x80], Filled(4097, t));
        yield return ([0xff], Filled(70 * 1024, t));                 // > 64 KiB: spans WAL blocks
        yield return ([0xff, 0xff], Filled(1536 * 1024, t));         // 1.5 MiB
        yield return ([0xff, 0xff, 0xff, 0xff], new byte[33 * 1024]); // all zero, > WAL block size
        yield return (Filled(2000, t), A("long-key-" + tag));       // long key
        yield return ([.. A("bin/"), 0x00, 0x01, 0xfe, 0xff], A(tag));
        for (int i = 0; i < 64; i++) yield return ([.. A("seq/"), (byte)i, 0xff, (byte)(255 - i)], Filled(i * 37 % 300, (byte)(t + i)));
    }

    [TestMethod]
    [DataRow(CompressionType.None, 5, FilterKind.Bloom)]
    [DataRow(CompressionType.Snappy, 6, FilterKind.Ribbon)]
    [DataRow(CompressionType.LZ4, 7, FilterKind.None)]
    [DataRow(CompressionType.Zstd, 7, FilterKind.Bloom)]
    public async Task EdgeCaseKeysAndValues_BothWays(CompressionType compression, int formatVersion, FilterKind filter)
    {
        DbSpec spec = DbSpec.Standard(compression, formatVersion, filter, 256UL << 10, false);
        RoundTripHarness h = CreateHarness(spec, $"{compression}-fv{formatVersion}-{filter}");
        EngineKind engine = EngineKind.Native;
        for (int round = 0; round < 4; round++)
        {
            string tag = engine + round.ToString(CultureInfo.InvariantCulture);
            ScriptBuilder b = h.NewScript("edge-" + tag);
            foreach (string cf in new[] { "default", "blobs" })
            {
                int i = 0;
                foreach ((byte[] k, byte[] v) in EdgeEntries(tag))
                {
                    // Every round overwrites, deletes and re-creates a different subset.
                    if ((i + round) % 5 == 4) b.Add(Op.Delete(cf, k));
                    else b.Add(Op.Put(cf, k, v));
                    i++;
                }
                if (round % 2 == 0) b.Add(Op.Flush(cf)); // odd rounds leave everything in the WAL
            }
            b.Add(Op.Merge("default", [0x00, 0x00], A("m" + tag)))           // merge onto a 1-byte 0x00 value
             .Add(Op.Merge("default", A("merge-empty"), []))                  // empty operand
             .Add(Op.Merge("counters", [0xff], ReferenceModel.U64(ulong.MaxValue - 1)))
             .Add(Op.Merge("counters", [0xff], ReferenceModel.U64(5)));       // wraps around
            b.Verify().Dump();
            await h.RunAsync(engine, b);
            engine = Other(engine);
        }
        await h.RunAsync(engine, h.NewScript("final"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // many keys across levels

    private static byte[] ManyKey(int i) => A("key" + i.ToString("D8", CultureInfo.InvariantCulture));

    private static byte[] ManyValue(int i, int gen) => Filled((int)(((long)i * 7919 + gen * 31) % 160), (byte)(i + gen));

    /// <summary>
    /// The loader writes n keys in random order with small write buffers and auto compaction, so the data spreads over
    /// L0..Ln (asserted); the other engine verifies, updates a slice and compacts; the loader reads it back.
    /// </summary>
    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task ManyKeysAcrossLevels(EngineKind loader)
    {
        EngineKind other = Other(loader);
        int n = LongRuns ? 1_000_000 : 200_000;
        DbSpec spec = new();
        spec.ColumnFamilies.Add(new CfSpec { Name = "default", Compression = CompressionType.LZ4, FormatVersion = 6, Filter = FilterKind.Bloom, WriteBufferSize = 512UL << 10, Merge = MergeKind.StringAppend });
        spec.ColumnFamilies.Add(new CfSpec { Name = "counters", Compression = CompressionType.Zstd, FormatVersion = 7, Filter = FilterKind.Ribbon, WriteBufferSize = 256UL << 10, Merge = MergeKind.UInt64Add });
        RoundTripHarness h = CreateHarness(spec, loader + "-" + n.ToString(CultureInfo.InvariantCulture));
        h.Model.MaxAbsentPerCf = 2000;

        ScriptBuilder b = h.NewScript(loader + "-load");
        int[] order = new int[n];
        for (int i = 0; i < n; i++) order[i] = i;
        h.Random.Shuffle(order);
        for (int i = 0; i < n; i += 100)
        {
            var ops = new List<Op>(100);
            for (int j = i; j < Math.Min(n, i + 100); j++)
            {
                int k = order[j];
                ops.Add(Op.Put("default", ManyKey(k), ManyValue(k, 0)));
                if (k % 10 == 0) ops.Add(Op.Merge("counters", ManyKey(k / 10), ReferenceModel.U64((ulong)k)));
            }
            b.Batch(false, ops);
        }
        b.Add(Op.Flush("default")).Add(Op.Props("default")).Verify();
        await h.RunAsync(loader, b);
        int deeper = 0;
        foreach (string line in h.LastOutput)
        {
            // "PROP default files-at-levelN COUNT"
            string[] f = line.Split(' ');
            if (f.Length == 4 && f[0] == "PROP" && f[2] != "files-at-level0") deeper += int.Parse(f[3], CultureInfo.InvariantCulture);
            if (f.Length == 4 && f[0] == "PROP") h.Log(line);
        }
        Assert.IsTrue(deeper > 0, $"{loader} should have compacted data below L0: " + string.Join(" | ", h.LastOutput));

        // The other engine: verify everything, overwrite / delete / range-delete a slice, flush part of it, compact.
        ScriptBuilder m = h.NewScript(other + "-update");
        for (int k = 0; k < n; k += 10)
        {
            if (k % 30 == 0) m.Add(Op.Delete("default", ManyKey(k)));
            else m.Add(Op.Put("default", ManyKey(k), ManyValue(k, 1)));
        }
        m.Add(Op.DeleteRange("default", ManyKey(n / 2), ManyKey(n / 2 + 777)))
         .Add(Op.Merge("counters", ManyKey(3), ReferenceModel.U64(1)))
         .Add(Op.Flush("default"))
         .Add(Op.Compact("counters"));
        for (int k = 1; k < n; k += 1000) m.Add(Op.Put("default", ManyKey(k), ManyValue(k, 2))); // stays in the WAL
        m.Verify();
        await h.RunAsync(other, m);

        // The loader reads the result back and compacts it fully; the other engine verifies once more.
        await h.RunAsync(loader, h.NewScript(loader + "-verify").Add(Op.Compact("default")).Verify());
        await h.RunAsync(other, h.NewScript(other + "-final"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // external SST files

    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task SstFiles_WrittenByOneIngestedByOther(EngineKind sstWriter)
    {
        EngineKind ingester = Other(sstWriter);
        DbSpec spec = DbSpec.Standard(CompressionType.Zstd, 6, FilterKind.Bloom, 1UL << 20, false);
        RoundTripHarness h = CreateHarness(spec, $"{sstWriter}-writes");

        // The ingesting engine creates the DB with overlapping data, so ingestion needs global sequence numbers.
        ScriptBuilder setup = h.NewScript($"{ingester}-base");
        OpGenerator.Generate(setup, new GeneratorOptions { Ops = 800, KeySpace = 1000, AllowColumnFamilyChanges = false, AllowSnapshots = false });
        setup.Add(Op.Flush("default")).Verify();
        await h.RunAsync(ingester, setup);

        // The writer builds the files (it opens a scratch DB only because every child run opens one).
        string sstDir = h.SubDir("sst");
        Directory.CreateDirectory(sstDir);
        ScriptBuilder w = h.NewScriptWithoutVerify($"{sstWriter}-sst", h.SubDir("scratch-db"), spec, h.Model);
        string f1 = Path.Combine(sstDir, "default.sst"), f2 = Path.Combine(sstDir, "counters.sst"), f3 = Path.Combine(sstDir, "blobs.sst");
        w.Add(Op.SstBegin("default", f1));
        for (int i = 100; i < 700; i += 3)
        {
            byte[] k = A("k" + i.ToString("D6", CultureInfo.InvariantCulture));
            switch (i % 4)
            {
                case 0: w.Add(Op.SstMerge(k, A("sst" + i))); break;
                case 1: w.Add(Op.SstDelete(k)); break;
                default: w.Add(Op.SstPut(k, i % 5 == 0 ? [] : A("sst-value-" + i))); break;
            }
        }
        w.Add(Op.SstDeleteRange(A("k000200"), A("k000260"))).Add(Op.SstEnd());
        w.Add(Op.SstBegin("counters", f2));
        for (int i = 0; i < 300; i++)
            w.Add(i % 2 == 0 ? Op.SstMerge(A("k" + i.ToString("D6", CultureInfo.InvariantCulture)), ReferenceModel.U64((ulong)i))
                             : Op.SstPut(A("k" + i.ToString("D6", CultureInfo.InvariantCulture)), ReferenceModel.U64(1000 + (ulong)i)));
        w.Add(Op.SstEnd());
        w.Add(Op.SstBegin("blobs", f3));
        for (int i = 0; i < 20; i++) w.Add(Op.SstPut(A("big" + i.ToString("D3", CultureInfo.InvariantCulture)), Filled(10_000 + i * 9000, (byte)i)));
        w.Add(Op.SstEnd());
        await h.RunAsync(sstWriter, w);

        ScriptBuilder ingest = h.NewScript($"{ingester}-ingest")
            .Add(Op.Ingest("default", [f1], false))
            .Add(Op.Ingest("counters", [f2], true))
            .Add(Op.Ingest("blobs", [f3], false))
            .Verify().Dump();
        await h.RunAsync(ingester, ingest);
        // Round trip: the SST writer's engine reads the DB with the ingested files.
        await h.RunAsync(sstWriter, h.NewScript($"{sstWriter}-verify"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // checkpoints

    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task Checkpoint_CreatedByOneOpenedByOther(EngineKind creator)
    {
        EngineKind opener = Other(creator);
        DbSpec spec = DbSpec.Standard(CompressionType.LZ4, 6, FilterKind.Ribbon, 256UL << 10, false);
        RoundTripHarness h = CreateHarness(spec, $"{creator}-creates");
        string ckpt = h.SubDir("checkpoint");

        ScriptBuilder b = h.NewScript($"{creator}-write");
        OpGenerator.Generate(b, new GeneratorOptions { Ops = 1500, AllowColumnFamilyChanges = false });
        b.Add(Op.Checkpoint(ckpt));
        OpGenerator.Generate(b, new GeneratorOptions { Ops = 300, AllowColumnFamilyChanges = false, AllowFlush = false });
        b.Verify();
        await h.RunAsync(creator, b);

        // The other engine opens the checkpoint (contents as of the checkpoint), reads and extends it.
        ReferenceModel ckptModel = ReferenceModel.FromContents(spec, h.Model.Checkpoints[ckpt]);
        ScriptBuilder o = h.NewScript($"{opener}-checkpoint", ckpt, spec, ckptModel);
        OpGenerator.Generate(o, new GeneratorOptions { Ops = 600, AllowColumnFamilyChanges = false });
        o.Verify().Dump();
        await h.RunAsync(opener, o);

        // The creator reads the extended checkpoint back, and its own DB is unaffected.
        await h.RunAsync(creator, h.NewScript($"{creator}-checkpoint-back", ckpt, spec, ckptModel));
        await h.RunAsync(creator, h.NewScript($"{creator}-source"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // backups

    /// <summary>
    /// BackupEngine interoperability: <paramref name="first"/> backs up the DB, the other engine restores it, extends the
    /// restored DB and adds an incremental backup to the same backup directory, and the first engine restores that.
    /// </summary>
    [TestMethod]
    [DataRow(EngineKind.Managed)]
    [DataRow(EngineKind.Native)]
    public async Task Backup_RestoredAndExtendedByOther(EngineKind first)
    {
        EngineKind second = Other(first);
        DbSpec spec = DbSpec.Standard(CompressionType.LZ4, 6, FilterKind.Bloom, 256UL << 10, false);
        RoundTripHarness h = CreateHarness(spec, $"{first}-first");
        string backupDir = h.SubDir("backup");
        string restore1 = h.SubDir("restore1"), restore2 = h.SubDir("restore2");

        // 1. first engine: write (flushes leave SSTs, the tail stays in the WAL), back up without flushing, keep writing.
        ScriptBuilder b = h.NewScript($"{first}-write-backup");
        OpGenerator.Generate(b, new GeneratorOptions { Ops = 1500, AllowColumnFamilyChanges = false });
        b.Add(Op.Backup(backupDir, flush: false));
        OpGenerator.Generate(b, new GeneratorOptions { Ops = 200, AllowColumnFamilyChanges = false, AllowFlush = false });
        b.Verify();
        await h.RunAsync(first, b);
        SkipIfNativeMetaIsEmpty(backupDir);
        ReferenceModel backup1 = ReferenceModel.FromContents(spec, h.Model.Backups[backupDir]);

        // 2. second engine restores it (from a script on the source DB), opens the restored DB, extends it and adds an
        //    incremental backup (flushed) to the same backup directory.
        ScriptBuilder r = h.NewScriptWithoutVerify($"{second}-restore", h.DbPath, spec, h.Model);
        r.Add(Op.Restore(backupDir, restore1));
        await h.RunAsync(second, r);
        ScriptBuilder e = h.NewScript($"{second}-extend-backup", restore1, spec, backup1);
        OpGenerator.Generate(e, new GeneratorOptions { Ops = 800, AllowColumnFamilyChanges = false });
        e.Add(Op.Backup(backupDir, flush: true));
        e.Verify();
        await h.RunAsync(second, e);
        SkipIfNativeMetaIsEmpty(backupDir);
        ReferenceModel backup2 = ReferenceModel.FromContents(spec, backup1.Backups[backupDir]);

        // 3. first engine restores the incremental backup and verifies it.
        ScriptBuilder r2 = h.NewScriptWithoutVerify($"{first}-restore", h.DbPath, spec, h.Model);
        r2.Add(Op.Restore(backupDir, restore2));
        await h.RunAsync(first, r2);
        ScriptBuilder v = h.NewScript($"{first}-verify-restored", restore2, spec, backup2);
        v.Verify().Dump();
        await h.RunAsync(first, v);
    }

    /// <summary>
    /// librocksdb 11.12 writes empty backup meta files when hosted by .NET 11 (see the compat tests); such a run is
    /// inconclusive.
    /// </summary>
    private static void SkipIfNativeMetaIsEmpty(string backupDir)
    {
        uint id = NativeEngine.LatestBackupId(backupDir);
        if (new FileInfo(Path.Combine(backupDir, "meta", id.ToString(CultureInfo.InvariantCulture))).Length == 0)
        {
            Assert.Inconclusive("native BackupEngine wrote an empty meta file (librocksdb 11.12 hosted by .NET " + Environment.Version + ")");
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // TTL

    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task TtlDatabase_BothWays(EngineKind first)
    {
        var spec = new DbSpec { TtlSeconds = 86400 };
        spec.ColumnFamilies.Add(new CfSpec { Name = "default", Compression = CompressionType.Snappy, Merge = MergeKind.None, WriteBufferSize = 256UL << 10 });
        RoundTripHarness h = CreateHarness(spec, first + "-first");
        var gen = new GeneratorOptions
        {
            Ops = 1000, AllowMerge = false, AllowRangeDelete = false, AllowSingleDelete = false, AllowSnapshots = false,
            AllowColumnFamilyChanges = false,
        };
        EngineKind engine = first;
        for (int i = 0; i < 4; i++)
        {
            ScriptBuilder b = h.NewScript($"{engine}-ttl-{i}");
            OpGenerator.Generate(b, gen);
            b.Verify().Dump();
            await h.RunAsync(engine, b);
            engine = Other(engine);
        }

        // Opened without TTL, every stored value carries a plausible 4-byte write time suffix.
        using var raw = new ManagedEngine();
        raw.Open(h.DbPath, new DbSpec { ColumnFamilies = [new CfSpec { Name = "default", Compression = CompressionType.Snappy, Merge = MergeKind.None }] });
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        int count = 0;
        foreach (KeyValuePair<byte[], byte[]> kv in raw.Scan("default", null))
        {
            Assert.IsTrue(kv.Value.Length >= TtlValue.TimestampLength, "value without TTL suffix: " + Hex.Show(kv.Key));
            int ts = TtlValue.GetTimestamp(kv.Value);
            Assert.IsTrue(ts > now - 3600 && ts <= now + 60, $"implausible TTL timestamp {ts} (now {now}) for {Hex.Show(kv.Key)}");
            count++;
        }
        Assert.AreEqual(h.Model.Current["default"].Live.Count, count);
    }

    // ---------------------------------------------------------------------------------------------------------
    // OPTIONS files

    [TestMethod]
    [DataRow(EngineKind.Native)]
    [DataRow(EngineKind.Managed)]
    public async Task OptionsFile_WrittenByOneLoadedByOther(EngineKind writer)
    {
        EngineKind loader = Other(writer);
        DbSpec spec = DbSpec.Standard(CompressionType.Zstd, 5, FilterKind.Ribbon, 128UL << 10, false);
        RoundTripHarness h = CreateHarness(spec, $"{writer}-writes");
        var gen = new GeneratorOptions { Ops = 1200, AllowColumnFamilyChanges = false };

        ScriptBuilder b = h.NewScript($"{writer}-write");
        OpGenerator.Generate(b, gen);
        b.Verify();
        await h.RunAsync(writer, b);

        // The loader opens the DB with the options recorded in the writer's latest OPTIONS file (incl. merge
        // operators instantiated by name), reads, merges, flushes and verifies.
        DbSpec fromFile = spec.Clone();
        fromFile.OptionsFromFile = true;
        ScriptBuilder l = h.NewScript($"{loader}-options-from-file", h.DbPath, fromFile, h.Model);
        OpGenerator.Generate(l, gen);
        l.Add(Op.Flush("default")).Add(Op.Flush("counters")).Verify().Dump();
        await h.RunAsync(loader, l);

        // And back: the writer opens what the loader wrote using the loader's OPTIONS file.
        await h.RunAsync(writer, h.NewScript($"{writer}-options-back", h.DbPath, fromFile, h.Model));
        await h.RunAsync(writer, h.NewScript($"{writer}-own-options"));
    }
}
