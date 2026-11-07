using System.Globalization;
using System.Text;
using RocksDb.Managed.RoundTrip.Protocol;

namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>Shape of the keys, values and op mix of a generated stream.</summary>
public enum DataPattern
{
    /// <summary>Uniformly random keys, the default op mix.</summary>
    Random,
    /// <summary>Keys written in increasing order (append-style; trivial moves, non-overlapping files).</summary>
    Sequential,
    /// <summary>Keys written in decreasing order.</summary>
    Reverse,
    /// <summary>A few dozen hot keys overwritten again and again (many versions per key, snapshots pin old ones).</summary>
    HotKeys,
    /// <summary>Keys with a 120-byte shared prefix (prefix compression in blocks, index shortening).</summary>
    LongPrefix,
    /// <summary>Mostly deletes, single deletes and wide range deletes overlapping across files and levels.</summary>
    TombstoneHeavy,
    /// <summary>Mostly merges on a small key set, so operand chains span memtable, L0 and the bottom level.</summary>
    MergeHeavy,
    /// <summary>Tiny (0-4 byte) values mixed with huge (1-3 MiB) values.</summary>
    ValueSizes,
}

/// <summary>When the generated stream flushes memtables.</summary>
public enum FlushPattern
{
    /// <summary>Random per-CF flushes.</summary>
    Random,
    /// <summary>No flush ops: only write-buffer-full and recovery flushes.</summary>
    None,
    /// <summary>Every column family is flushed every <see cref="GeneratorOptions.FlushEvery"/> ops.</summary>
    EveryN,
    /// <summary>All column families flushed together (FlushAll; atomic with atomic_flush) at random points.</summary>
    AllAtOnce,
}

/// <summary>Kind of manual compaction the stream issues.</summary>
public enum ManualCompaction
{
    /// <summary>Full-range CompactRange with default options.</summary>
    Plain,
    /// <summary>Partial or full CompactRange with bottommost_level_compaction=kForce(Optimized) and exclusive_manual_compaction.</summary>
    BottommostExclusive,
    /// <summary>Full CompactRange with change_level=true, target_level=1 (level compaction only).</summary>
    ChangeLevel,
    /// <summary>DeleteFilesInRange over a key range followed by a DeleteRange of the same range.</summary>
    DeleteFiles,
}

/// <summary>External file ingestion mixed into the stream (SST written by the same engine, then ingested).</summary>
public enum IngestMode
{
    None,
    /// <summary>Files overlapping existing keys (global sequence numbers).</summary>
    GlobalSeqno,
    /// <summary>ingest_behind (needs allow_ingest_behind) of fresh keys into the reserved last level.</summary>
    Behind,
    /// <summary>allow_global_seqno=false: files of fresh keys above every other key, kept at sequence number 0 (with a
    /// snapshot open, overlapping files with global sequence numbers instead). Not mixed with
    /// <see cref="Behind"/>: upstream refuses ingest_behind while files with sequence number 0 sit above the last level.</summary>
    NoGlobalSeqno,
}

/// <summary>Knobs for <see cref="OpGenerator"/>.</summary>
public sealed class GeneratorOptions
{
    public int Ops { get; set; } = 1500;
    public int KeySpace { get; set; } = 3000;
    public bool AllowFlush { get; set; } = true;
    public bool AllowCompact { get; set; } = true;
    public bool AllowSnapshots { get; set; } = true;
    public bool AllowColumnFamilyChanges { get; set; } = true;
    public bool AllowSingleDelete { get; set; } = true;
    public bool AllowRangeDelete { get; set; } = true;
    public bool AllowMerge { get; set; } = true;
    /// <summary>Probability of a large value (&gt; 64 KiB) per put.</summary>
    public double LargeValueRate { get; set; } = 0.002;

    public DataPattern Pattern { get; set; }
    public FlushPattern Flushes { get; set; }
    public int FlushEvery { get; set; } = 150;
    public ManualCompaction Compaction { get; set; }
    public IngestMode Ingest { get; set; }
    /// <summary>Mix in disableWAL writes. Each disableWAL segment ends with a FlushAll when
    /// <see cref="FlushAfterNoWal"/> is set (avoid_flush_during_shutdown or an abandoned process would lose them).</summary>
    public bool NoWalWrites { get; set; }
    public bool FlushAfterNoWal { get; set; }
    /// <summary>Mix in FlushWAL calls (manual_wal_flush).</summary>
    public bool FlushWalOps { get; set; }
    /// <summary>Probability that a "sync" op turns synced single writes on.</summary>
    public double SyncRate { get; set; } = 1.0 / 3;
}

/// <summary>
/// Seeded random op streams that respect the RocksDB API contracts, so the model's expected contents are well defined:
/// <list type="bullet">
/// <item>SingleDelete only on "sd/" keys, which only ever see Put (when absent) and SingleDelete (when present);
/// range deletes, deletes, merges, DeleteFilesInRange and ingestion never touch them.</item>
/// <item>Merges only on column families with a merge operator; uint64add CFs only get 8-byte values.</item>
/// <item>DeleteFilesInRange only without an open snapshot (it drops data a snapshot could see), always followed by a
/// DeleteRange of the same range; ingestion with allow_global_seqno=false also only without a snapshot.</item>
/// </list>
/// Keys include ASCII "k" keys, binary keys with 0x00 / 0xff bytes and the empty key; values include empty and
/// large (&gt; block size, &gt; 64 KiB) values. Main keys are <see cref="KeyFor"/>(i), increasing in i for every pattern,
/// so key ranges [KeyFor(a), KeyFor(b)) are well defined.
/// </summary>
public static class OpGenerator
{
    private static readonly string s_longPrefix = new('p', 120);

    /// <summary>Main key number <paramref name="i"/> (monotonic in i).</summary>
    public static byte[] KeyFor(GeneratorOptions o, int i) =>
        Encoding.ASCII.GetBytes((o.Pattern == DataPattern.LongPrefix ? "k" + s_longPrefix : "k") + i.ToString("D6", CultureInfo.InvariantCulture));

    private sealed class State
    {
        public int Sequential;
        public int Reverse;
    }

    public static void Generate(ScriptBuilder b, GeneratorOptions o)
    {
        Random r = b.Random;
        ReferenceModel m = b.Model;
        var st = new State { Sequential = r.Next(o.KeySpace), Reverse = o.KeySpace - 1 - r.Next(o.KeySpace / 4 + 1) };
        long nextSnapshot = 1000 + r.Next(1000) * 1000;
        long openSnapshot = -1;
        int opsSinceSnapshot = 0;
        int noWalLeft = 0;
        // Op mix per pattern: thresholds out of 1000 for put / merge / delete / single delete / range delete.
        (int put, int merge, int del, int sd, int rd) mix = o.Pattern switch
        {
            DataPattern.TombstoneHeavy => (250, 300, 560, 700, 760),
            DataPattern.MergeHeavy => (150, 640, 700, 740, 760),
            DataPattern.HotKeys => (520, 640, 700, 740, 760),
            _ => (420, 560, 660, 740, 760),
        };
        for (int i = 0; i < o.Ops; i++)
        {
            string cf = PickCf(r, m);
            if (o.Flushes == FlushPattern.EveryN && o.AllowFlush && i > 0 && i % o.FlushEvery == 0)
            {
                foreach (string c in new List<string>(m.ColumnFamilyNames)) b.Add(Op.Flush(c));
            }
            if (o.NoWalWrites)
            {
                if (noWalLeft == 0 && r.Next(60) == 0)
                {
                    noWalLeft = 5 + r.Next(30);
                    b.Add(Op.NoWal(true));
                }
                else if (noWalLeft > 0 && --noWalLeft == 0) EndNoWal(b, o);
            }
            int roll = r.Next(1000);
            if (roll < mix.put)
            {
                b.Add(Op.Put(cf, MainKey(r, o, st), Value(r, m.MergeOf(cf), o)));
            }
            else if (roll < mix.merge)
            {
                if (o.AllowMerge && m.MergeOf(cf) != MergeKind.None) b.Add(Op.Merge(cf, MainKey(r, o, st), Operand(r, m.MergeOf(cf))));
                else b.Add(Op.Put(cf, MainKey(r, o, st), Value(r, m.MergeOf(cf), o)));
            }
            else if (roll < mix.del)
            {
                b.Add(Op.Delete(cf, ExistingOrRandomKey(r, m, cf, o, st)));
            }
            else if (roll < mix.sd)
            {
                if (o.AllowSingleDelete) SingleDeleteStep(b, r, cf);
            }
            else if (roll < mix.rd)
            {
                if (o.AllowRangeDelete) b.Add(RangeDelete(r, cf, o));
            }
            else if (roll < 900)
            {
                var ops = new List<Op>();
                int n = 1 + r.Next(20);
                for (int j = 0; j < n; j++)
                {
                    string c = PickCf(r, m);
                    int kind = r.Next(10);
                    if (kind < 5) ops.Add(Op.Put(c, MainKey(r, o, st), Value(r, m.MergeOf(c), o)));
                    else if (kind < 7 && o.AllowMerge && m.MergeOf(c) != MergeKind.None) ops.Add(Op.Merge(c, MainKey(r, o, st), Operand(r, m.MergeOf(c))));
                    else if (kind < 9) ops.Add(Op.Delete(c, ExistingOrRandomKey(r, m, c, o, st)));
                    else if (o.AllowRangeDelete) ops.Add(RangeDelete(r, c, o));
                }
                b.Batch(r.Next(4) == 0, ops);
            }
            else if (roll < 915)
            {
                if (o.FlushWalOps && r.Next(3) == 0) b.Add(Op.FlushWal(r.Next(2) == 0));
                else b.Add(Op.Sync(r.NextDouble() < o.SyncRate));
            }
            else if (roll < 930)
            {
                if (o.AllowFlush)
                {
                    switch (o.Flushes)
                    {
                        case FlushPattern.Random: b.Add(Op.Flush(cf)); break;
                        case FlushPattern.AllAtOnce: if (r.Next(3) == 0) b.Add(Op.FlushAll()); break;
                    }
                }
            }
            else if (roll < 945)
            {
                if (o.AllowSnapshots)
                {
                    if (openSnapshot < 0)
                    {
                        openSnapshot = nextSnapshot++;
                        opsSinceSnapshot = 0;
                        b.Add(Op.Snap(openSnapshot));
                    }
                    else if (opsSinceSnapshot > 30)
                    {
                        b.SnapVerify(openSnapshot);
                        b.Add(Op.SnapRelease(openSnapshot));
                        openSnapshot = -1;
                    }
                }
            }
            else if (roll < 950)
            {
                if (o.AllowCompact) ManualCompactionStep(b, r, cf, o, openSnapshot >= 0);
            }
            else if (roll < 953)
            {
                if (o.AllowColumnFamilyChanges && b.Spec.ColumnFamilies.Exists(c => c.Name == "late"))
                {
                    if (m.HasCf("late")) b.Add(Op.DropCf("late"));
                    else b.Add(Op.CreateCf("late"));
                }
            }
            else if (roll < 956)
            {
                if (o.Ingest != IngestMode.None && !b.Spec.Cf(cf).UserTimestamps) IngestStep(b, r, cf, o, st, openSnapshot >= 0);
            }
            else
            {
                // Reads/writes of edge-case keys: empty key, keys made of 0x00 / 0xff bytes.
                byte[] k = EdgeKey(r);
                if (r.Next(3) == 0) b.Add(Op.Delete(cf, k));
                else b.Add(Op.Put(cf, k, Value(r, m.MergeOf(cf), o)));
            }
            opsSinceSnapshot++;
        }
        if (noWalLeft > 0) EndNoWal(b, o);
        if (openSnapshot >= 0)
        {
            b.SnapVerify(openSnapshot);
            b.Add(Op.SnapRelease(openSnapshot));
        }
    }

    private static void EndNoWal(ScriptBuilder b, GeneratorOptions o)
    {
        b.Add(Op.NoWal(false));
        if (o.FlushAfterNoWal) b.Add(Op.FlushAll());
    }

    private static void ManualCompactionStep(ScriptBuilder b, Random r, string cf, GeneratorOptions o, bool snapshotOpen)
    {
        switch (o.Compaction)
        {
            case ManualCompaction.Plain:
                b.Add(Op.Compact(cf));
                break;
            case ManualCompaction.BottommostExclusive:
                {
                    var spec = new CompactSpec(r.Next(2) == 0 ? BottommostLevelCompaction.Force : BottommostLevelCompaction.ForceOptimized,
                        Exclusive: r.Next(2) == 0, ChangeLevel: false, TargetLevel: -1, MaxSubcompactions: (uint)(1 + r.Next(3)));
                    if (r.Next(2) == 0) b.Add(Op.CompactEx(cf, null, null, spec));
                    else
                    {
                        int a = r.Next(o.KeySpace);
                        b.Add(Op.CompactEx(cf, KeyFor(o, a), KeyFor(o, a + 1 + r.Next(o.KeySpace / 3)), spec));
                    }
                    break;
                }
            case ManualCompaction.ChangeLevel:
                b.Add(Op.CompactEx(cf, null, null, new CompactSpec(BottommostLevelCompaction.IfHaveCompactionFilter, false, ChangeLevel: true, TargetLevel: 1, 1)));
                break;
            case ManualCompaction.DeleteFiles:
                {
                    if (snapshotOpen || b.Spec.Cf(cf).UserTimestamps)
                    {
                        b.Add(Op.Compact(cf));
                        break;
                    }
                    int a = r.Next(o.KeySpace);
                    byte[] begin = KeyFor(o, a);
                    // end = a key no stream ever writes, so include_end = true and the exclusive DeleteRange agree.
                    byte[] end = [.. KeyFor(o, a + 1 + r.Next(o.KeySpace / 4)), 0x00];
                    b.Add(Op.DeleteFiles(cf, begin, end)).Add(Op.DeleteRange(cf, begin, end));
                    break;
                }
        }
    }

    /// <summary>Key spaces above every other key: ingest_behind files and allow_global_seqno=false files.</summary>
    private static byte[] IngestKey(byte space, int file, int i) =>
        [0xff, 0xff, 0xff, space, (byte)(file >> 8), (byte)file, (byte)(i >> 8), (byte)i];

    private static void IngestStep(ScriptBuilder b, Random r, string cf, GeneratorOptions o, State st, bool snapshotOpen)
    {
        ReferenceModel m = b.Model;
        MergeKind merge = m.MergeOf(cf);
        string path = b.NewFile($"ingest-{cf}.sst");
        int id = ++m.IngestCounter;
        if (o.Ingest == IngestMode.GlobalSeqno || snapshotOpen && o.Ingest == IngestMode.NoGlobalSeqno)
        {
            // Overlapping main keys: the file gets a global sequence number above everything in the CF.
            var keys = new SortedSet<int>();
            int n = 5 + r.Next(60);
            for (int j = 0; j < n; j++) keys.Add(MainIndex(r, o, st));
            b.Add(Op.SstBegin(cf, path));
            foreach (int k in keys)
            {
                int kind = r.Next(10);
                if (kind < 6) b.Add(Op.SstPut(KeyFor(o, k), Value(r, merge, o)));
                else if (kind < 8 && merge != MergeKind.None) b.Add(Op.SstMerge(KeyFor(o, k), Operand(r, merge)));
                else b.Add(Op.SstDelete(KeyFor(o, k)));
            }
            b.Add(Op.SstEnd()).Add(Op.Ingest(cf, [path], r.Next(2) == 0 ? IngestFlags.Move : IngestFlags.None));
            return;
        }
        bool behind = o.Ingest == IngestMode.Behind;
        // An ingested-behind file has sequence number 0, so older snapshots see it too (outside the model).
        if (behind && snapshotOpen) return;
        byte space = behind ? (byte)0xfe : (byte)0xff;
        b.Add(Op.SstBegin(cf, path));
        int count = 3 + r.Next(40);
        for (int j = 0; j < count; j++) b.Add(Op.SstPut(IngestKey(space, id, j), Value(r, merge, o)));
        b.Add(Op.SstEnd()).Add(Op.Ingest(cf, [path], behind ? IngestFlags.IngestBehind : IngestFlags.NoGlobalSeqno));
    }

    private static string PickCf(Random r, ReferenceModel m)
    {
        var names = new List<string>(m.ColumnFamilyNames);
        // default gets half of the traffic.
        return r.Next(2) == 0 ? "default" : names[r.Next(names.Count)];
    }

    /// <summary>Uniformly random main key (pattern-independent; for callers without a stream state).</summary>
    public static byte[] MainKey(Random r, GeneratorOptions o) => KeyFor(o, r.Next(o.KeySpace));

    private static int MainIndex(Random r, GeneratorOptions o, State st) => o.Pattern switch
    {
        DataPattern.Sequential => st.Sequential = (st.Sequential + 1 + (r.Next(8) == 0 ? r.Next(5) : 0)) % (o.KeySpace * 4),
        DataPattern.Reverse => st.Reverse = st.Reverse > 0 ? st.Reverse - 1 - (r.Next(8) == 0 ? Math.Min(st.Reverse - 1, r.Next(5)) : 0) : o.KeySpace * 4 - 1,
        DataPattern.HotKeys => r.Next(10) == 0 ? r.Next(o.KeySpace) : r.Next(24) * 37,
        DataPattern.MergeHeavy => r.Next(5) == 0 ? r.Next(o.KeySpace) : r.Next(60) * 11,
        _ => r.Next(o.KeySpace),
    };

    private static byte[] MainKey(Random r, GeneratorOptions o, State st) => KeyFor(o, MainIndex(r, o, st));

    private static byte[] ExistingOrRandomKey(Random r, ReferenceModel m, string cf, GeneratorOptions o, State st)
    {
        CfContents c = m.Current[cf];
        if (c.Live.Count > 0 && r.Next(2) == 0)
        {
            // Pick a near-random existing non-"sd/" key by seeking from a random main key.
            byte[] probe = MainKey(r, o, st);
            foreach (byte[] k in c.Live.Keys)
            {
                if (ByteComparer.Instance.Compare(k, probe) >= 0 && !IsSdKey(k)) return k;
            }
        }
        return MainKey(r, o, st);
    }

    private static bool IsSdKey(byte[] k) => k.Length >= 3 && k[0] == (byte)'s' && k[1] == (byte)'d' && k[2] == (byte)'/';

    private static void SingleDeleteStep(ScriptBuilder b, Random r, string cf)
    {
        byte[] k = Encoding.ASCII.GetBytes("sd/" + r.Next(200).ToString("D4", CultureInfo.InvariantCulture));
        if (b.Model.Current[cf].Live.ContainsKey(k)) b.Add(Op.SingleDelete(cf, k));
        else b.Add(Op.Put(cf, k, b.Model.MergeOf(cf) == MergeKind.UInt64Add ? ReferenceModel.U64((ulong)r.NextInt64()) : RandomBytes(r, r.Next(1, 40))));
    }

    private static Op RangeDelete(Random r, string cf, GeneratorOptions o)
    {
        if (r.Next(5) == 0)
        {
            // Binary key space below "k": [0x00.., 0x00 0x80).
            return Op.DeleteRange(cf, [0x00], [0x00, 0x80]);
        }
        int a = r.Next(o.KeySpace);
        // Tombstone-heavy streams use wide ranges that overlap many files and levels.
        int width = o.Pattern == DataPattern.TombstoneHeavy && r.Next(3) == 0 ? 1 + r.Next(o.KeySpace / 3) : 1 + r.Next(40);
        return Op.DeleteRange(cf, KeyFor(o, a), KeyFor(o, a + width));
    }

    private static byte[] EdgeKey(Random r) => r.Next(6) switch
    {
        0 => [],
        1 => [0x00],
        2 => [0x00, 0x00, (byte)r.Next(256)],
        3 => [0xff],
        4 => [0xff, 0xff, (byte)r.Next(256), 0x00],
        _ => [0x00, (byte)r.Next(0x80), 0xff],
    };

    public static byte[] RandomBytes(Random r, int n)
    {
        byte[] b = new byte[n];
        r.NextBytes(b);
        return b;
    }

    private static byte[] Value(Random r, MergeKind merge, GeneratorOptions o)
    {
        if (merge == MergeKind.UInt64Add) return ReferenceModel.U64((ulong)r.Next(1_000_000));
        double p = r.NextDouble();
        if (o.Pattern == DataPattern.ValueSizes)
        {
            if (p < 0.006) return RandomBytes(r, (1 << 20) + r.Next(2 << 20)); // 1-3 MiB
            if (p < 0.6) return RandomBytes(r, r.Next(5));                    // 0-4 bytes
        }
        if (p < o.LargeValueRate) return RandomBytes(r, 65 * 1024 + r.Next(150 * 1024));
        if (p < 0.05) return [];
        if (p < 0.08) return RandomBytes(r, 4096 + r.Next(12 * 1024)); // larger than the block size
        int n = r.Next(1, 100);
        byte[] v = new byte[n];
        for (int i = 0; i < n; i++) v[i] = (byte)('a' + r.Next(26));
        return v;
    }

    private static byte[] Operand(Random r, MergeKind merge) => merge switch
    {
        MergeKind.UInt64Add => ReferenceModel.U64((ulong)r.Next(1000)),
        MergeKind.BytesXor => RandomBytes(r, r.Next(0, 12)),
        _ => Encoding.ASCII.GetBytes("m" + r.Next(1000).ToString(CultureInfo.InvariantCulture)),
    };
}
