using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.CompatTests.Api;

/// <summary>
/// Compatibility of the operational / replication APIs with native RocksDB (rocksdb-sharp): GetUpdatesSince over WALs
/// written by the other side, checkpoints opened by the other side, SstFileWriter output ingested by the other side,
/// read-only / secondary instances over a live native primary, and a replication round trip in the style of
/// rocksdb-sharp's ReplicationSource / ReplicationConsumer (checkpoint, then stream WAL batches into the replica).
/// </summary>
[TestClass]
public sealed class ReplicationCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);
    private static string K(int i) => "key" + i.ToString("D6");
    private static string V(int i, int round) => "v" + round + "-" + i + new string((char)('a' + i % 26), i % 40);

    private static RocksDbSharp.DbOptions NativeOptions(bool walTtl)
    {
        var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
        if (walTtl) o.SetWalTtlSeconds(3600);
        s_keepAlive.Add(o);
        return o;
    }

    private static DbOptions ManagedOptions(bool walTtl) => new() { CreateIfMissing = true, WalTtlSeconds = walTtl ? 3600UL : 0UL };

    /// <summary>Applies a deterministic workload as batches (puts, deletes, range deletes) through <paramref name="write"/>.</summary>
    private static void Workload(int from, int to, Action<List<(char Op, string A, string? B)>> write)
    {
        for (int i = from; i < to; i++)
        {
            var ops = new List<(char, string, string?)>();
            ops.Add(('p', K(i), V(i, 0)));
            if (i % 3 == 0) ops.Add(('p', K(i + 1), V(i, 1)));
            if (i % 7 == 0) ops.Add(('d', K(i - 5 < 0 ? 0 : i - 5), null));
            if (i % 29 == 0) ops.Add(('r', K(i - 20 < 0 ? 0 : i - 20), K(i - 15 < 0 ? 1 : i - 15)));
            write(ops);
        }
    }

    private static void NativeWrite(RocksDbSharp.RocksDb db, List<(char Op, string A, string? B)> ops)
    {
        using var b = new RocksDbSharp.WriteBatch();
        foreach ((char op, string a, string? v) in ops)
        {
            switch (op)
            {
                case 'p': b.Put(U(a), U(v!)); break;
                case 'd': b.Delete(U(a)); break;
                default:
                    byte[] x = U(a), y = U(v!);
                    b.DeleteRange(x, (ulong)x.Length, y, (ulong)y.Length);
                    break;
            }
        }
        db.Write(b);
    }

    private static void ManagedWrite(RocksDatabase db, List<(char Op, string A, string? B)> ops)
    {
        using WriteBatch b = db.CreateWriteBatch();
        foreach ((char op, string a, string? v) in ops)
        {
            switch (op)
            {
                case 'p': b.Put(U(a), U(v!)); break;
                case 'd': b.Delete(U(a)); break;
                default: b.DeleteRange(U(a), U(v!)); break;
            }
        }
        db.Write(b);
    }

    private static List<(ulong Seq, byte[] Rep)> NativeUpdates(RocksDbSharp.RocksDb db, ulong from)
    {
        var list = new List<(ulong, byte[])>();
        using RocksDbSharp.TransactionLogIterator it = db.GetUpdatesSince(from);
        for (; it.Valid(); it.Next())
        {
            using RocksDbSharp.WriteBatch b = it.GetBatch(out ulong seq);
            list.Add((seq, b.ToBytes()));
        }
        return list;
    }

    private static List<(ulong Seq, byte[] Rep)> ManagedUpdates(RocksDatabase db, ulong from)
    {
        var list = new List<(ulong, byte[])>();
        using TransactionLogIterator it = db.GetUpdatesSince(from);
        for (; it.Valid; it.Next()) list.Add((it.Current.Sequence, it.Current.Span.ToArray()));
        it.ThrowIfError();
        return list;
    }

    private static void AssertSameUpdates(List<(ulong Seq, byte[] Rep)> expected, List<(ulong Seq, byte[] Rep)> actual)
    {
        Assert.AreEqual(expected.Count, actual.Count, "batch count");
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.AreEqual(expected[i].Seq, actual[i].Seq, "seq of batch " + i);
            CollectionAssert.AreEqual(expected[i].Rep, actual[i].Rep, "bytes of batch " + i);
        }
    }

    private static SortedDictionary<string, string> NativeContents(RocksDbSharp.RocksDb db)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using RocksDbSharp.Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid(); it.Next()) d[Encoding.UTF8.GetString(it.Key())] = Encoding.UTF8.GetString(it.Value());
        return d;
    }

    private static SortedDictionary<string, string> ManagedContents(RocksDatabase db)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using Iterator it = db.NewIterator();
        for (it.SeekToFirst(); it.Valid; it.Next()) d[Encoding.UTF8.GetString(it.Key)] = Encoding.UTF8.GetString(it.Value);
        return d;
    }

    private static void AssertSameContents(SortedDictionary<string, string> expected, SortedDictionary<string, string> actual, string what)
    {
        Assert.AreEqual(expected.Count, actual.Count, what + ": key count");
        foreach (KeyValuePair<string, string> kv in expected)
        {
            Assert.IsTrue(actual.TryGetValue(kv.Key, out string? v), what + ": missing " + kv.Key);
            Assert.AreEqual(kv.Value, v, what + ": value of " + kv.Key);
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // GetUpdatesSince

    [TestMethod]
    public void ManagedGetUpdatesSinceOverNativeWals()
    {
        using var dir = new TempDir("upd-n");
        List<(ulong, byte[])> expected;
        SortedDictionary<string, string> contents;
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: true), dir.Path))
        {
            Workload(0, 150, ops => NativeWrite(n, ops));
            n.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true)); // WAL archived
            Workload(150, 300, ops => NativeWrite(n, ops));
            expected = NativeUpdates(n, 1);
            contents = NativeContents(n);
        }
        Assert.IsTrue(Directory.Exists(Path.Combine(dir.Path, "archive")));
        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(dir.Path, new DbOptions()))
        {
            AssertSameUpdates(expected, ManagedUpdates(ro, 1));
            AssertSameContents(contents, ManagedContents(ro), "read-only");
            int m = expected.FindIndex(static e => new WalBatch(e.Item2).Count > 1);
            List<(ulong, byte[])> mid = ManagedUpdates(ro, expected[m].Item1 + 1); // inside a multi-entry batch
            AssertSameUpdates(expected.GetRange(m, expected.Count - m), mid);
        }
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, ManagedOptions(walTtl: true)))
        {
            // Opening read-write recovers (flushes) the WAL; with WAL_ttl_seconds it is archived, so still readable.
            AssertSameUpdates(expected, ManagedUpdates(db, 1));
        }
    }

    [TestMethod]
    public void NativeGetUpdatesSinceOverManagedWals()
    {
        using var dir = new TempDir("upd-m");
        List<(ulong, byte[])> expected;
        SortedDictionary<string, string> contents;
        using (RocksDatabase db = RocksDatabase.Open(dir.Path, ManagedOptions(walTtl: true)))
        {
            Workload(0, 150, ops => ManagedWrite(db, ops));
            db.Flush();
            Workload(150, 300, ops => ManagedWrite(db, ops));
            expected = ManagedUpdates(db, 1);
            contents = ManagedContents(db);
            Assert.AreEqual(db.GetLatestSequenceNumber(), expected[^1].Item1 + (ulong)new WalBatch(expected[^1].Item2).Count - 1);
        }
        using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: true), dir.Path);
        AssertSameUpdates(expected, NativeUpdates(n, 1));
        AssertSameContents(contents, NativeContents(n), "native");
    }

    // ---------------------------------------------------------------------------------------------------
    // Checkpoint

    [TestMethod]
    public void ManagedCheckpointOpensNatively()
    {
        using var dir = new TempDir("cp-m");
        string cpFlush = Path.Combine(dir.Path, "cp-flush"), cpWal = Path.Combine(dir.Path, "cp-wal"), db1 = Path.Combine(dir.Path, "db");
        SortedDictionary<string, string> contents;
        using (RocksDatabase db = RocksDatabase.Open(db1, ManagedOptions(walTtl: false)))
        {
            Workload(0, 200, ops => ManagedWrite(db, ops));
            db.Flush();
            Workload(200, 400, ops => ManagedWrite(db, ops));
            contents = ManagedContents(db);
            ulong s1 = db.CreateCheckpoint(cpWal, ulong.MaxValue);
            ulong s2 = db.CreateCheckpoint(cpFlush);
            Assert.AreEqual(s1, s2);
            // The table files are hard links of the DB's files.
            Assert.IsTrue(Directory.GetFiles(cpFlush, "*.sst").Length >= 2);
            Workload(400, 450, ops => ManagedWrite(db, ops)); // not in the checkpoints
        }
        foreach (string cp in new[] { cpWal, cpFlush })
        {
            using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: false), cp);
            AssertSameContents(contents, NativeContents(n), cp);
        }
    }

    [TestMethod]
    public void NativeCheckpointOpensManaged()
    {
        using var dir = new TempDir("cp-n");
        string cp = Path.Combine(dir.Path, "cp"), db1 = Path.Combine(dir.Path, "db");
        SortedDictionary<string, string> contents;
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: false), db1))
        {
            Workload(0, 200, ops => NativeWrite(n, ops));
            n.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            Workload(200, 300, ops => NativeWrite(n, ops));
            contents = NativeContents(n);
            using RocksDbSharp.Checkpoint c = n.Checkpoint();
            c.Save(cp, ulong.MaxValue);
        }
        using RocksDatabase db = RocksDatabase.Open(cp, ManagedOptions(walTtl: false));
        AssertSameContents(contents, ManagedContents(db), "managed");
    }

    // ---------------------------------------------------------------------------------------------------
    // SstFileWriter / ingestion

    [TestMethod]
    public void ManagedSstFileWriterIngestedByNative()
    {
        using var dir = new TempDir("sst-m");
        string f1 = Path.Combine(dir.Path, "1.sst"), f2 = Path.Combine(dir.Path, "2.sst");
        using (var w = new SstFileWriter(new ColumnFamilyOptions()))
        {
            w.Open(f1);
            for (int i = 0; i < 1000; i++) w.Put(U(K(i)), U(V(i, 7)));
            ExternalSstFileInfo info = w.Finish();
            Assert.AreEqual(1000UL, info.NumEntries);
        }
        using (var w = new SstFileWriter(new ColumnFamilyOptions { Compression = CompressionType.Zstd }))
        {
            w.Open(f2);
            w.Put(U(K(2000)), U("x")).Delete(U(K(2001))).Put(U(K(2002)), U("y")).DeleteRange(U(K(500)), U(K(510)));
            w.Finish();
        }
        using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: false), Path.Combine(dir.Path, "db"));
        n.Put(U(K(2001)), U("old"));
        n.IngestExternalFiles([f1], new RocksDbSharp.IngestExternalFileOptions());
        n.IngestExternalFiles([f2], new RocksDbSharp.IngestExternalFileOptions());
        Assert.AreEqual(V(5, 7), Encoding.UTF8.GetString(n.Get(U(K(5)))));
        Assert.IsNull(n.Get(U(K(505))));
        Assert.AreEqual(V(510, 7), Encoding.UTF8.GetString(n.Get(U(K(510)))));
        Assert.IsNull(n.Get(U(K(2001))));
        Assert.AreEqual("y", Encoding.UTF8.GetString(n.Get(U(K(2002)))));
    }

    [TestMethod]
    public async Task NativeSstFileWriterIngestedByManaged()
    {
        using var dir = new TempDir("sst-n");
        string f1 = Path.Combine(dir.Path, "1.sst"), f2 = Path.Combine(dir.Path, "2.sst");
        var cfo = new RocksDbSharp.ColumnFamilyOptions();
        s_keepAlive.Add(cfo);
        using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo))
        {
            w.Open(f1);
            for (int i = 0; i < 1000; i++) w.Put(U(K(i)), U(V(i, 9)));
            w.Finish();
        }
        using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo))
        {
            w.Open(f2);
            w.Put(U(K(100)), U("over"));
            w.Delete(U(K(101)));
            w.Finish();
        }
        await using RocksDatabase db = await RocksDatabase.OpenAsync(Path.Combine(dir.Path, "db"), ManagedOptions(walTtl: false));
        await db.IngestExternalFilesAsync([f1], new IngestExternalFileOptions { MoveFiles = true });
        Assert.IsFalse(File.Exists(f1));
        db.Put(U("zzz"), U("1"));
        using Snapshot snap = db.GetSnapshot();
        await db.IngestExternalFilesAsync([f2], new IngestExternalFileOptions());
        Assert.AreEqual(V(5, 9), Encoding.UTF8.GetString(db.Get(U(K(5)))!));
        Assert.AreEqual("over", Encoding.UTF8.GetString(db.Get(U(K(100)))!));
        Assert.IsNull(db.Get(U(K(101))));
        Assert.AreEqual(V(101, 9), Encoding.UTF8.GetString(db.Get(new ReadOptions { Snapshot = snap }, U(K(101)))!));
        await db.DisposeAsync();
        // And the result opens natively.
        using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: false), Path.Combine(dir.Path, "db"));
        Assert.AreEqual("over", Encoding.UTF8.GetString(n.Get(U(K(100)))));
        Assert.IsNull(n.Get(U(K(101))));
        Assert.AreEqual(V(999, 9), Encoding.UTF8.GetString(n.Get(U(K(999)))));
    }

    // ---------------------------------------------------------------------------------------------------
    // Read-only / secondary over a live native primary

    [TestMethod]
    public async Task ManagedSecondaryFollowsLiveNativePrimary()
    {
        using var dir = new TempDir("sec");
        string primaryPath = Path.Combine(dir.Path, "db");
        RocksDbSharp.DbOptions po = NativeOptions(walTtl: false);
        using RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.Open(po, primaryPath);
        Workload(0, 100, ops => NativeWrite(n, ops));

        using (RocksDatabase ro = RocksDatabase.OpenReadOnly(primaryPath, new DbOptions()))
        {
            AssertSameContents(NativeContents(n), ManagedContents(ro), "read-only");
        }

        await using RocksDatabase sec = await RocksDatabase.OpenAsSecondaryAsync(primaryPath, Path.Combine(dir.Path, "sec"), new DbOptions());
        AssertSameContents(NativeContents(n), ManagedContents(sec), "secondary at open");
        for (int round = 1; round <= 3; round++)
        {
            Workload(round * 100, round * 100 + 100, ops => NativeWrite(n, ops));
            await sec.TryCatchUpWithPrimaryAsync();
            AssertSameContents(NativeContents(n), ManagedContents(sec), "round " + round + " (WAL)");
            n.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            Workload(round * 100 + 50, round * 100 + 80, ops => NativeWrite(n, ops));
            await sec.TryCatchUpWithPrimaryAsync();
            AssertSameContents(NativeContents(n), ManagedContents(sec), "round " + round + " (flush)");
            Assert.AreEqual(n.GetLatestSequenceNumber(), sec.GetLatestSequenceNumber());
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Replication round trip (ReplicationSource / ReplicationConsumer style)

    [TestMethod]
    public async Task ReplicationRoundTripIntoManagedAndNativeReplicas()
    {
        using var dir = new TempDir("repl");
        string sourcePath = Path.Combine(dir.Path, "source");
        string cpManaged = Path.Combine(dir.Path, "cp-managed"), cpNative = Path.Combine(dir.Path, "cp-native");
        await using RocksDatabase source = await RocksDatabase.OpenAsync(sourcePath, ManagedOptions(walTtl: true));
        source.DisableFileDeletions(); // ReplicationSource requirement
        Workload(0, 300, ops => ManagedWrite(source, ops));
        source.Flush();
        Workload(300, 400, ops => ManagedWrite(source, ops));

        // GetInitialState: checkpoint (one per replica here).
        ulong s1 = await source.CreateCheckpointAsync(cpManaged, ulong.MaxValue);
        ulong s2 = await source.CreateCheckpointAsync(cpNative, ulong.MaxValue);
        Assert.AreEqual(s1, s2);

        await using RocksDatabase replica = await RocksDatabase.OpenAsync(cpManaged, ManagedOptions(walTtl: false));
        using RocksDbSharp.RocksDb nativeReplica = RocksDbSharp.RocksDb.Open(NativeOptions(walTtl: false), cpNative);
        Assert.AreEqual(s1, replica.GetLatestSequenceNumber());
        Assert.AreEqual(s1, nativeReplica.GetLatestSequenceNumber());

        // The source keeps writing (with flushes / WAL rotations) while the replicas stream its WAL.
        Task writer = Task.Run(async () =>
        {
            for (int i = 400; i < 1400; i++)
            {
                List<(char, string, string?)>? captured = null;
                Workload(i, i + 1, ops => captured = ops);
                ManagedWrite(source, captured!);
                if (i % 250 == 0) await source.FlushAsync();
            }
        });
        await writer;
        ulong target = source.GetLatestSequenceNumber();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await foreach (WalBatch b in source.TailUpdatesAsync(s1 + 1, cts.Token))
        {
            // ReplicationConsumer.IngestBatch: apply the primary's batch bytes as one write.
            using (WriteBatch wb = b.ToWriteBatch()) await replica.WriteAsync(wb);
            using (var nb = new RocksDbSharp.WriteBatch(b.Span.ToArray())) nativeReplica.Write(nb);
            if (b.LastSequence >= target) break;
        }
        Assert.AreEqual(target, replica.GetLatestSequenceNumber());
        Assert.AreEqual(target, nativeReplica.GetLatestSequenceNumber());
        SortedDictionary<string, string> expected = ManagedContents(source);
        AssertSameContents(expected, ManagedContents(replica), "managed replica");
        AssertSameContents(expected, NativeContents(nativeReplica), "native replica");
        source.EnableFileDeletions();
    }
    // ---------------------------------------------------------------------------------------------------
    // TTL

    [TestMethod]
    public void TtlValueEncodingMatchesNative()
    {
        using var dir = new TempDir("ttl");
        using (RocksDbSharp.RocksDb n = RocksDbSharp.RocksDb.OpenWithTtl(NativeOptions(walTtl: false), dir.Path, 3600))
        {
            n.Put(U("native"), U("n-value"));
            n.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            n.Put(U("native2"), U("n-value2"));
        }
        using (TtlDatabase db = TtlDatabase.Open(dir.Path, ManagedOptions(walTtl: false), 3600))
        {
            Assert.AreEqual("n-value", Encoding.UTF8.GetString(db.Get(U("native"))!));
            Assert.AreEqual("n-value2", Encoding.UTF8.GetString(db.Get(U("native2"))!));
            byte[] raw = db.Database.Get(U("native"))!;
            Assert.IsTrue(Math.Abs(TtlValue.GetTimestamp(raw) - TtlValue.Now) < 60);
            db.Put(U("managed"), U("m-value"));
        }
        using RocksDbSharp.RocksDb n2 = RocksDbSharp.RocksDb.OpenWithTtl(NativeOptions(walTtl: false), dir.Path, 3600);
        Assert.AreEqual("m-value", Encoding.UTF8.GetString(n2.Get(U("managed"))));
        Assert.AreEqual("n-value", Encoding.UTF8.GetString(n2.Get(U("native"))));
    }
}
