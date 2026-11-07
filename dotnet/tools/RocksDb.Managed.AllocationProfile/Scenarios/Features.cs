using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tracing;

namespace RocksDb.Managed.AllocationProfile;

/// <summary>
/// Optional features: TTL databases, attribute groups, cross-column-family iterators, MultiScan, PlainTable /
/// CuckooTable, blob files (cache, GC), the rate limiter, tracing, remote compaction, user-defined timestamps and merge
/// operands.
/// </summary>
internal static partial class Scenarios
{
    /// <summary>A trace sink that only counts bytes (the scenarios measure the tracer, not file I/O).</summary>
    private sealed class NullTraceWriter : TraceWriter
    {
        private ulong _size;

        public override void Write(ReadOnlySpan<byte> data) => _size += (ulong)data.Length;

        // Report 0 so the size limit never stops the trace during long measurements.
        public override ulong FileSize => 0;
    }

    /// <summary>A compaction service that runs every job in-process with <see cref="RocksDatabase.OpenAndCompactAsync"/>.</summary>
    private sealed class InProcessCompactionService(string dbPath) : CompactionService
    {
        private readonly Dictionary<string, byte[]> _jobs = new();
        private int _next;

        public override string Name => "InProcess";

        public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> input,
            CancellationToken cancellationToken)
        {
            lock (_jobs)
            {
                string id = "job-" + (++_next).ToString(System.Globalization.CultureInfo.InvariantCulture);
                _jobs[id] = input.ToArray();
                return ValueTask.FromResult(new CompactionServiceScheduleResponse(id, CompactionServiceJobStatus.Success));
            }
        }

        public override async ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken)
        {
            byte[] input;
            lock (_jobs)
            {
                input = _jobs[scheduledJobId];
                _jobs.Remove(scheduledJobId);
            }
            byte[] result = await RocksDatabase.OpenAndCompactAsync(new OpenAndCompactOptions(), dbPath, scheduledJobId, input,
                new CompactionServiceOptionsOverride(), cancellationToken);
            return new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, result);
        }
    }

    private static void AddFeatures(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);
        const int KeySpace = 200_000;

        // ---------------------------------------------------------------------------------------------- TTL

        static TtlDatabase OpenTtl(ScenarioContext ctx, ColumnFamilyOptions o) =>
            ctx.Own(TtlDatabase.Open(ctx.NewDirectory("ttl"), ScenarioContext.DefaultDbOptions(), o, 3600));

        list.Add(S("ttl.put", "ttl", PathKind.Hot, "TtlDatabase.Put(key, value) (timestamp suffix appended)", ctx =>
        {
            TtlDatabase db = OpenTtl(ctx, ScenarioContext.DefaultCfOptions());
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(keys[c.Next()], value); }));
        }));
        list.Add(S("ttl.put.async", "ttl", PathKind.Hot, "await TtlDatabase.PutAsync(key, value)", ctx =>
        {
            TtlDatabase db = OpenTtl(ctx, ScenarioContext.DefaultCfOptions());
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.PutAsync(keys[c.Next()], value);
            }));
        }));
        list.Add(S("ttl.write-batch", "ttl", PathKind.Hot, "reused WriteBatch: Clear + 10 Put + TtlDatabase.Write (rewritten with timestamps)", ctx =>
        {
            TtlDatabase db = OpenTtl(ctx, ScenarioContext.DefaultCfOptions());
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var batch = ctx.Own(new WriteBatch());
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    batch.Clear();
                    for (int j = 0; j < 10; j++) batch.Put(keys[c.Next()], value);
                    db.Write(batch);
                }
            }));
        }, chunk: 200));
        TtlDatabase TtlReadDb(ScenarioContext ctx, out byte[][] shuffled)
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.DisableAutoCompactions = true;
            TtlDatabase db = OpenTtl(ctx, o);
            byte[][] keys = ScenarioContext.Keys(ReadKeys);
            foreach (byte[] k in keys) db.Put(k, value);
            db.Flush();
            db.CompactRange();
            shuffled = ScenarioContext.Shuffled(keys);
            var w = new ArrayBufferWriter<byte>(256);
            foreach (byte[] k in shuffled) { w.ResetWrittenCount(); db.TryGet(k, w); }
            return db;
        }
        list.Add(S("ttl.get.sst-cached", "ttl", PathKind.Hot, "TtlDatabase.TryGet(key, reused IBufferWriter), sst-cached", ctx =>
        {
            TtlDatabase db = TtlReadDb(ctx, out byte[][] keys);
            var w = new ArrayBufferWriter<byte>(256);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { w.ResetWrittenCount(); db.TryGet(keys[c.Next()], w); } }));
        }));
        list.Add(S("ttl.iter.seek-next10.sst-cached", "ttl", PathKind.Hot, "reused TTL Iterator: Seek + 10 Next (Value strips the suffix)", ctx =>
        {
            TtlDatabase db = TtlReadDb(ctx, out byte[][] keys);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Value.Length; it.Next(); }
                }
            }));
        }));
        list.Add(S("ttl.compaction", "ttl", PathKind.Cold, $"4 x ({FlushEntries} Put + Flush) + CompactRange through the TTL compaction filter", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            TtlDatabase db = OpenTtl(ctx, o);
            byte[][] keys = ScenarioContext.Keys(FlushEntries * 4);
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int f = 0; f < 4; f++)
                    {
                        for (int k = 0; k < FlushEntries; k++) db.DefaultColumnFamily.Put(wo, keys[f * FlushEntries + k], value);
                        db.Flush();
                    }
                    db.CompactRange();
                }
            }));
        }, chunk: 1, notes: "Includes the Puts; per compaction."));

        // ---------------------------------------------------------------------------------------------- attribute groups

        (RocksDatabase Db, ColumnFamily Hot, ColumnFamily Cold, byte[][] Keys) AttributeGroupDb(ScenarioContext ctx, bool fill)
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            ColumnFamily hot = db.CreateColumnFamily("hot", o);
            ColumnFamily cold = db.CreateColumnFamily("cold", o);
            byte[][] keys = ScenarioContext.Keys(fill ? 50_000 : KeySpace);
            if (fill)
            {
                WideColumn[] hc = [WideColumn.Default(ScenarioContext.Value(40, 2)), new WideColumn("n"u8.ToArray(), ScenarioContext.Value(8, 3))];
                WideColumn[] cc = [new WideColumn("email"u8.ToArray(), ScenarioContext.Value(30, 4)), new WideColumn("city"u8.ToArray(), ScenarioContext.Value(20, 5))];
                AttributeGroup[] groups = [new AttributeGroup(hot, hc), new AttributeGroup(cold, cc)];
                var wo = new WriteOptions { DisableWal = true };
                foreach (byte[] k in keys) db.PutAttributeGroups(wo, k, groups);
                hot.Flush();
                cold.Flush();
                hot.CompactRange();
                cold.CompactRange();
                keys = ScenarioContext.Shuffled(keys);
                PinnableAttributeGroup[] read = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
                foreach (byte[] k in keys) db.GetAttributeGroups(null, k, read);
            }
            return (db, hot, cold, keys);
        }
        list.Add(S("attrgroup.put", "attrgroup", PathKind.Hot, "PutAttributeGroups(key, 2 groups x 2 columns)", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: false);
            WideColumn[] hc = [WideColumn.Default(ScenarioContext.Value(40, 2)), new WideColumn("n"u8.ToArray(), ScenarioContext.Value(8, 3))];
            WideColumn[] cc = [new WideColumn("email"u8.ToArray(), ScenarioContext.Value(30, 4)), new WideColumn("city"u8.ToArray(), ScenarioContext.Value(20, 5))];
            AttributeGroup[] groups = [new AttributeGroup(hot, hc), new AttributeGroup(cold, cc)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.PutAttributeGroups(keys[c.Next()], groups); }));
        }));
        list.Add(S("attrgroup.put.async", "attrgroup", PathKind.Hot, "await PutAttributeGroupsAsync(key, 2 groups x 2 columns)", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: false);
            WideColumn[] hc = [WideColumn.Default(ScenarioContext.Value(40, 2)), new WideColumn("n"u8.ToArray(), ScenarioContext.Value(8, 3))];
            WideColumn[] cc = [new WideColumn("email"u8.ToArray(), ScenarioContext.Value(30, 4)), new WideColumn("city"u8.ToArray(), ScenarioContext.Value(20, 5))];
            AttributeGroup[] groups = [new AttributeGroup(hot, hc), new AttributeGroup(cold, cc)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.PutAttributeGroupsAsync(null, keys[c.Next()], groups);
            }));
        }));
        list.Add(S("attrgroup.get", "attrgroup", PathKind.Hot, "GetAttributeGroups(key, 2 reused PinnableAttributeGroups), sst-cached", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            PinnableAttributeGroup[] read = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.GetAttributeGroups(null, keys[c.Next()], read); }));
        }));
        list.Add(S("attrgroup.get-one-cf", "attrgroup", PathKind.Hot, "GetAttributeGroups(key, 1 reused PinnableAttributeGroup), sst-cached", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, _, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            PinnableAttributeGroup[] read = [new PinnableAttributeGroup(hot)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.GetAttributeGroups(null, keys[c.Next()], read); }));
        }));
        list.Add(S("attrgroup.get-async", "attrgroup", PathKind.Hot, "await GetAttributeGroupsAsync(key, 2 reused PinnableAttributeGroups), sst-cached", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            PinnableAttributeGroup[] read = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.GetAttributeGroupsAsync(null, keys[c.Next()], read);
            }));
        }));
        list.Add(S("attrgroup.get-snapshot", "attrgroup", PathKind.Hot, "GetAttributeGroups(ReadOptions{Snapshot} reused, key, 2 reused groups), sst-cached", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            var ro = new ReadOptions { Snapshot = ctx.Own(db.GetSnapshot()) };
            PinnableAttributeGroup[] read = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.GetAttributeGroups(ro, keys[c.Next()], read); }));
        }));
        list.Add(S("attrgroup.multiget16-async", "attrgroup", PathKind.Hot, "await MultiGetAttributeGroupsAsync(16 keys x 2 reused groups) (per batch)", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            var k16 = new ReadOnlyMemory<byte>[16];
            var results = new PinnableAttributeGroup[16][];
            for (int j = 0; j < 16; j++) results[j] = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int j = 0; j < 16; j++) k16[j] = keys[c.Next()];
                    await db.MultiGetAttributeGroupsAsync(null, k16, results);
                }
            }));
        }, chunk: 50));
        list.Add(S("attrgroup.multiget16", "attrgroup", PathKind.Hot, "MultiGetAttributeGroups(16 keys x 2 reused groups) (per batch)", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            var k16 = new ReadOnlyMemory<byte>[16];
            var results = new PinnableAttributeGroup[16][];
            for (int j = 0; j < 16; j++) results[j] = [new PinnableAttributeGroup(hot), new PinnableAttributeGroup(cold)];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int j = 0; j < 16; j++) k16[j] = keys[c.Next()];
                    db.MultiGetAttributeGroups(null, k16, results);
                }
            }));
        }, chunk: 50));

        // ---------------------------------------------------------------------------------------------- multi-CF iterators

        list.Add(S("iter.coalescing.seek-next10", "attrgroup", PathKind.Hot, "reused CoalescingIterator (2 CFs): Seek + 10 x (Columns) Next", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            CoalescingIterator it = ctx.Own(db.NewCoalescingIterator(null, [hot, cold]));
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Columns.Count; it.Next(); }
                }
            }));
        }));
        list.Add(S("iter.attrgroup.seek-next10", "attrgroup", PathKind.Hot, "reused AttributeGroupIterator (2 CFs): Seek + 10 x (AttributeGroups) Next", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            AttributeGroupIterator it = ctx.Own(db.NewAttributeGroupIterator(null, [hot, cold]));
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++)
                    {
                        IReadOnlyList<IteratorAttributeGroup> g = it.AttributeGroups;
                        for (int k = 0; k < g.Count; k++) sum += g[k].Columns.Count;
                        it.Next();
                    }
                }
            }));
        }));
        list.Add(S("iter.coalescing.reverse", "attrgroup", PathKind.Hot, "reused CoalescingIterator (2 CFs): Seek + Next + 10 Prev (direction switch)", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            CoalescingIterator it = ctx.Own(db.NewCoalescingIterator(null, [hot, cold]));
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    if (it.Valid) it.Next();
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Columns.Count; it.Prev(); }
                }
            }));
        }));
        list.Add(S("iter.coalescing.create", "attrgroup", PathKind.Warm, "NewCoalescingIterator (2 CFs) + Seek + 10 Next + Dispose", ctx =>
        {
            (RocksDatabase db, ColumnFamily hot, ColumnFamily cold, byte[][] keys) = AttributeGroupDb(ctx, fill: true);
            ColumnFamily[] cfs = [hot, cold];
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using CoalescingIterator it = db.NewCoalescingIterator(null, cfs);
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Columns.Count; it.Next(); }
                }
            }));
        }));

        // ---------------------------------------------------------------------------------------------- MultiScan

        foreach (bool reuseArgs in (bool[])[true])
        {
            list.Add(S("multiscan.4ranges.sst-cached", "multiscan", PathKind.Hot, "NewMultiScan(4 ranges of 10 keys, reused MultiScanArgs) + scan all + Dispose", ctx =>
            {
                (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
                byte[][] sorted = ScenarioContext.Keys(ReadKeys);
                MultiScanArgs[] args = new MultiScanArgs[1000];
                for (int a = 0; a < args.Length; a++)
                {
                    var m = new MultiScanArgs();
                    int b = a * 97 % (ReadKeys - 200);
                    for (int r = 0; r < 4; r++) m.Add(sorted[b + r * 40], sorted[b + r * 40 + 10]);
                    args[a] = m;
                }
                var c = new Cursor(args.Length);
                long sum = 0;
                return Done(Runner.Sync(n =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        using MultiScan scan = db.NewMultiScan(args[c.Next()]);
                        while (scan.MoveNextRange())
                        {
                            for (; scan.Valid; scan.Next()) sum += scan.Value.Length;
                        }
                    }
                }));
            }));
            list.Add(S("multiscan.4ranges-async.sst-cached", "multiscan", PathKind.Hot, "NewMultiScan(4 ranges) + await MoveNextRangeAsync / NextAsync + Dispose", ctx =>
            {
                (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
                byte[][] sorted = ScenarioContext.Keys(ReadKeys);
                MultiScanArgs[] args = new MultiScanArgs[1000];
                for (int a = 0; a < args.Length; a++)
                {
                    var m = new MultiScanArgs();
                    int b = a * 97 % (ReadKeys - 200);
                    for (int r = 0; r < 4; r++) m.Add(sorted[b + r * 40], sorted[b + r * 40 + 10]);
                    args[a] = m;
                }
                var c = new Cursor(args.Length);
                long sum = 0;
                return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        using MultiScan scan = db.NewMultiScan(args[c.Next()]);
                        while (await scan.MoveNextRangeAsync())
                        {
                            while (scan.Valid)
                            {
                                sum += scan.Value.Length;
                                await scan.NextAsync();
                            }
                        }
                    }
                }));
            }));
        }

        // ---------------------------------------------------------------------------------------------- PlainTable / CuckooTable

        foreach (TableFactoryType tf in (TableFactoryType[])[TableFactoryType.PlainTable, TableFactoryType.CuckooTable])
        {
            string name = tf == TableFactoryType.PlainTable ? "plaintable" : "cuckoo";
            (RocksDatabase Db, byte[][] Keys) TableDb(ScenarioContext ctx)
            {
                return ReadDb(ctx, Location.Cached, configure: (d, o) =>
                {
                    d.AllowMmapReads = true;
                    o.TableFactory = tf;
                    // PlainTable's default (hash) mode needs a prefix extractor: 8-byte prefixes of "k%015d" keys.
                    if (tf == TableFactoryType.PlainTable) o.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
                    // Cuckoo tables are written by flush only (one file per memtable; no compaction into them).
                    o.WriteBufferSize = 64UL << 20;
                });
            }
            list.Add(S(name + ".get", "table-format", PathKind.Hot, $"TryGet(key, Span) from a {tf} SST (mmap)", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = TableDb(ctx);
                byte[] buf = new byte[256];
                var c = new Cursor(keys.Length);
                return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
            }));
            list.Add(S(name + ".get.async", "table-format", PathKind.Hot, $"await TryGetAsync(key, IBufferWriter) from a {tf} SST (mmap)", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = TableDb(ctx);
                var w = new ArrayBufferWriter<byte>(256);
                var c = new Cursor(keys.Length);
                return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    for (int i = 0; i < n; i++) { w.ResetWrittenCount(); await db.TryGetAsync(keys[c.Next()], w); }
                }));
            }));
            if (tf == TableFactoryType.PlainTable)
            {
                list.Add(S(name + ".iter.seek-next10", "table-format", PathKind.Hot, $"reused Iterator: Seek + 10 Next over a {tf} SST", ctx =>
                {
                    (RocksDatabase db, byte[][] keys) = TableDb(ctx);
                    Iterator it = ctx.Own(db.NewIterator());
                    var c = new Cursor(keys.Length);
                    long sum = 0;
                    return Done(Runner.Sync(n =>
                    {
                        for (int i = 0; i < n; i++)
                        {
                            it.Seek(keys[c.Next()]);
                            for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Value.Length; it.Next(); }
                        }
                    }));
                }));
            }
            else
            {
                list.Add(S(name + ".iter.next", "table-format", PathKind.Hot, $"reused Iterator: one Next over a {tf} SST (SeekToFirst when exhausted)", ctx =>
                {
                    (RocksDatabase db, _) = TableDb(ctx);
                    Iterator it = ctx.Own(db.NewIterator());
                    it.SeekToFirst();
                    long sum = 0;
                    return Done(Runner.Sync(n =>
                    {
                        for (int i = 0; i < n; i++)
                        {
                            if (!it.Valid) it.SeekToFirst();
                            sum += it.Value.Length;
                            it.Next();
                        }
                    }));
                }));
            }
            list.Add(S(name + ".flush", "table-format", PathKind.Cold, $"{FlushEntries} Put (no WAL) + Flush into a {tf} SST", ctx =>
            {
                ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
                o.WriteBufferSize = 64UL << 20;
                o.DisableAutoCompactions = true;
                o.TableFactory = tf;
                if (tf == TableFactoryType.PlainTable) o.PrefixExtractor = SliceTransform.CreateFixedPrefix(8);
                DbOptions d = ScenarioContext.DefaultDbOptions();
                d.AllowMmapReads = true;
                RocksDatabase db = ctx.OpenDb("db", d, o);
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
            }, chunk: 1, notes: "Includes the Puts; per flush."));
        }

        // ---------------------------------------------------------------------------------------------- blobs

        (RocksDatabase Db, byte[][] Keys) BlobDb(ScenarioContext ctx, bool cache)
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.EnableBlobFiles = true;
            o.MinBlobSize = 512;
            o.DisableAutoCompactions = true;
            if (cache) o.BlobCache = Cache.BlockCache.CreateLru(64L << 20);
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(5_000);
            ScenarioContext.Fill(db, keys, ScenarioContext.Value(4096), flush: true, compact: false);
            byte[][] sk = ScenarioContext.Shuffled(keys);
            byte[] buf = new byte[8192];
            foreach (byte[] k in sk) db.TryGet(k, buf, out _);
            return (db, sk);
        }
        list.Add(S("blob.get.blob-cache", "blob", PathKind.Hot, "TryGet(key, Span) of a 4 KiB blob value, blob cache hit", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = BlobDb(ctx, cache: true);
            byte[] buf = new byte[8192];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("blob.get-pooled.blob-cache", "blob", PathKind.Hot, "using PooledValue? = GetPooled(key) of a 4 KiB blob value, blob cache hit", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = BlobDb(ctx, cache: true);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { using PooledValue? v = db.GetPooled(keys[c.Next()]); sum += v?.Length ?? 0; } }));
        }));
        list.Add(S("blob.get.async", "blob", PathKind.Hot, "await TryGetAsync(key, IBufferWriter) of a 4 KiB blob value (no blob cache)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = BlobDb(ctx, cache: false);
            var w = new ArrayBufferWriter<byte>(8192);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { w.ResetWrittenCount(); await db.TryGetAsync(keys[c.Next()], w); }
            }));
        }));
        list.Add(S("blob.get.async.blob-cache", "blob", PathKind.Hot, "await TryGetAsync(key, IBufferWriter) of a 4 KiB blob value, blob cache hit", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = BlobDb(ctx, cache: true);
            var w = new ArrayBufferWriter<byte>(8192);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { w.ResetWrittenCount(); await db.TryGetAsync(keys[c.Next()], w); }
            }));
        }));
        list.Add(S("blob.iter.seek-next10", "blob", PathKind.Hot, "reused Iterator: Seek + 10 Next reading 4 KiB blob values (blob cache)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = BlobDb(ctx, cache: true);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Value.Length; it.Next(); }
                }
            }));
        }));
        list.Add(S("blob.gc-compaction", "blob", PathKind.Cold, "overwrite 2000 of 4000 blob values + Flush + CompactRange with blob GC (age cutoff 1.0)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.EnableBlobFiles = true;
            o.MinBlobSize = 512;
            o.DisableAutoCompactions = true;
            o.EnableBlobGarbageCollection = true;
            o.BlobGarbageCollectionAgeCutoff = 1.0;
            o.WriteBufferSize = 64UL << 20;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(4_000);
            byte[] big = ScenarioContext.Value(4096);
            ScenarioContext.Fill(db, keys, big, flush: true, compact: true);
            var wo = new WriteOptions { DisableWal = true };
            int round = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    int start = (round++ & 1) * 2000;
                    for (int k = 0; k < 2000; k++) db.Put(wo, keys[start + k], big);
                    db.Flush();
                    db.CompactRange();
                }
            }));
        }, chunk: 1, notes: "Includes the Puts; per compaction (~16 MB of blobs rewritten)."));

        // ---------------------------------------------------------------------------------------------- rate limiter

        list.Add(S("ratelimiter.put", "ratelimiter", PathKind.Hot, "Put(key, value), WAL on, DbOptions.RateLimiter = 1 GB/s (flush / compaction charged)", ctx =>
        {
            DbOptions d = ScenarioContext.DefaultDbOptions();
            d.RateLimiter = ctx.Own(new GenericRateLimiter(1L << 30));
            RocksDatabase db = ctx.OpenDb("db", d);
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(keys[c.Next()], value); }));
        }));
        list.Add(S("ratelimiter.request", "ratelimiter", PathKind.Hot, "GenericRateLimiter.Request(4 KiB, User) granted from the bucket (1 TB/s)", ctx =>
        {
            var rl = ctx.Own(new GenericRateLimiter(1L << 40, 100_000, 10, RateLimiterMode.AllIo, autoTuned: false, 0));
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) rl.Request(4096, IoPriority.User); }));
        }));
        list.Add(S("ratelimiter.request-delayed", "ratelimiter", PathKind.Warm, "GenericRateLimiter.Request(64 KiB, Low) at 64 MB/s, 1 ms refills (most requests wait)", ctx =>
        {
            var rl = ctx.Own(new GenericRateLimiter(64L << 20, 1_000, 10, RateLimiterMode.AllIo, autoTuned: false, 0));
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) rl.Request(64 << 10, IoPriority.Low); }));
        }, chunk: 20));
        list.Add(S("ratelimiter.request-delayed.async", "ratelimiter", PathKind.Warm, "await GenericRateLimiter.RequestAsync(64 KiB, Low) at 64 MB/s, 1 ms refills", ctx =>
        {
            var rl = ctx.Own(new GenericRateLimiter(64L << 20, 1_000, 10, RateLimiterMode.AllIo, autoTuned: false, 0));
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await rl.RequestAsync(64 << 10, IoPriority.Low, CancellationToken.None);
            }));
        }, chunk: 20));
        list.Add(S("ratelimiter.compaction", "ratelimiter", PathKind.Cold, $"4 x ({FlushEntries} Put + Flush) + CompactRange with a 1 GB/s rate limiter", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            DbOptions d = ScenarioContext.DefaultDbOptions();
            d.RateLimiter = ctx.Own(new GenericRateLimiter(1L << 30));
            RocksDatabase db = ctx.OpenDb("db", d, o);
            byte[][] keys = ScenarioContext.Keys(FlushEntries * 4);
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int f = 0; f < 4; f++)
                    {
                        for (int k = 0; k < FlushEntries; k++) db.Put(wo, keys[f * FlushEntries + k], value);
                        db.Flush();
                    }
                    db.CompactRange();
                }
            }));
        }, chunk: 1, notes: "Includes the Puts; per compaction."));

        // ---------------------------------------------------------------------------------------------- tracing

        list.Add(S("trace.put", "trace", PathKind.Hot, "Put(key, value) while StartTrace is active (null writer)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            db.StartTrace(new TraceOptions(), new NullTraceWriter());
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(keys[c.Next()], value); }));
        }));
        list.Add(S("trace.get.sst-cached", "trace", PathKind.Hot, "TryGet(key, Span) while StartTrace is active (null writer)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            db.StartTrace(new TraceOptions { IncludeReadWriteOptions = true }, new NullTraceWriter());
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("trace.get-async.sst-cached", "trace", PathKind.Hot, "await TryGetAsync(key, IBufferWriter) while StartTrace is active", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            db.StartTrace(new TraceOptions(), new NullTraceWriter());
            var w = new ArrayBufferWriter<byte>(256);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { w.ResetWrittenCount(); await db.TryGetAsync(keys[c.Next()], w); }
            }));
        }));
        list.Add(S("trace.iter.seek-next10.sst-cached", "trace", PathKind.Hot, "reused Iterator: Seek + 10 Next while StartTrace is active", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            db.StartTrace(new TraceOptions(), new NullTraceWriter());
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Value.Length; it.Next(); }
                }
            }));
        }));
        list.Add(S("trace.multiget16.sst-cached", "trace", PathKind.Hot, "reused MultiGetBatch of 16 keys while StartTrace is active (per batch)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            db.StartTrace(new TraceOptions(), new NullTraceWriter());
            var batch = ctx.Own(new MultiGetBatch(16));
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    batch.Clear();
                    for (int j = 0; j < 16; j++) batch.Add(keys[c.Next()]);
                    db.MultiGet(batch);
                }
            }));
        }, chunk: 50));
        list.Add(S("trace.blockcache.get.sst-cached", "trace", PathKind.Hot, "TryGet(key, Span) while StartBlockCacheTrace is active (null writer)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            db.StartBlockCacheTrace(new BlockCacheTraceOptions(), new NullTraceWriter());
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("trace.blockcache.get-async.sst-cached", "trace", PathKind.Hot, "await TryGetAsync(key, IBufferWriter) while StartBlockCacheTrace is active", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            db.StartBlockCacheTrace(new BlockCacheTraceOptions(), new NullTraceWriter());
            var w = new ArrayBufferWriter<byte>(256);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { w.ResetWrittenCount(); await db.TryGetAsync(keys[c.Next()], w); }
            }));
        }));
        list.Add(S("trace.io.get.sst-uncached", "trace", PathKind.Hot, "TryGet(key, Span) with block reads while StartIOTrace is active (null writer)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Uncached);
            db.StartIOTrace(new TraceOptions(), new NullTraceWriter());
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("trace.start-end", "trace", PathKind.Cold, "StartTrace(null writer) + EndTrace", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            var to = new TraceOptions();
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    db.StartTrace(to, new NullTraceWriter());
                    db.EndTrace();
                }
            }));
        }, chunk: 100, notes: "Includes the caller's NullTraceWriter (24 B)."));

        // ---------------------------------------------------------------------------------------------- remote compaction

        list.Add(S("remote-compaction", "remote-compaction", PathKind.Cold, $"4 x ({FlushEntries} Put + Flush) + CompactRange run by an in-process CompactionService", ctx =>
        {
            string dir = ctx.NewDirectory("db");
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            DbOptions d = ScenarioContext.DefaultDbOptions();
            d.CompactionService = new InProcessCompactionService(dir);
            RocksDatabase db = ctx.Own(RocksDatabase.Open(dir, d, o));
            byte[][] keys = ScenarioContext.Keys(FlushEntries * 4);
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    for (int f = 0; f < 4; f++)
                    {
                        for (int k = 0; k < FlushEntries; k++) db.Put(wo, keys[f * FlushEntries + k], value);
                        db.Flush();
                    }
                    db.CompactRange();
                }
            }));
        }, chunk: 1, notes: "Includes the Puts and the worker's OpenAndCompact (secondary open of the DB); per compaction."));

        // ---------------------------------------------------------------------------------------------- user-defined timestamps

        (RocksDatabase Db, byte[][] Keys) UdtDb(ScenarioContext ctx, bool flush)
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.Comparator = Comparator.BytewiseWithU64Ts;
            o.DisableAutoCompactions = true;
            o.WriteBufferSize = 256UL << 20;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(ReadKeys);
            byte[] ts = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(ts, 10);
            foreach (byte[] k in keys) db.Put(k, ts, value);
            if (flush) { db.Flush(); db.CompactRange(); }
            byte[][] sk = ScenarioContext.Shuffled(keys);
            return (db, sk);
        }
        static ReadOptions UdtRead()
        {
            byte[] readTs = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(readTs, 20);
            return new ReadOptions { Timestamp = readTs };
        }
        list.Add(S("get.udt.memtable", "udt", PathKind.Hot, "TryGet(ReadOptions{Timestamp}, key, value writer, timestamp writer), memtable", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = UdtDb(ctx, flush: false);
            ReadOptions ro = UdtRead();
            var vw = new ArrayBufferWriter<byte>(256);
            var tw = new ArrayBufferWriter<byte>(16);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++) { vw.ResetWrittenCount(); tw.ResetWrittenCount(); db.TryGet(ro, keys[c.Next()], vw, tw); }
            }));
        }));
        list.Add(S("get.udt-async.sst-cached", "udt", PathKind.Hot, "await TryGetAsync(ReadOptions{Timestamp}, key, value writer, timestamp writer)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = UdtDb(ctx, flush: true);
            ReadOptions ro = UdtRead();
            var vw = new ArrayBufferWriter<byte>(256);
            var tw = new ArrayBufferWriter<byte>(16);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { vw.ResetWrittenCount(); tw.ResetWrittenCount(); await db.TryGetAsync(ro, keys[c.Next()], vw, tw); }
            }));
        }));
        list.Add(S("get.udt-span.sst-cached", "udt", PathKind.Hot, "TryGet(ReadOptions{Timestamp}, key, Span<byte>) (value only)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = UdtDb(ctx, flush: true);
            ReadOptions ro = UdtRead();
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(ro, keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("iter.udt.seek-next10.sst-cached", "udt", PathKind.Hot, "reused Iterator{Timestamp}: Seek + 10 x (Key, Value, Timestamp) Next", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = UdtDb(ctx, flush: true);
            Iterator it = ctx.Own(db.NewIterator(UdtRead()));
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Value.Length + it.Timestamp.Length; it.Next(); }
                }
            }));
        }));
        list.Add(S("udt.get-full-history-ts-low", "udt", PathKind.Warm, "GetFullHistoryTsLow() (set once)", ctx =>
        {
            (RocksDatabase db, _) = UdtDb(ctx, flush: false);
            byte[] low = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(low, 5);
            db.IncreaseFullHistoryTsLow(low);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetFullHistoryTsLow()!.Length; }));
        }));
        list.Add(S("udt.try-get-full-history-ts-low", "udt", PathKind.Warm, "TryGetFullHistoryTsLow(reused IBufferWriter) (set once)", ctx =>
        {
            (RocksDatabase db, _) = UdtDb(ctx, flush: false);
            byte[] low = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(low, 5);
            db.IncreaseFullHistoryTsLow(low);
            var w = new ArrayBufferWriter<byte>(16);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { w.ResetWrittenCount(); db.TryGetFullHistoryTsLow(w); } }));
        }));
        list.Add(S("udt.delete", "udt", PathKind.Hot, "Delete(key, timestamp) with Comparator.BytewiseWithU64Ts", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.Comparator = Comparator.BytewiseWithU64Ts;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            byte[] ts = new byte[8];
            ulong t = 1;
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(ts, t++);
                    db.Delete(keys[c.Next()], ts);
                }
            }));
        }));

        // ---------------------------------------------------------------------------------------------- merge operands / user merge operators

        (RocksDatabase Db, byte[][] Keys) MergeDb(ScenarioContext ctx, MergeOperator op, bool flush, long blockCache = 64L << 20)
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions(blockCache);
            o.MergeOperator = op;
            o.WriteBufferSize = 64UL << 20;
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(20_000);
            byte[] v8 = new byte[8];
            var wo = new WriteOptions { DisableWal = true };
            for (int j = 0; j < 4; j++)
            {
                foreach (byte[] k in keys) { v8[0] = (byte)(j + 1); db.Merge(wo, k, v8); }
                // One operand per file when flushing: the reads merge across 4 SSTs.
                if (flush) db.Flush();
            }
            byte[][] sk = ScenarioContext.Shuffled(keys);
            var w = new ArrayBufferWriter<byte>(64);
            foreach (byte[] k in sk) { w.ResetWrittenCount(); db.TryGet(k, w); }
            return (db, sk);
        }
        list.Add(S("get.merge.user-bufferwriter.sst-cached", "merge", PathKind.Hot, "TryGet(key, IBufferWriter) of a key with 4 user-operator operands in 4 SSTs", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = MergeDb(ctx, new Int64MaxOperator(), flush: true);
            var w = new ArrayBufferWriter<byte>(64);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { w.ResetWrittenCount(); db.TryGet(keys[c.Next()], w); } }));
        }));
        list.Add(S("get.merge.user-async.sst-cached", "merge", PathKind.Hot, "await TryGetAsync(key, IBufferWriter), 4 user-operator operands in 4 SSTs", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = MergeDb(ctx, new Int64MaxOperator(), flush: true);
            var w = new ArrayBufferWriter<byte>(64);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { w.ResetWrittenCount(); await db.TryGetAsync(keys[c.Next()], w); }
            }));
        }));
        list.Add(S("getmergeoperands.sst-cached", "merge", PathKind.Hot, "TryGetMergeOperands(key, options, reused MergeOperandsResult), 4 operands in 4 SSTs", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = MergeDb(ctx, MergeOperator.UInt64Add, flush: true);
            var mo = new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 16 };
            var result = ctx.Own(new MergeOperandsResult());
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.DefaultColumnFamily.TryGetMergeOperands(keys[c.Next()], mo, result); }));
        }));
        list.Add(S("getmergeoperands-async.sst-cached", "merge", PathKind.Hot, "await TryGetMergeOperandsAsync(null, key, options, reused result), 4 operands in 4 SSTs", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = MergeDb(ctx, MergeOperator.UInt64Add, flush: true);
            var mo = new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 16 };
            var result = ctx.Own(new MergeOperandsResult());
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.DefaultColumnFamily.TryGetMergeOperandsAsync(null, keys[c.Next()], mo, result);
            }));
        }));
        list.Add(S("getmergeoperands-async.sst-uncached", "merge", PathKind.Hot, "await TryGetMergeOperandsAsync, 4 operands in 4 SSTs, 256 KiB block cache (block reads)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = MergeDb(ctx, MergeOperator.UInt64Add, flush: true, blockCache: 256L << 10);
            var mo = new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 16 };
            var result = ctx.Own(new MergeOperandsResult());
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.DefaultColumnFamily.TryGetMergeOperandsAsync(null, keys[c.Next()], mo, result);
            }));
        }));
        list.Add(S("iter.merge.user.sst-cached", "merge", PathKind.Hot, "reused Iterator: Seek + 10 Next over keys with 4 user-operator operands in 4 SSTs", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = MergeDb(ctx, new Int64MaxOperator(), flush: true);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++) { sum += it.Value.Length; it.Next(); }
                }
            }));
        }));
    }
}
