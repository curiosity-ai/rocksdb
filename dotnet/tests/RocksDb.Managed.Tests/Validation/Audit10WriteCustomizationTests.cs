using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #10, user customization points on the write, flush and compaction paths: merge operators
/// (include/rocksdb/merge_operator.h, db/merge_helper.cc), compaction filters and their factories
/// (include/rocksdb/compaction_filter.h, CompactionIterator::InvokeFilterIfNeeded, BuildTable), table properties
/// collectors (include/rocksdb/table_properties.h, table/meta_blocks.cc), SST partitioners, event listeners, file
/// checksum generators and WAL filters. Upstream's contract forbids exceptions in these callbacks ("Exceptions MUST NOT
/// propagate out of overridden functions into RocksDB", compaction_filter.h); the port's contract for a callback that
/// throws is pinned here: a background job (flush / compaction) fails like an I/O error of that job (background
/// error, no orphan output, data intact after a reopen), a foreground call gets the exception and the DB stays usable.
/// </summary>
[TestClass]
public sealed class Audit10WriteCustomizationTests
{
    private const string Db = "/audit10wc";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    // ---- shared helpers ---------------------------------------------------------------------------------------------

    /// <summary>Switches a callback between correct and throwing behaviour.</summary>
    private sealed class Switch
    {
        public volatile bool On;
        public int Calls;
    }

    private sealed class UserCallbackException(string message) : Exception(message);

    private static void Boom(Switch s, string where)
    {
        Interlocked.Increment(ref s.Calls);
        if (s.On) throw new UserCallbackException("user callback threw in " + where);
    }

    private static MemoryFileSystem NewDbFs()
    {
        var fs = new MemoryFileSystem();
        fs.CreateDirectoryIfMissing(Db);
        fs.SyncDirectory("/");
        return fs;
    }

    private static SortedSet<string> SstOnDisk(IFileSystem fs)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string f in fs.GetChildren(Db))
        {
            string name = Path.GetFileName(f);
            if (name.EndsWith(".sst", StringComparison.Ordinal)) set.Add(name);
        }
        return set;
    }

    private static SortedSet<string> LiveSst(RocksDatabase db)
    {
        var set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (LiveFileMetadata m in db.GetLiveFilesMetadata()) set.Add(Path.GetFileName(m.Name));
        return set;
    }

    private static void DisposeWithin(RocksDatabase db)
    {
        Assert.IsTrue(Task.Run(db.Dispose).Wait(Timeout), "Dispose hung");
    }

    /// <summary>Records background errors so a test can wait for one.</summary>
    private sealed class BgErrorListener : EventListener
    {
        public readonly ManualResetEventSlim Seen = new();
        public volatile RocksDbException? Error;
        public BackgroundErrorReason Reason;

        public override void OnBackgroundError(BackgroundErrorEventArgs e)
        {
            Reason = e.Reason;
            Error = e.Error;
            Seen.Set();
        }
    }

    /// <summary>StringAppend (',') that throws for keys starting with "bad" while its switch is on.</summary>
    private sealed class ThrowingAppend(Switch s) : MergeOperator
    {
        public override string Name => "StringAppendOperator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands,
            IBufferWriter<byte> result)
        {
            if (key.StartsWith("bad"u8)) Boom(s, "FullMerge");
            bool first = true;
            if (hasExistingValue)
            {
                result.Write(existingValue);
                first = false;
            }
            for (int i = 0; i < operands.Count; i++)
            {
                if (!first) result.Write(","u8);
                result.Write(operands[i]);
                first = false;
            }
            return true;
        }

        public override bool PartialMerge(ReadOnlySpan<byte> key, in MergeOperands operands, IBufferWriter<byte> result)
        {
            if (key.StartsWith("bad"u8)) Boom(s, "PartialMerge");
            for (int i = 0; i < operands.Count; i++)
            {
                if (i > 0) result.Write(","u8);
                result.Write(operands[i]);
            }
            return true;
        }
    }

    /// <summary>The model of the standard workload: good keys with values and merges, a "bad" key with a merge chain.</summary>
    private static Dictionary<string, string?> WriteWorkload(RocksDatabase db, int round)
    {
        var model = new Dictionary<string, string?>(StringComparer.Ordinal);
        for (int i = 0; i < 40; i++)
        {
            string k = "good" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
            string v = "v" + round + "-" + i;
            db.Put(B(k), B(v));
            if (i % 3 == 0)
            {
                db.Merge(B(k), B("m" + round));
                v += ",m" + round;
            }
            model[k] = v;
        }
        db.Put(B("bad"), B("base" + round));
        db.Merge(B("bad"), B("a" + round));
        db.Merge(B("bad"), B("b" + round));
        model["bad"] = "base" + round + ",a" + round + ",b" + round;
        return model;
    }

    private static void AssertModel(RocksDatabase db, Dictionary<string, string?> model, string phase)
    {
        foreach (KeyValuePair<string, string?> kv in model)
        {
            Assert.AreEqual(kv.Value, GetString(db, kv.Key), phase + ": " + kv.Key);
        }
        int n = 0;
        using (Iterator it = db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                string k = System.Text.Encoding.UTF8.GetString(it.Key);
                Assert.IsTrue(model.TryGetValue(k, out string? v), phase + ": unexpected " + k);
                Assert.AreEqual(v, System.Text.Encoding.UTF8.GetString(it.Value), phase + ": iterator " + k);
                n++;
            }
        }
        Assert.AreEqual(model.Count, n, phase + ": key count");
    }

    /// <summary>
    /// After a background job failed because a user callback threw: the error is an IOError carrying the user exception,
    /// no table file outside the live set is left, the data reads as before (callbacks disabled), the DB closes, and a
    /// reopen with the callbacks disabled reads the same data and can flush and compact.
    /// </summary>
    private static void AssertBackgroundFailureRecoverable(MemoryFileSystem fs, RocksDatabase db, Exception? failure, Switch s,
        Dictionary<string, string?> model, Func<DbOptions> dbOptions, Func<ColumnFamilyOptions> cfOptions, string phase)
    {
        Assert.IsNotNull(failure, phase + ": the job did not fail");
        RocksDbException r = failure as RocksDbException ?? RocksDbException.FromException(failure);
        Assert.AreEqual(StatusCode.IOError, r.Code, phase + ": " + failure);
        Assert.IsInstanceOfType<UserCallbackException>(FindUserException(failure), phase + ": the user exception is not carried: " + failure);
        CollectionAssert.AreEqual(LiveSst(db).ToList(), SstOnDisk(fs).ToList(), phase + ": orphan table files");
        s.On = false;
        AssertModel(db, model, phase + " (after failure)");
        DisposeWithin(db);
        using RocksDatabase reopened = RocksDatabase.Open(Db, dbOptions(), cfOptions());
        AssertModel(reopened, model, phase + " (reopened)");
        reopened.Flush();
        reopened.CompactRange();
        AssertModel(reopened, model, phase + " (reopened, compacted)");
        CollectionAssert.AreEqual(LiveSst(reopened).ToList(), SstOnDisk(fs).ToList(), phase + ": orphan table files after reopen");
    }

    private static Exception? FindUserException(Exception? e)
    {
        for (Exception? x = e; x is not null; x = x.InnerException)
        {
            if (x is UserCallbackException) return x;
            if (x is AggregateException a)
            {
                foreach (Exception inner in a.InnerExceptions)
                {
                    if (FindUserException(inner) is { } found) return found;
                }
            }
        }
        return null;
    }

    private static Exception? Capture(Action a)
    {
        try
        {
            a();
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    // ---- merge operator that throws ---------------------------------------------------------------------------------

    /// <summary>
    /// Foreground merges (GetContext::SaveValue / TimedFullMerge in DBImpl::GetImpl, MultiGet, DBIter::MergeValuesNewToOld
    /// and the reverse merge of DBIter::FindValueForCurrentKey): a merge operator that throws surfaces the exception to the
    /// caller of Get / GetAsync / MultiGet / the iterator; other keys read correctly and the DB stays usable (writes,
    /// flush, close). Both with the chain in the memtable and in table files.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task ThrowingMergeOnReadsSurfacesToTheCallerAndLeavesTheDbUsable(bool flushed)
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        var cfo = new ColumnFamilyOptions { MergeOperator = new ThrowingAppend(s) };
        RocksDatabase db = RocksDatabase.Open(Db, NewOptions(fs), cfo);
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        if (flushed)
        {
            // Base value and operands in different files: the flush cannot merge them, every read must.
            db.Flush();
            db.Merge(B("bad"), B("c"));
            db.Flush();
            model["bad"] += ",c";
        }
        s.On = true;
        for (int round = 0; round < 3; round++)
        {
            Assert.IsInstanceOfType<UserCallbackException>(Capture(() => db.Get(B("bad"))), "Get");
            Assert.IsInstanceOfType<UserCallbackException>(Capture(() => db.GetAsync(B("bad")).AsTask().GetAwaiter().GetResult()), "GetAsync");
            Assert.IsInstanceOfType<UserCallbackException>(Capture(() => db.MultiGet([B("good000"), B("bad"), B("good003")])), "MultiGet");
            Assert.AreEqual(model["good003"], GetString(db, "good003"));
            Assert.IsInstanceOfType<UserCallbackException>(Capture(() =>
            {
                using Iterator it = db.NewIterator();
                for (it.SeekToFirst(); it.Valid; it.Next()) { }
            }), "forward iteration");
            Assert.IsInstanceOfType<UserCallbackException>(Capture(() =>
            {
                using Iterator it = db.NewIterator();
                for (it.SeekToLast(); it.Valid; it.Prev()) { }
            }), "backward iteration");
            Assert.IsInstanceOfType<UserCallbackException>(Capture(() =>
            {
                using Iterator it = db.NewIterator();
                it.Seek(B("bad"));
            }), "seek");
            // An iterator that stepped on the failing key can still be repositioned and read the other keys.
            using (Iterator it = db.NewIterator())
            {
                Assert.IsInstanceOfType<UserCallbackException>(Capture(() => it.Seek(B("bad"))));
                it.Seek(B("good005"));
                Assert.IsTrue(it.Valid);
                Assert.AreEqual("good005", S(it.Key.ToArray()));
            }
            Assert.AreEqual(model["good006"], S(await db.GetAsync(B("good006"))));
            db.Put(B("good100"), B("x"));
            model["good100"] = "x";
        }
        s.On = false;
        AssertModel(db, model, "after throwing reads");
        db.Flush();
        db.CompactRange();
        AssertModel(db, model, "after compaction");
        DisposeWithin(db);
    }

    /// <summary>
    /// A merge operator that throws while a flush merges a chain (BuildTable reads the memtable through a
    /// CompactionIterator, MergeHelper::MergeUntil): the flush fails like a table-file I/O error
    /// (BackgroundErrorReason.FlushNoWal), its output is deleted, the memtable is kept, and a reopen with a working
    /// operator replays the WAL and reads everything.
    /// </summary>
    [TestMethod]
    public void ThrowingMergeDuringFlushIsABackgroundIoErrorAndRecoverable()
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        var listener = new BgErrorListener();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.Listeners.Add(listener);
            return o;
        }
        ColumnFamilyOptions Cfo() => new() { MergeOperator = new ThrowingAppend(s) };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        s.On = true;
        Exception? failure = Capture(db.Flush);
        Assert.IsTrue(listener.Seen.IsSet, "no background error");
        Assert.AreEqual(BackgroundErrorReason.FlushNoWal, listener.Reason);
        Assert.IsTrue(s.Calls > 0);
        AssertBackgroundFailureRecoverable(fs, db, failure, s, model, Dbo, Cfo, "flush");
    }

    /// <summary>
    /// A merge operator that throws in a compaction (MergeHelper::MergeUntil, FullMerge and PartialMergeMulti): manual
    /// and automatic compactions fail like an I/O error of the job (kCompaction), the outputs are deleted
    /// (CompactionJob::CleanupCompaction), the inputs stay live, nothing is lost.
    /// </summary>
    [TestMethod]
    [DataRow(false, false)]
    [DataRow(false, true)]
    [DataRow(true, false)]
    public void ThrowingMergeDuringCompactionIsABackgroundIoErrorAndRecoverable(bool automatic, bool partialOnly)
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        var listener = new BgErrorListener();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.Listeners.Add(listener);
            return o;
        }
        ColumnFamilyOptions Cfo() => new()
        {
            MergeOperator = new ThrowingAppend(s),
            Level0FileNumCompactionTrigger = partialOnly ? 2 : automatic ? 3 : 100,
            LevelCompactionDynamicLevelBytes = !partialOnly,
        };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model;
        if (partialOnly)
        {
            // The base value in L6, operands in two L0 files: the automatic L0 -> L1 compaction is not bottommost and
            // has no base value, so MergeUntil calls PartialMergeMulti.
            model = new Dictionary<string, string?>(StringComparer.Ordinal);
            db.Put(B("bad"), B("base"));
            db.Flush();
            db.CompactRange(new CompactRangeOptions { ChangeLevel = true, TargetLevel = 6 }, null, null);
            // "zz" makes the two L0 files overlap (by internal key, VersionStorageInfo::GenerateLevel0NonOverlapping):
            // otherwise they would be moved down trivially.
            db.Merge(B("bad"), B("x"));
            db.Put(B("zz"), B("1"));
            db.Flush();
            db.Merge(B("bad"), B("y"));
            db.Put(B("zz"), B("2"));
            model["bad"] = "base,x,y";
            model["zz"] = "2";
            s.On = true;
            db.Flush();
            Assert.IsTrue(listener.Seen.Wait(Timeout), "the automatic compaction did not fail: calls " + s.Calls + " "
                + db.GetProperty("rocksdb.levelstats"));
            Assert.IsTrue(s.Calls > 0, "the merge operator was not called");
            AssertBackgroundFailureRecoverable(fs, db, listener.Error, s, model, Dbo, Cfo, "partial merge");
            return;
        }
        model = WriteWorkload(db, 1);
        db.Flush();
        // One operand per later file (no merge at flush), the base value in the first: the compaction merges. "zz"
        // makes the files overlap (by internal key), so they are not moved trivially.
        db.Merge(B("bad"), B("c"));
        db.Put(B("zz"), B("1"));
        db.Flush();
        db.Merge(B("bad"), B("d"));
        db.Put(B("zz"), B("2"));
        model["bad"] += ",c,d";
        model["zz"] = "2";
        s.On = true;
        int callsBefore = s.Calls;
        db.Flush();
        Assert.AreEqual(callsBefore, s.Calls, "the flush merged");
        Exception? failure;
        if (automatic)
        {
            Assert.IsTrue(listener.Seen.Wait(Timeout), "the automatic compaction did not fail");
            failure = listener.Error;
            Assert.AreEqual(BackgroundErrorReason.Compaction, listener.Reason);
        }
        else
        {
            failure = Capture(db.CompactRange);
            Assert.IsTrue(listener.Seen.IsSet, "no background error for " + failure);
        }
        Assert.IsTrue(s.Calls > 0);
        AssertBackgroundFailureRecoverable(fs, db, failure, s, model, Dbo, Cfo, automatic ? "automatic compaction" : "manual compaction");
    }

    // ---- compaction filter that throws ------------------------------------------------------------------------------

    private sealed class ThrowingFilter(Switch s) : CompactionFilter
    {
        public override string Name => "ThrowingFilter";

        public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
            ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil)
        {
            if (key.StartsWith("bad"u8)) Boom(s, "Filter");
            return CompactionFilterDecision.Keep;
        }
    }

    private sealed class ThrowingFilterFactory(Switch s, bool throwOnCreate, bool flushes) : CompactionFilterFactory
    {
        public override string Name => "ThrowingFilterFactory";

        public override CompactionFilter? CreateCompactionFilter(in CompactionFilterContext context)
        {
            if (throwOnCreate) Boom(s, "CreateCompactionFilter");
            return new ThrowingFilter(s);
        }

        public override bool ShouldFilterTableFileCreation(bool isFlush) => flushes || !isFlush;
    }

    public enum FilterKind
    {
        Filter,
        FactoryFilter,
        FactoryCreate,
    }

    /// <summary>
    /// A compaction filter (the plain option or a factory's), or the factory's CreateCompactionFilter, that throws
    /// (CompactionIterator::InvokeFilterIfNeeded, MergeHelper::FilterMerge, CompactionJob::SetupAndValidateCompactionFilter):
    /// the compaction fails like an I/O error of the job and the DB is recoverable.
    /// </summary>
    [TestMethod]
    [DataRow(FilterKind.Filter)]
    [DataRow(FilterKind.FactoryFilter)]
    [DataRow(FilterKind.FactoryCreate)]
    public void ThrowingCompactionFilterFailsTheCompactionRecoverably(FilterKind kind)
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        var listener = new BgErrorListener();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.Listeners.Add(listener);
            return o;
        }
        ColumnFamilyOptions Cfo()
        {
            var o = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator(), Level0FileNumCompactionTrigger = 100 };
            if (kind == FilterKind.Filter) o.CompactionFilter = new ThrowingFilter(s);
            else o.CompactionFilterFactory = new ThrowingFilterFactory(s, kind == FilterKind.FactoryCreate, flushes: false);
            return o;
        }
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        db.Flush();
        Dictionary<string, string?> more = WriteWorkload(db, 2);
        foreach (KeyValuePair<string, string?> kv in more) model[kv.Key] = kv.Value;
        db.Flush();
        s.On = true;
        Exception? failure = Capture(db.CompactRange);
        Assert.IsTrue(s.Calls > 0);
        Assert.AreEqual(BackgroundErrorReason.Compaction, listener.Reason);
        AssertBackgroundFailureRecoverable(fs, db, failure, s, model, Dbo, Cfo, kind.ToString());
    }

    /// <summary>
    /// A flush filter (ShouldFilterTableFileCreation(kFlush), BuildTable) that throws, or whose factory throws: the
    /// flush fails like a table-file I/O error and the DB is recoverable.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ThrowingFlushFilterFailsTheFlushRecoverably(bool throwOnCreate)
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        DbOptions Dbo() => NewOptions(fs);
        ColumnFamilyOptions Cfo() => new()
        {
            MergeOperator = new StringAppendMergeOperator(),
            CompactionFilterFactory = new ThrowingFilterFactory(s, throwOnCreate, flushes: true),
        };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        s.On = true;
        Exception? failure = Capture(db.Flush);
        Assert.IsTrue(s.Calls > 0);
        AssertBackgroundFailureRecoverable(fs, db, failure, s, model, Dbo, Cfo, "flush filter");
    }

    // ---- table properties collectors --------------------------------------------------------------------------------

    public enum CollectorThrow
    {
        Create,
        AddUserKey,
        BlockAdd,
        Finish,
        NeedCompact,
        Readable,
    }

    private sealed class ThrowingCollector(Switch s, CollectorThrow where) : TablePropertiesCollector
    {
        private int _count;

        public override string Name => "ThrowingCollector";

        public override void AddUserKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TableEntryType type, ulong sequence, ulong fileSize)
        {
            _count++;
            if (where == CollectorThrow.AddUserKey) Boom(s, "AddUserKey");
        }

        public override void BlockAdd(ulong blockUncompressedBytes, ulong blockCompressedBytesFast, ulong blockCompressedBytesSlow)
        {
            if (where == CollectorThrow.BlockAdd) Boom(s, "BlockAdd");
        }

        public override void Finish(IDictionary<string, byte[]> properties)
        {
            properties["audit10.count"] = BitConverter.GetBytes(_count);
            if (where == CollectorThrow.Finish) Boom(s, "Finish");
        }

        public override IReadOnlyDictionary<string, string> GetReadableProperties()
        {
            if (where == CollectorThrow.Readable) Boom(s, "GetReadableProperties");
            return new Dictionary<string, string> { ["audit10.count"] = _count.ToString(System.Globalization.CultureInfo.InvariantCulture) };
        }

        public override bool NeedCompact
        {
            get
            {
                if (where == CollectorThrow.NeedCompact) Boom(s, "NeedCompact");
                return false;
            }
        }
    }

    private sealed class ThrowingCollectorFactory(Switch s, CollectorThrow where) : TablePropertiesCollectorFactory
    {
        public override string Name => "ThrowingCollectorFactory";

        public override TablePropertiesCollector? CreateTablePropertiesCollector(TablePropertiesCollectorContext context)
        {
            if (where == CollectorThrow.Create) Boom(s, "CreateTablePropertiesCollector");
            return new ThrowingCollector(s, where);
        }
    }

    /// <summary>
    /// Table properties collectors that throw. Upstream (table/meta_blocks.cc NotifyCollectTableCollectorsOnAdd /
    /// OnBlockAdd / OnFinish) logs a non-OK Status of Add / BlockAdd / Finish and goes on; a failed Finish contributes no
    /// properties ("if (s.ok()) ... user_collected_properties.merge(user_properties)"). The port maps an exception to
    /// that non-OK Status: the flush and the compaction succeed, the internal properties are intact, and the user
    /// properties of a collector whose Finish threw are absent, including those it added before throwing. Exceptions
    /// outside that contract (the factory, NeedCompact, GetReadableProperties) must not lose data either.
    /// </summary>
    [TestMethod]
    [DataRow(CollectorThrow.Create)]
    [DataRow(CollectorThrow.AddUserKey)]
    [DataRow(CollectorThrow.BlockAdd)]
    [DataRow(CollectorThrow.Finish)]
    [DataRow(CollectorThrow.NeedCompact)]
    public void ThrowingTablePropertiesCollector(CollectorThrow where)
    {
        var s = new Switch { On = true };
        MemoryFileSystem fs = NewDbFs();
        DbOptions Dbo() => NewOptions(fs);
        ColumnFamilyOptions Cfo()
        {
            var o = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator(), Level0FileNumCompactionTrigger = 100 };
            o.TablePropertiesCollectorFactories.Add(new ThrowingCollectorFactory(s, where));
            return o;
        }
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        Exception? flush = Capture(db.Flush);
        bool contract = where is CollectorThrow.AddUserKey or CollectorThrow.BlockAdd or CollectorThrow.Finish;
        if (!contract)
        {
            // Outside upstream's Status contract (the factory, NeedCompact): the flush fails like an I/O error of its
            // table file.
            Assert.IsTrue(s.Calls > 0, "the collector never threw");
            AssertBackgroundFailureRecoverable(fs, db, flush, s, model, Dbo, Cfo, where.ToString());
            return;
        }
        Dictionary<string, string?> more = WriteWorkload(db, 2);
        foreach (KeyValuePair<string, string?> kv in more) model[kv.Key] = kv.Value;
        Exception? flush2 = Capture(db.Flush);
        Exception? compact = Capture(db.CompactRange);
        Assert.IsTrue(s.Calls > 0, "the collector never threw");
        {
            Assert.IsNull(flush, where + ": flush " + flush);
            Assert.IsNull(flush2, where + ": flush " + flush2);
            Assert.IsNull(compact, where + ": compaction " + compact);
            foreach (KeyValuePair<string, RocksDb.Managed.Table.TableProperties> p in db.GetPropertiesOfAllTables())
            {
                Assert.AreNotEqual(0UL, p.Value.NumEntries, p.Key);
                bool has = p.Value.UserCollectedProperties.ContainsKey("audit10.count");
                Assert.AreEqual(where != CollectorThrow.Finish, has, where + ": " + p.Key);
            }
            AssertModel(db, model, where.ToString());
            CollectionAssert.AreEqual(LiveSst(db).ToList(), SstOnDisk(fs).ToList());
            DisposeWithin(db);
            using RocksDatabase reopened = RocksDatabase.Open(Db, Dbo(), Cfo());
            AssertModel(reopened, model, where + " reopened");
        }
    }

    // ---- SST partitioner --------------------------------------------------------------------------------------------

    public enum PartitionerThrow
    {
        Create,
        ShouldPartition,
        CanDoTrivialMove,
    }

    private sealed class ThrowingPartitioner(Switch s, PartitionerThrow where) : SstPartitioner
    {
        public override string Name => "ThrowingPartitioner";

        public override PartitionerResult ShouldPartition(ReadOnlySpan<byte> prevUserKey, ReadOnlySpan<byte> currentUserKey, ulong currentOutputFileSize)
        {
            if (where == PartitionerThrow.ShouldPartition) Boom(s, "ShouldPartition");
            return PartitionerResult.NotRequired;
        }

        public override bool CanDoTrivialMove(ReadOnlySpan<byte> smallestUserKey, ReadOnlySpan<byte> largestUserKey)
        {
            if (where == PartitionerThrow.CanDoTrivialMove) Boom(s, "CanDoTrivialMove");
            return true;
        }
    }

    private sealed class ThrowingPartitionerFactory(Switch s, PartitionerThrow where) : SstPartitionerFactory
    {
        public override string Name => "ThrowingPartitionerFactory";

        public override SstPartitioner? CreatePartitioner(in SstPartitionerContext context)
        {
            if (where == PartitionerThrow.Create) Boom(s, "CreatePartitioner");
            return new ThrowingPartitioner(s, where);
        }
    }

    /// <summary>
    /// An SST partitioner (CompactionOutputs::ShouldStopBefore, Compaction::IsTrivialMove / CreateSstPartitioner) that
    /// throws: the compaction (manual or automatic, trivial-move candidate or not) fails like an I/O error of the job,
    /// the input files are released (a later compaction with a working partitioner runs), and the DB is recoverable.
    /// </summary>
    [TestMethod]
    [DataRow(PartitionerThrow.Create, false)]
    [DataRow(PartitionerThrow.ShouldPartition, false)]
    [DataRow(PartitionerThrow.Create, true)]
    [DataRow(PartitionerThrow.CanDoTrivialMove, true)]
    public void ThrowingSstPartitionerFailsTheCompactionRecoverably(PartitionerThrow where, bool automatic)
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        var listener = new BgErrorListener();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.Listeners.Add(listener);
            return o;
        }
        bool trivial = where == PartitionerThrow.CanDoTrivialMove;
        ColumnFamilyOptions Cfo() => new()
        {
            MergeOperator = new StringAppendMergeOperator(),
            // A single L0 file is moved trivially to L1 (not the last level, so the partitioner is consulted).
            Level0FileNumCompactionTrigger = !automatic ? 100 : trivial ? 1 : 2,
            LevelCompactionDynamicLevelBytes = !trivial,
            SstPartitionerFactory = new ThrowingPartitionerFactory(s, where),
        };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        if (trivial)
        {
            s.On = true;
            // The compaction scheduled when the flush is installed may fail before the flush's wait returns, which then
            // reports the background error (WaitForFlushMemTables).
            _ = Capture(db.Flush);
        }
        else
        {
            db.Flush();
            Dictionary<string, string?> more = WriteWorkload(db, 2);
            foreach (KeyValuePair<string, string?> kv in more) model[kv.Key] = kv.Value;
            s.On = true;
            db.Flush();
        }
        Exception? failure;
        if (automatic)
        {
            Assert.IsTrue(listener.Seen.Wait(Timeout), "the automatic compaction did not fail");
            failure = listener.Error;
        }
        else
        {
            failure = Capture(db.CompactRange);
        }
        Assert.IsTrue(s.Calls > 0, "the partitioner was not called");
        AssertBackgroundFailureRecoverable(fs, db, failure, s, model, Dbo, Cfo, where + (automatic ? " automatic" : " manual"));
    }

    /// <summary>
    /// Compaction::IsTrivialMove consults the partitioner's CanDoTrivialMove from the compaction scheduler, under the DB
    /// mutex (DBImpl::EnoughRoomForCompaction with the default SstFileManager), which runs when a flush installs its
    /// super version. A CanDoTrivialMove that throws must fail only the compaction it was asked for: with
    /// paranoid_checks off (a compaction IOError is then kNoError, DefaultErrorSeverityMap) the flush that scheduled it
    /// succeeds, the DB keeps working, and once the partitioner behaves the files move and everything reads back.
    /// Before the fix the exception escaped from InstallSuperVersion inside the flush's MANIFEST commit, failing a
    /// flush whose edit was already applied.
    /// </summary>
    [TestMethod]
    public void ThrowingCanDoTrivialMoveFailsOnlyTheCompaction()
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.ParanoidChecks = false;
            return o;
        }
        ColumnFamilyOptions Cfo() => new()
        {
            MergeOperator = new StringAppendMergeOperator(),
            Level0FileNumCompactionTrigger = 1,
            LevelCompactionDynamicLevelBytes = false,
            SstPartitionerFactory = new ThrowingPartitionerFactory(s, PartitionerThrow.CanDoTrivialMove),
        };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        var model = new Dictionary<string, string?>(StringComparer.Ordinal);
        s.On = true;
        for (int r = 0; r < 3; r++)
        {
            foreach (KeyValuePair<string, string?> kv in WriteWorkload(db, r)) model[kv.Key] = kv.Value;
            db.Flush();
            AssertModel(db, model, "round " + r);
        }
        Assert.IsTrue(s.Calls > 0, "the partitioner was not called");
        s.On = false;
        db.Flush();
        db.CompactRange();
        AssertModel(db, model, "compacted");
        CollectionAssert.AreEqual(LiveSst(db).ToList(), SstOnDisk(fs).ToList(), "orphan table files");
        DisposeWithin(db);
        using RocksDatabase reopened = RocksDatabase.Open(Db, Dbo(), Cfo());
        AssertModel(reopened, model, "reopened");
    }

    // ---- event listeners --------------------------------------------------------------------------------------------

    private sealed class ThrowingListener(Switch s) : EventListener
    {
        public override void OnFlushBegin(RocksDatabase? db, FlushJobInfo info) => Boom(s, "OnFlushBegin");
        public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info) => Boom(s, "OnFlushCompleted");
        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info) => Boom(s, "OnCompactionBegin");
        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) => Boom(s, "OnCompactionCompleted");
        public override void OnTableFileCreated(TableFileCreationInfo info) => Boom(s, "OnTableFileCreated");
        public override void OnTableFileDeleted(TableFileDeletionInfo info) => Boom(s, "OnTableFileDeleted");
        public override void OnMemTableSealed(MemTableInfo info) => Boom(s, "OnMemTableSealed");
        public override void OnStallConditionsChanged(WriteStallInfo info) => Boom(s, "OnStallConditionsChanged");
        public override void OnBackgroundError(BackgroundErrorEventArgs e) => Boom(s, "OnBackgroundError");
        public override void OnExternalFileIngested(RocksDatabase? db, ExternalFileIngestionInfo info) => Boom(s, "OnExternalFileIngested");
        public override void OnColumnFamilyHandleDeletionStarted(ColumnFamily handle) => Boom(s, "OnColumnFamilyHandleDeletionStarted");
        public override void OnDBShutdownBegin(RocksDatabase? db) => Boom(s, "OnDBShutdownBegin");
    }

    /// <summary>
    /// Every listener callback throwing: the port catches and logs listener exceptions (EventListener.cs), so flushes,
    /// compactions (with file deletions), memtable switches, stalls, ingestion, column family drop and close all
    /// complete and the data is intact.
    /// </summary>
    [TestMethod]
    public void ThrowingListenerDoesNotFailTheOperations()
    {
        var s = new Switch { On = true };
        MemoryFileSystem fs = NewDbFs();
        DbOptions dbo = NewOptions(fs);
        dbo.Listeners.Add(new ThrowingListener(s));
        var cfo = new ColumnFamilyOptions
        {
            MergeOperator = new StringAppendMergeOperator(),
            Level0FileNumCompactionTrigger = 2,
            Level0SlowdownWritesTrigger = 3,
            WriteBufferSize = 8 << 10,
        };
        RocksDatabase db = RocksDatabase.Open(Db, dbo, cfo);
        var model = new Dictionary<string, string?>(StringComparer.Ordinal);
        for (int r = 0; r < 4; r++)
        {
            foreach (KeyValuePair<string, string?> kv in WriteWorkload(db, r)) model[kv.Key] = kv.Value;
            db.Flush();
        }
        db.CompactRange();
        ColumnFamily cf = db.CreateColumnFamily("x", new ColumnFamilyOptions());
        cf.Put(B("k"), B("v"));
        db.DropColumnFamily(cf);
        // Ingestion.
        string sst = Db + "/ingest.sst.tmp";
        using (var w = new SstFileWriter(new ColumnFamilyOptions(), fs))
        {
            w.Open(sst);
            w.Put(B("zzz"), B("ingested"));
            w.Finish();
        }
        db.IngestExternalFiles([sst], new IngestExternalFileOptions());
        model["zzz"] = "ingested";
        Assert.IsTrue(s.Calls > 5, "listeners not called: " + s.Calls);
        AssertModel(db, model, "listener");
        DisposeWithin(db);
        dbo = NewOptions(fs);
        using RocksDatabase reopened = RocksDatabase.Open(Db, dbo, cfo);
        AssertModel(reopened, model, "listener reopened");
    }

    /// <summary>
    /// A listener's OnBackgroundError that throws while a real background error is reported must not lose the error
    /// (ErrorHandler::SetBGError): the DB still reports it and recovers like without the listener.
    /// </summary>
    [TestMethod]
    public void ThrowingOnBackgroundErrorKeepsTheError()
    {
        var s = new Switch();
        var ms = new Switch { On = true };
        MemoryFileSystem fs = NewDbFs();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.Listeners.Add(new ThrowingListener(s));
            return o;
        }
        ColumnFamilyOptions Cfo() => new() { MergeOperator = new ThrowingAppend(ms) };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        s.On = true;
        Exception? failure = Capture(db.Flush);
        Assert.IsNotNull(failure);
        // Writes are stopped by the (fatal) background error even though the listener threw while being told.
        Exception? write = Capture(() => db.Put(B("after"), B("x")));
        Assert.IsInstanceOfType<RocksDbException>(write, "writes continued after the background error: " + write);
        s.On = false;
        AssertBackgroundFailureRecoverable(fs, db, failure, ms, model, Dbo, Cfo, "listener + merge");
    }

    // ---- file checksum generator ------------------------------------------------------------------------------------

    public enum ChecksumThrow
    {
        Create,
        Update,
        Finish,
        GetChecksum,
    }

    private sealed class ThrowingChecksumGen(Switch s, ChecksumThrow where) : FileChecksumGenerator
    {
        private uint _crc;

        public override string Name => "FileChecksumCrc32c";

        public override void Update(ReadOnlySpan<byte> data)
        {
            if (where == ChecksumThrow.Update) Boom(s, "Update");
            _crc = RocksDb.Managed.Util.Crc32C.Extend(_crc, data);
        }

        public override void Finish()
        {
            if (where == ChecksumThrow.Finish) Boom(s, "Finish");
        }

        public override byte[] GetChecksum()
        {
            if (where == ChecksumThrow.GetChecksum) Boom(s, "GetChecksum");
            byte[] b = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(b, _crc);
            return b;
        }
    }

    private sealed class ThrowingChecksumFactory(Switch s, ChecksumThrow where) : FileChecksumGenFactory
    {
        public override string Name => "FileChecksumGenCrc32cFactory";

        public override FileChecksumGenerator? CreateFileChecksumGenerator(FileChecksumGenContext context)
        {
            if (where == ChecksumThrow.Create) Boom(s, "CreateFileChecksumGenerator");
            return new ThrowingChecksumGen(s, where);
        }
    }

    /// <summary>
    /// A file checksum generator (or its factory) that throws while a flush or compaction writes its output
    /// (WritableFileWriter with the checksum generator, FlushJob / CompactionJob): the job fails like an I/O error of
    /// its output file, the output is removed, and the DB is recoverable.
    /// </summary>
    [TestMethod]
    [DataRow(ChecksumThrow.Create, false)]
    [DataRow(ChecksumThrow.Update, false)]
    [DataRow(ChecksumThrow.Finish, false)]
    [DataRow(ChecksumThrow.GetChecksum, false)]
    [DataRow(ChecksumThrow.Update, true)]
    [DataRow(ChecksumThrow.GetChecksum, true)]
    public void ThrowingFileChecksumGeneratorFailsTheJobRecoverably(ChecksumThrow where, bool compaction)
    {
        var s = new Switch();
        MemoryFileSystem fs = NewDbFs();
        DbOptions Dbo()
        {
            DbOptions o = NewOptions(fs);
            o.FileChecksumGenFactory = new ThrowingChecksumFactory(s, where);
            return o;
        }
        ColumnFamilyOptions Cfo() => new() { MergeOperator = new StringAppendMergeOperator(), Level0FileNumCompactionTrigger = 100 };
        RocksDatabase db = RocksDatabase.Open(Db, Dbo(), Cfo());
        Dictionary<string, string?> model = WriteWorkload(db, 1);
        if (compaction)
        {
            db.Flush();
            Dictionary<string, string?> more = WriteWorkload(db, 2);
            foreach (KeyValuePair<string, string?> kv in more) model[kv.Key] = kv.Value;
            db.Flush();
        }
        s.On = true;
        Exception? failure = Capture(compaction ? db.CompactRange : db.Flush);
        Assert.IsTrue(s.Calls > 0);
        AssertBackgroundFailureRecoverable(fs, db, failure, s, model, Dbo, Cfo, where + (compaction ? " compaction" : " flush"));
    }

    // ---- WAL filter -------------------------------------------------------------------------------------------------

    private sealed class ThrowingWalFilter(Switch s, bool inMap) : WalFilter
    {
        public override string Name => "ThrowingWalFilter";

        public override void ColumnFamilyLogNumberMap(IReadOnlyDictionary<uint, ulong> columnFamilyLogNumbers,
            IReadOnlyDictionary<string, uint> columnFamilyNameIds)
        {
            if (inMap) Boom(s, "ColumnFamilyLogNumberMap");
        }

        public override WalProcessingOption LogRecord(WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
        {
            batchChanged = false;
            if (!inMap) Boom(s, "LogRecord");
            return WalProcessingOption.ContinueProcessing;
        }
    }

    /// <summary>
    /// A WAL filter that throws during recovery (DBImpl::InvokeWalFilterIfNeededOnWalRecord /
    /// OnColumnFamilyToWalNumberMap): the open fails, nothing is changed on disk, and an open without the filter
    /// recovers every write (validation.md: "an exception thrown by the filter fails the open").
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ThrowingWalFilterFailsTheOpenWithoutChangingTheDb(bool inMap)
    {
        var s = new Switch { On = true };
        MemoryFileSystem fs = NewDbFs();
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator() };
        Dictionary<string, string?> model;
        using (RocksDatabase db = RocksDatabase.Open(Db, NewOptions(fs), cfo))
        {
            model = WriteWorkload(db, 1);
        }
        // The reopen flushed the recovered WAL; this write stays in the new WAL only.
        using (RocksDatabase db = RocksDatabase.Open(Db, NewOptions(fs), cfo))
        {
            db.Put(B("walonly"), B("1"));
            model["walonly"] = "1";
        }
        // The info LOG is rotated by every open, also by upstream's failed ones (CreateLoggerFromOptions runs first).
        static SortedSet<string> DbFiles(IFileSystem fs) => new(fs.GetChildren(Db).Select(Path.GetFileName)
            .Where(n => !n!.StartsWith("LOG", StringComparison.Ordinal))!, StringComparer.Ordinal);
        SortedSet<string> files = DbFiles(fs);
        DbOptions withFilter = NewOptions(fs);
        withFilter.WalFilter = new ThrowingWalFilter(s, inMap);
        Exception? open = Capture(() => RocksDatabase.Open(Db, withFilter, cfo).Dispose());
        Assert.IsNotNull(open, "open succeeded although the filter threw");
        Assert.IsTrue(s.Calls > 0);
        Assert.IsNotNull(FindUserException(open), open.ToString());
        SortedSet<string> after = DbFiles(fs);
        CollectionAssert.AreEqual(files.ToList(), after.ToList(), "the failed open changed the directory: before " + string.Join(" ", files)
            + " after " + string.Join(" ", after));
        using RocksDatabase reopened = RocksDatabase.Open(Db, NewOptions(fs), cfo);
        AssertModel(reopened, model, "after failed filtered open");
    }
}
