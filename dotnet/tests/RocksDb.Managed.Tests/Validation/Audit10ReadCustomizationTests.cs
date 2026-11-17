using System.Buffers;
using System.Globalization;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #10, user customization points that shape keys, ordering and reads: prefix extractors (SliceTransform),
/// comparators, custom filter policies and the read callbacks (ReadOptions.TableFilter, inplace_callback,
/// GetMergeOperands continue_cb). Upstream has no exceptions in these contracts; the port's rule is: a hook that throws
/// on a foreground call (Get, MultiGet, iterator, GetMergeOperands) fails that call and leaves the DB usable with nothing
/// leaked; one that throws while a memtable is written fails like upstream's failed memtable insert
/// (DBImpl::HandleMemTableInsertFailure: fatal background error kMemTable); one that throws in a flush or compaction
/// fails that job like an I/O error (fatal background error with paranoid_checks) and leaves no output file live, the
/// data being intact after a reopen.
/// </summary>
[TestClass]
public sealed class Audit10ReadCustomizationTests
{
    private sealed class HookException() : Exception("hook failed");

    /// <summary>Bytewise order under a custom name (no built-in fast path); throws while <see cref="Throw"/> is set or once after <see cref="ThrowAfter"/> calls.</summary>
    private sealed class ThrowingComparator : Comparator
    {
        public volatile bool Throw;
        public int ThrowAfter = -1;
        public override string Name => "audit10.Bytewise";

        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            if (Throw) throw new HookException();
            if (Volatile.Read(ref ThrowAfter) >= 0 && Interlocked.Decrement(ref ThrowAfter) < 0) throw new HookException();
            return a.SequenceCompareTo(b);
        }
    }

    /// <summary>Bytewise, or reverse bytewise once <see cref="Reverse"/> is set (same name).</summary>
    private sealed class FlippingComparator : Comparator
    {
        public volatile bool Reverse;
        public override string Name => "audit10.Flipping";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Reverse ? b.SequenceCompareTo(a) : a.SequenceCompareTo(b);
    }

    /// <summary>A fixed 2-byte prefix under a custom name; throws while <see cref="Throw"/> is set.</summary>
    private sealed class ThrowingPrefix : SliceTransform
    {
        public volatile bool Throw;
        public override string Name => "audit10.Prefix2";

        public override ReadOnlySpan<byte> Transform(ReadOnlySpan<byte> key)
        {
            if (Throw) throw new HookException();
            return key[..2];
        }

        public override bool InDomain(ReadOnlySpan<byte> key)
        {
            if (Throw) throw new HookException();
            return key.Length >= 2;
        }
    }

    /// <summary>A Bloom filter policy under a custom compatibility name whose builder or reader throws on demand.</summary>
    private sealed class ThrowingFilterPolicy(string compatibilityName) : FilterPolicy
    {
        public volatile bool ThrowOnAdd, ThrowOnFinish, ThrowOnCreate, ThrowOnRead;
        public int Reads;
        private readonly FilterPolicy _inner = new BloomFilterPolicy(10);
        public override string CompatibilityName => compatibilityName;
        public override string Name => "audit10.ThrowingFilter";

        public override IFilterBitsBuilder CreateBuilder(int formatVersion)
        {
            if (ThrowOnCreate) throw new HookException();
            return new Builder(this, _inner.CreateBuilder(formatVersion));
        }

        public override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents)
        {
            Interlocked.Increment(ref Reads);
            if (ThrowOnRead) throw new HookException();
            return BuiltinFilterBitsReader.Create(contents);
        }

        private sealed class Builder(ThrowingFilterPolicy p, IFilterBitsBuilder inner) : IFilterBitsBuilder
        {
            public void AddKey(ReadOnlySpan<byte> key)
            {
                if (p.ThrowOnAdd) throw new HookException();
                inner.AddKey(key);
            }

            public int EstimatedEntries => inner.EstimatedEntries;

            public void Finish(IBufferWriter<byte> output)
            {
                if (p.ThrowOnFinish) throw new HookException();
                inner.Finish(output);
            }

            public void Dispose() => inner.Dispose();
        }
    }

    private static DbOptions Opts(MemoryFileSystem fs) => new() { CreateIfMissing = true, FileSystem = fs };

    private static string I(int i) => i.ToString(CultureInfo.InvariantCulture);

    private static void Fill(RocksDatabase db, int n, string tag)
    {
        for (int i = 0; i < n; i++) db.Put(Key(i), B(tag + I(i)));
    }

    /// <summary>Keys 0..n-1 hold tag+i, seen by Get and by a full scan.</summary>
    private static void AssertFilled(RocksDatabase db, int n, string tag)
    {
        for (int i = 0; i < n; i++) Assert.AreEqual(tag + I(i), S(db.Get(Key(i))), "key " + i);
        Assert.AreEqual(n, Scan(db, null).Count);
    }

    private static List<string> Scan(RocksDatabase db, ReadOptions? ro)
    {
        var keys = new List<string>();
        using Iterator it = db.NewIterator(ro);
        for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(S(it.Key.ToArray())!);
        return keys;
    }

    private static bool HasTableFiles(MemoryFileSystem fs)
    {
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c.EndsWith(".sst", StringComparison.Ordinal)) return true;
        }
        return false;
    }

    /// <summary>
    /// Every table file in the DB directory is live: nothing kept alive by a leaked super version or table reader lease,
    /// and no output of a failed job left behind (checked after a compaction that made the previous files obsolete).
    /// </summary>
    private static void AssertOnlyLiveTableFiles(RocksDatabase db, MemoryFileSystem fs)
    {
        var live = new HashSet<string>(StringComparer.Ordinal);
        foreach (LiveFileMetadata f in db.GetLiveFilesMetadata()) live.Add(System.IO.Path.GetFileName(f.Name));
        foreach (string c in fs.GetChildren(DbPath))
        {
            if (c.EndsWith(".sst", StringComparison.Ordinal)) Assert.IsTrue(live.Contains(c), c + " is not live");
        }
    }

    private static RocksDbException BackgroundError(RocksDatabase db) => db.BackgroundError ?? throw new AssertFailedException("no background error");

    // ---- comparator --------------------------------------------------------------------------------------------

    /// <summary>
    /// Comparator calls on the read path (MemTable::Get, BlockBasedTable::Get, MergingIterator / DBIter::Seek): an
    /// exception reaches the caller and leaves no super version, pinned block or reader lease behind (a later
    /// compaction purges its inputs).
    /// </summary>
    [TestMethod]
    public void ComparatorThrowingOnReadsFailsTheReadOnly()
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new ThrowingComparator();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), new ColumnFamilyOptions { Comparator = cmp, DisableAutoCompactions = true });
        Fill(db, 100, "a");
        db.Flush();
        Fill(db, 50, "b");
        db.Flush();
        Fill(db, 10, "c");
        cmp.Throw = true;
        Assert.ThrowsExactly<HookException>(() => db.Get(Key(3)));
        Assert.ThrowsExactly<HookException>(() => db.Get(Key(70)));
        Assert.ThrowsExactly<HookException>(() => db.MultiGet([Key(3), Key(70)]));
        using (Iterator it = db.NewIterator())
        {
            Assert.ThrowsExactly<HookException>(() => it.Seek(Key(10)));
            cmp.Throw = false;
            it.Seek(Key(10));
            Assert.AreEqual("b10", S(it.Value.ToArray()));
        }
        Assert.IsNull(db.BackgroundError);
        for (int i = 0; i < 100; i++) Assert.AreEqual((i < 10 ? "c" : i < 50 ? "b" : "a") + I(i), S(db.Get(Key(i))));
        db.CompactRange();
        AssertOnlyLiveTableFiles(db, fs);
    }

    /// <summary>
    /// The comparator throwing in the memtable insert (InlineSkipList::Insert) after the WAL write: like a failed
    /// memtable insert upstream (DBImpl::HandleMemTableInsertFailure -> SetBGError(kMemTable), kFatalError in
    /// DefaultReasonMap) the write fails and the DB stops writes; the data written before is intact after a reopen.
    /// </summary>
    [TestMethod]
    public void ComparatorThrowingOnMemtableInsertStopsWritesLikeAFailedMemtableInsert()
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new ThrowingComparator();
        var cfo = new ColumnFamilyOptions { Comparator = cmp };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Fill(db, 10, "a");
            cmp.Throw = true;
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(Key(5), B("x")));
            Assert.IsInstanceOfType<HookException>(e.InnerException);
            cmp.Throw = false;
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            Assert.ThrowsExactly<RocksDbException>(() => db.Put(Key(6), B("y")));
            Assert.AreEqual("a6", S(db.Get(Key(6))));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            // The refused write was cut from the WAL at the close (SetAttemptTruncateSize), as upstream.
            for (int i = 0; i < 10; i++) Assert.AreEqual("a" + I(i), S(db.Get(Key(i))));
            db.Put(Key(11), B("z"));
            Assert.AreEqual("z", S(db.Get(Key(11))));
        }
    }

    /// <summary>
    /// The comparator throwing in one memtable insert of a write group with concurrent writers (parallel memtable
    /// inserts with allow_concurrent_memtable_write, or the leader inserting for the group): every writer returns (no
    /// hang), the failing one with the exception; writes stop (fatal background error) and the DB closes and reopens.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task ComparatorThrowingInAGroupOfConcurrentWritersDoesNotHang(bool concurrentMemtableWrites)
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new ThrowingComparator();
        var cfo = new ColumnFamilyOptions { Comparator = cmp };
        DbOptions dbo = Opts(fs);
        dbo.AllowConcurrentMemtableWrite = concurrentMemtableWrites;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo))
        {
            Fill(db, 50, "a");
            cmp.ThrowAfter = 2000;
            var tasks = new List<Task>();
            for (int t = 0; t < 8; t++)
            {
                int thread = t;
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 300; i++) db.Put(Key(1000 + thread * 1000 + i), B("w"));
                }));
            }
            Task all = Task.WhenAll(tasks);
            Assert.AreSame(all, await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(30))), "writers hung");
            cmp.ThrowAfter = -1;
            int failed = 0;
            foreach (Task t in tasks)
            {
                if (t.IsFaulted) failed++;
            }
            Assert.IsTrue(failed > 0, "no writer saw the failure");
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            for (int i = 0; i < 50; i++) Assert.AreEqual("a" + I(i), S(db.Get(Key(i))));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo))
        {
            for (int i = 0; i < 50; i++) Assert.AreEqual("a" + I(i), S(db.Get(Key(i))));
            db.Put(Key(1), B("z"));
        }
    }

    /// <summary>
    /// User delegates on the read API that throw (a ReadOnlySpanFunc of a zero-copy Get, also with
    /// inplace_update_support where the value is read under the memtable's read lock; a LiveFileVisitor): the exception
    /// reaches the caller after the lock, pooled lookup state and super version were released (writes to the same key
    /// and a compaction purging the files still work).
    /// </summary>
    [TestMethod]
    public void ThrowingReadDelegatesReleaseWhatTheyPinned()
    {
        MemoryFileSystem fs = NewFs();
        DbOptions dbo = Opts(fs);
        dbo.AllowConcurrentMemtableWrite = false;
        using RocksDatabase db = RocksDatabase.Open(DbPath, dbo, new ColumnFamilyOptions { InplaceUpdateSupport = true, DisableAutoCompactions = true });
        Fill(db, 20, "a");
        db.Flush();
        db.Put(Key(1), B("mem"));
        for (int round = 0; round < 3; round++)
        {
            Assert.ThrowsExactly<HookException>(() => db.Get(Key(1), 0, ThrowingReader));
            Assert.ThrowsExactly<HookException>(() => db.Get(Key(2), 0, ThrowingReader));
            Assert.ThrowsExactly<HookException>(() => db.VisitLiveFiles(0, static (in LiveFileInfo _, int _) => throw new HookException()));
        }
        db.Put(Key(1), B("in-place"));
        Assert.AreEqual("in-place", S(db.Get(Key(1))));
        Fill(db, 20, "b");
        db.Flush();
        db.CompactRange();
        AssertOnlyLiveTableFiles(db, fs);
    }

    private static int ThrowingReader(ReadOnlySpan<byte> value, int state) => throw new HookException();

    /// <summary>Reverse bytewise order (the empty key sorts last) whose separator / successor overrides do nothing.</summary>
    private sealed class ReverseNoShortening : Comparator
    {
        public override string Name => "audit10.ReverseNoShortening";
        public override int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => b.SequenceCompareTo(a);
        public override void FindShortestSeparator(ReadOnlySpan<byte> start, ReadOnlySpan<byte> limit, IBufferWriter<byte> result) { }
        public override void FindShortSuccessor(ReadOnlySpan<byte> key, IBufferWriter<byte> result) { }
    }

    /// <summary>
    /// include/rocksdb/comparator.h (FindShortestSeparator / FindShortSuccessor): "Simple comparator implementations may
    /// return with *start unchanged, i.e., an implementation of this method that does nothing is correct." An override
    /// that writes nothing keeps the index key unchanged (ShortenedIndexBuilder::FindShortestInternalKeySeparator /
    /// FindShortInternalKeySuccessor). Before the fix the empty key became the index entry of every block, which sorts
    /// last under this comparator: point lookups missed keys (and the native differential run did not finish). Native:
    /// Audit10ReadCustomizationCompatTests.ComparatorWithEmptySeparatorOverridesMatchesNative.
    /// </summary>
    [TestMethod]
    [Timeout(30_000)]
    [DataRow(false)]
    [DataRow(true)]
    public void ComparatorWhoseSeparatorOverridesWriteNothingKeepsIndexKeys(bool partitionedIndex)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { Comparator = new ReverseNoShortening() };
        cfo.TableOptions.BlockSize = 256;
        if (partitionedIndex)
        {
            cfo.TableOptions.IndexType = IndexType.TwoLevelIndexSearch;
            cfo.TableOptions.MetadataBlockSize = 256;
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Fill(db, 500, "a");
            db.Flush();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            AssertFilled(db, 500, "a");
            using Iterator it = db.NewIterator();
            it.Seek(Key(250));
            Assert.AreEqual("a250", S(it.Value.ToArray()));
            it.Next();
            Assert.AreEqual("a249", S(it.Value.ToArray()));
            it.SeekForPrev(Key(250));
            Assert.AreEqual("a250", S(it.Value.ToArray()));
            it.Prev();
            Assert.AreEqual("a251", S(it.Value.ToArray()));
            it.SeekToLast();
            Assert.AreEqual("a0", S(it.Value.ToArray()));
        }
    }

    /// <summary>
    /// The comparator throwing during a flush (BuildTable): the flush fails like an I/O error (kFlushNoWAL + IOError with
    /// paranoid_checks: kFatalError, so Resume refuses), no table file is left and the data is intact after a reopen.
    /// </summary>
    [TestMethod]
    public void ComparatorThrowingInFlushIsAFatalBackgroundError()
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new ThrowingComparator();
        var cfo = new ColumnFamilyOptions { Comparator = cmp };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Fill(db, 100, "a");
            cmp.Throw = true;
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, e.Code);
            cmp.Throw = false;
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            Assert.ThrowsExactly<RocksDbException>(() => db.Resume());
            AssertFilled(db, 100, "a");
            Assert.IsFalse(HasTableFiles(fs));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            AssertFilled(db, 100, "a");
            db.Flush();
            AssertFilled(db, 100, "a");
        }
    }

    /// <summary>
    /// The comparator throwing while a manual compaction is picked (CompactionPicker::GetRange on the caller's thread):
    /// the exception reaches the caller and nothing stays marked as being compacted (the next CompactRange runs).
    /// Throwing in the compaction job itself (CompactionIterator): a fatal background error (kCompaction + IOError,
    /// paranoid), the output is deleted, the inputs stay live and the data is intact after a reopen. A background
    /// compaction behaves the same.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void ComparatorThrowingInCompactionFailsTheJobAndKeepsTheInputs(bool automatic)
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new ThrowingComparator();
        var cfo = new ColumnFamilyOptions { Comparator = cmp, DisableAutoCompactions = !automatic, Level0FileNumCompactionTrigger = 2 };
        // Automatic case: the comparator throws only while the compaction job runs. Arming it from OnCompactionBegin
        // (and disarming it at OnCompactionCompleted) makes the failure land inside the job deterministically; a count of
        // comparisons could land in the flush or the picking instead, depending on timing.
        var arm = new ArmDuringCompaction(cmp);
        DbOptions OpenOpts()
        {
            DbOptions o = Opts(fs);
            if (automatic) o.Listeners.Add(arm);
            return o;
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, OpenOpts(), cfo))
        {
            Fill(db, 100, "a");
            db.Flush();
            Fill(db, 100, "b");
            if (automatic)
            {
                arm.Armed = true;
                try
                {
                    db.Flush(); // the second L0 file triggers the automatic compaction
                }
                catch (RocksDbException e) when (e.InnerException is RocksDbException { InnerException: HookException } || e.InnerException is HookException)
                {
                    // The flush wait can return the background error of the compaction it triggered when that compaction
                    // fails first (WaitForFlushMemTables reports the background error, as upstream).
                }
                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (db.BackgroundError is null && sw.Elapsed < TimeSpan.FromSeconds(20)) Thread.Sleep(10);
                arm.Armed = false;
                cmp.Throw = false;
                Assert.IsTrue(arm.Fired, "the compaction did not start");
            }
            else
            {
                db.Flush();
                cmp.Throw = true;
                Assert.ThrowsExactly<HookException>(() => db.CompactRange());
                cmp.Throw = false;
                Assert.IsNull(db.BackgroundError);
                cmp.ThrowAfter = 300; // past the picking, inside the job
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.CompactRange());
                Assert.IsInstanceOfType<HookException>(e.InnerException);
            }
            cmp.ThrowAfter = -1;
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            Assert.AreEqual("2", db.GetProperty("rocksdb.num-files-at-level0"));
            if (!automatic) AssertOnlyLiveTableFiles(db, fs);
            AssertFilled(db, 100, "b");
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            AssertFilled(db, 100, "b");
            db.CompactRange();
            db.WaitForCompact();
            AssertFilled(db, 100, "b");
            AssertOnlyLiveTableFiles(db, fs);
        }
    }

    /// <summary>Makes <see cref="ThrowingComparator"/> throw only while a compaction job runs (once armed).</summary>
    private sealed class ArmDuringCompaction(ThrowingComparator cmp) : EventListener
    {
        public volatile bool Armed;
        public volatile bool Fired;

        public override void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info)
        {
            if (!Armed) return;
            Fired = true;
            cmp.Throw = true;
        }

        public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) => cmp.Throw = false;
    }

    /// <summary>
    /// A comparator whose order changes after keys were written (an inconsistent comparator): the flush's
    /// OutputValidator::Add (db/builder.cc BuildTable, db/output_validator.cc CanAdd) sees the memtable's keys out of
    /// order and fails the flush with Corruption "Compaction sees out-of-order keys." instead of writing an unsorted
    /// table (native: Audit10ReadCustomizationCompatTests.InconsistentComparatorFailsTheFlushOnBothEngines).
    /// </summary>
    [TestMethod]
    public void InconsistentComparatorIsDetectedByTheFlush()
    {
        MemoryFileSystem fs = NewFs();
        var cmp = new FlippingComparator();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), new ColumnFamilyOptions { Comparator = cmp });
        Fill(db, 20, "a");
        cmp.Reverse = true;
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.Corruption, e.Code);
        StringAssert.Contains(e.Message, "out-of-order keys");
        Assert.IsFalse(HasTableFiles(fs));
    }

    // ---- prefix extractor --------------------------------------------------------------------------------------

    /// <summary>
    /// A throwing prefix extractor: on reads (memtable bloom in MemTable::Get, FullFilterKeyMayMatch, prefix seek) the
    /// exception reaches the caller; in the memtable insert (DynamicBloom add) it is a failed memtable insert (fatal,
    /// writes stop; the refused write is cut from the WAL at the close); in a flush (filter builder) a fatal
    /// background error without a table file; the data is intact after a reopen.
    /// </summary>
    [TestMethod]
    public void PrefixExtractorThrowingFailsReadsAndJobsSafely()
    {
        MemoryFileSystem fs = NewFs();
        var pe = new ThrowingPrefix();
        var cfo = new ColumnFamilyOptions
        {
            PrefixExtractor = pe,
            MemtablePrefixBloomSizeRatio = 0.1,
            TableOptions = new BlockBasedTableOptions { FilterPolicy = new BloomFilterPolicy(10), WholeKeyFiltering = false },
        };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Fill(db, 100, "a");
            db.Flush();
            Fill(db, 50, "b");
            pe.Throw = true;
            Assert.ThrowsExactly<HookException>(() => db.Get(Key(3)));
            Assert.ThrowsExactly<HookException>(() => db.Get(Key(70)));
            Assert.ThrowsExactly<HookException>(() => db.MultiGet([Key(3), Key(70)]));
            using (Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true }))
            {
                Assert.ThrowsExactly<HookException>(() => it.Seek(Key(10)));
            }
            Assert.IsNull(db.BackgroundError);
            pe.Throw = false;
            using (Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true }))
            {
                it.Seek(Key(10));
                Assert.AreEqual("b10", S(it.Value.ToArray()));
            }
            pe.Throw = true;
            RocksDbException put = Assert.ThrowsExactly<RocksDbException>(() => db.Put(Key(200), B("z")));
            Assert.IsInstanceOfType<HookException>(put.InnerException);
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            pe.Throw = false;
            for (int i = 0; i < 100; i++) Assert.AreEqual((i < 50 ? "b" : "a") + I(i), S(db.Get(Key(i))));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            for (int i = 0; i < 100; i++) Assert.AreEqual((i < 50 ? "b" : "a") + I(i), S(db.Get(Key(i))));
            // The refused write was cut from the WAL at the close (DBImpl::WriteImpl SetAttemptTruncateSize), as upstream.
            Assert.IsNull(db.Get(Key(200)));
        }
        // A flush whose filter builder calls the throwing extractor.
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            db.Put(Key(300), B("w"));
            pe.Throw = true;
            Assert.AreEqual(StatusCode.IOError, Assert.ThrowsExactly<RocksDbException>(() => db.Flush()).Code);
            pe.Throw = false;
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            AssertOnlyLiveTableFiles(db, fs);
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Assert.AreEqual("w", S(db.Get(Key(300))));
        }
    }

    /// <summary>
    /// The prefix extractor changes between opens (BlockBasedTable::Open, PrefixExtractorChangedHelper): a table built
    /// with a custom extractor that is no longer configured keeps no table extractor (CreateFromString finds no built-in
    /// for its id), so its prefix filter is not used; a built-in one is recreated from rocksdb.prefix.extractor.name.
    /// Point lookups, prefix seeks (prefix_same_as_start with the new extractor) and total order scans stay correct.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PrefixExtractorChangedBetweenOpensKeepsReadsCorrect(bool builtIn)
    {
        MemoryFileSystem fs = NewFs();
        SliceTransform first = builtIn ? SliceTransform.CreateFixedPrefix(3) : new ThrowingPrefix();
        ColumnFamilyOptions Cfo(SliceTransform? pe) => new()
        {
            PrefixExtractor = pe,
            TableOptions = new BlockBasedTableOptions { FilterPolicy = new BloomFilterPolicy(10), WholeKeyFiltering = false },
        };
        string[] keys = ["aa1", "aa2", "ab1", "abc", "b", "ba1", "bb22", "bc", "c"];
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), Cfo(first)))
        {
            foreach (string k in keys) db.Put(B(k), B("v" + k));
            db.Flush();
            foreach (TableProperties p in db.DefaultColumnFamily.GetPropertiesOfAllTables().Values)
                Assert.AreEqual(first.AsString(), p.PrefixExtractorName);
        }
        foreach (SliceTransform? next in new[] { SliceTransform.CreateFixedPrefix(1), SliceTransform.CreateCappedPrefix(2), null })
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), Cfo(next));
            foreach (string k in keys) Assert.AreEqual("v" + k, S(db.Get(B(k))), k + " with " + next);
            Assert.AreEqual(keys.Length, Scan(db, new ReadOptions { TotalOrderSeek = true }).Count);
            if (next is null) continue;
            foreach (string k in keys)
            {
                if (!next.InDomain(B(k))) continue;
                // Every key with the configured prefix of k, from k on.
                var expected = new List<string>();
                foreach (string o in keys)
                {
                    if (string.CompareOrdinal(o, k) >= 0 && next.InDomain(B(o)) && next.Transform(B(o)).SequenceEqual(next.Transform(B(k))))
                        expected.Add(o);
                }
                var got = new List<string>();
                using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true });
                for (it.Seek(B(k)); it.Valid; it.Next()) got.Add(S(it.Key.ToArray())!);
                CollectionAssert.AreEqual(expected, got, k + " with " + next);
            }
        }
    }

    // ---- filter policy -----------------------------------------------------------------------------------------

    /// <summary>
    /// A custom filter policy whose builder throws (CreateBuilder / AddKey / Finish, called by the table builder during a
    /// flush): the flush fails like an I/O error (fatal background error), no table file is left and the data is intact
    /// after a reopen.
    /// </summary>
    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(2)]
    public void FilterBuilderThrowingFailsTheFlushWithoutATableFile(int mode)
    {
        MemoryFileSystem fs = NewFs();
        var fp = new ThrowingFilterPolicy("audit10.Bloom");
        var cfo = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { FilterPolicy = fp } };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Fill(db, 100, "a");
            fp.ThrowOnAdd = mode == 0;
            fp.ThrowOnFinish = mode == 1;
            fp.ThrowOnCreate = mode == 2;
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, e.Code);
            fp.ThrowOnAdd = fp.ThrowOnFinish = fp.ThrowOnCreate = false;
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            Assert.IsFalse(HasTableFiles(fs));
            AssertFilled(db, 100, "a");
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            AssertFilled(db, 100, "a");
            db.Flush();
            AssertFilled(db, 100, "a");
        }
    }

    /// <summary>
    /// The filter block is found by the configured policy's CompatibilityName (BlockBasedTable::PrefetchIndexAndFilterBlocks:
    /// "fullfilter." + CompatibilityName) and rocksdb.filter.policy records Name() (BlockBasedTableBuilder::
    /// WritePropertiesBlock): a table written with a custom policy is read without a filter by a DB configured with
    /// another policy (or none), and with its filter again when the policy comes back.
    /// </summary>
    [TestMethod]
    public void CustomFilterPolicyIsPersistedByNameAndIgnoredWhenItChanges()
    {
        MemoryFileSystem fs = NewFs();
        var fp = new ThrowingFilterPolicy("audit10.Bloom");
        ColumnFamilyOptions Cfo(FilterPolicy? p) => new() { TableOptions = new BlockBasedTableOptions { FilterPolicy = p } };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), Cfo(fp)))
        {
            Fill(db, 100, "a");
            db.Flush();
            TableProperties props = db.DefaultColumnFamily.GetPropertiesOfAllTables().Values.Single();
            Assert.AreEqual("audit10.ThrowingFilter", props.FilterPolicyName);
        }
        foreach (FilterPolicy? other in new FilterPolicy?[] { new BloomFilterPolicy(10), null })
        {
            using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), Cfo(other));
            AssertFilled(db, 100, "a");
            Assert.IsNull(db.Get(B("missing")));
        }
        int before = Volatile.Read(ref fp.Reads);
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), Cfo(fp)))
        {
            AssertFilled(db, 100, "a");
            Assert.IsNull(db.Get(B("missing")));
        }
        Assert.IsTrue(Volatile.Read(ref fp.Reads) > before, "the custom reader was not used");
    }

    /// <summary>
    /// A custom filter policy whose reader factory throws when a table is opened: the exception reaches the caller that
    /// opened the table: DB::Open when the table is loaded at open (VersionBuilder::LoadTableHandlers; the open fails and
    /// releases the DB), else the read that opens it; the DB stays usable and the table opens once the reader works.
    /// </summary>
    [TestMethod]
    public void FilterReaderThrowingFailsTheCallThatOpensTheTable()
    {
        MemoryFileSystem fs = NewFs();
        var fp = new ThrowingFilterPolicy("audit10.Bloom");
        var cfo = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { FilterPolicy = fp } };
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            Fill(db, 100, "a");
            db.Flush();
            Fill(db, 100, "b");
            db.Flush();
        }
        fp.ThrowOnRead = true;
        Assert.ThrowsExactly<HookException>(() => RocksDatabase.Open(DbPath, Opts(fs), cfo).Dispose());
        fp.ThrowOnRead = false;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo))
        {
            AssertFilled(db, 100, "b");
            Assert.IsTrue(Volatile.Read(ref fp.Reads) > 0);
        }
    }

    // ---- read callbacks ----------------------------------------------------------------------------------------

    /// <summary>
    /// ReadOptions::table_filter (TableCache::NewIterator, db/table_cache.cc): an iterator skips the tables the filter
    /// rejects, on level 0 and on sorted levels, while the range tombstones of a skipped table still apply; point
    /// lookups do not consult it.
    /// </summary>
    [TestMethod]
    public void TableFilterSkipsTablesForIteratorsOnly()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        for (int i = 0; i < 10; i++) db.Put(B("a" + I(i)), B("v"));
        db.Flush();
        for (int i = 0; i < 11; i++) db.Put(B("b" + I(i)), B("v"));
        db.Flush();
        db.CompactRange(); // both files moved to a sorted level (trivial move)
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
        for (int i = 0; i < 12; i++) db.Put(B("c" + I(i)), B("v"));
        db.Flush(); // L0, 12 entries
        db.Put(B("d0"), B("v"));
        db.DeleteRange(B("c3"), B("c6"));
        db.Flush(); // L0, a put and a range deletion
        int total = Scan(db, null).Count;
        Assert.AreEqual(1 + 10 + 11 + 12 - 3, total); // c3, c4, c5 deleted

        var seen = new List<ulong>();
        List<string> all = Scan(db, new ReadOptions { TableFilter = p => { lock (seen) seen.Add(p.NumEntries); return true; } });
        Assert.AreEqual(total, all.Count);
        CollectionAssert.IsSubsetOf(new ulong[] { 2, 10, 11, 12 }, seen);

        // Skip the b table of the sorted level: the b keys disappear from iterators only.
        List<string> noB = Scan(db, new ReadOptions { TableFilter = static p => p.NumEntries != 11 });
        Assert.AreEqual(total - 11, noB.Count);
        Assert.IsFalse(noB.Exists(static k => k[0] is 'b'));
        // Skip the table with the range deletion: d0 disappears, its tombstone still hides c3..c5.
        List<string> noDel = Scan(db, new ReadOptions { TableFilter = static p => p.NumRangeDeletions == 0 });
        CollectionAssert.DoesNotContain(noDel, "d0");
        CollectionAssert.DoesNotContain(noDel, "c4");
        Assert.AreEqual(total - 1, noDel.Count);
        // Reverse iteration and seeks over skipped tables.
        using (Iterator it = db.NewIterator(new ReadOptions { TableFilter = static p => p.NumEntries != 12 }))
        {
            it.SeekForPrev(B("c9"));
            Assert.AreEqual("b9", S(it.Key.ToArray()));
            it.Seek(B("c"));
            Assert.AreEqual("d0", S(it.Key.ToArray()));
            it.SeekToLast();
            Assert.AreEqual("d0", S(it.Key.ToArray()));
            it.Prev();
            Assert.AreEqual("b9", S(it.Key.ToArray()));
        }
        using (Iterator it = db.NewIterator(new ReadOptions { TableFilter = static p => p.NumEntries != 10 }))
        {
            // The level iterator steps over the skipped a table in both directions.
            it.Seek(B("a"));
            Assert.AreEqual("b0", S(it.Key.ToArray()));
            it.SeekForPrev(B("b0"));
            Assert.AreEqual("b0", S(it.Key.ToArray()));
            it.Prev();
            Assert.IsFalse(it.Valid);
        }
        // Point lookups ignore it.
        var ro = new ReadOptions { TableFilter = static _ => false };
        Assert.AreEqual("v", S(db.Get(ro, B("a1"))));
        Assert.AreEqual("v", S(db.MultiGet(ro, [B("c1")])[0]));
        Assert.AreEqual(0, Scan(db, ro).Count);
    }

    /// <summary>
    /// A throwing table_filter fails the iterator call that opened the table; nothing is leaked (a later compaction
    /// purges the files) and the DB stays usable. DBImpl::FailIfTableFilterWithRangeConversion: with
    /// min_tombstones_for_range_conversion &gt; 0 the iterator is refused with InvalidArgument.
    /// </summary>
    [TestMethod]
    public void TableFilterThrowingOrRefusedLeavesTheDbUsable()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), new ColumnFamilyOptions { DisableAutoCompactions = true });
        Fill(db, 50, "a");
        db.Flush();
        db.CompactRange();
        Fill(db, 50, "b");
        db.Flush();
        var ro = new ReadOptions { TableFilter = static _ => throw new HookException() };
        for (int round = 0; round < 3; round++)
        {
            try
            {
                using Iterator it = db.NewIterator(ro);
                Assert.ThrowsExactly<HookException>(() => it.SeekToFirst());
            }
            catch (HookException)
            {
                // Iterator creation opens the level-0 tables.
            }
        }
        AssertFilled(db, 50, "b");
        db.CompactRange();
        AssertOnlyLiveTableFiles(db, fs);

        db.SetOptions([new KeyValuePair<string, string>("min_tombstones_for_range_conversion", "4")]);
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.NewIterator(new ReadOptions { TableFilter = static _ => true }).Dispose());
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "table_filter is not supported when min_tombstones_for_range_conversion > 0");
        AssertFilled(db, 50, "b");
    }

    /// <summary>
    /// inplace_callback (MemTable::UpdateCallback) throwing: the memtable update fails like a failed memtable insert
    /// upstream (HandleMemTableInsertFailure: fatal background error kMemTable, writes stop); the value written before
    /// stays readable.
    /// </summary>
    [TestMethod]
    public void InplaceCallbackThrowingStopsWritesLikeAFailedMemtableInsert()
    {
        MemoryFileSystem fs = NewFs();
        bool doThrow = false;
        var cfo = new ColumnFamilyOptions
        {
            InplaceUpdateSupport = true,
            InplaceCallback = (Span<byte> existing, bool has, ref int size, ReadOnlySpan<byte> delta, IBufferWriter<byte> merged) =>
            {
                if (doThrow) throw new HookException();
                merged.Write(delta);
                return InplaceUpdateStatus.Updated;
            },
        };
        DbOptions dbo = Opts(fs);
        dbo.AllowConcurrentMemtableWrite = false;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo))
        {
            db.Put(B("k"), B("v1"));
            db.Put(B("k"), B("v2"));
            doThrow = true;
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => db.Put(B("k"), B("v3")));
            Assert.IsInstanceOfType<HookException>(e.InnerException);
            doThrow = false;
            Assert.AreEqual(ErrorSeverity.FatalError, BackgroundError(db).Severity);
            Assert.AreEqual("v2", S(db.Get(B("k"))));
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, dbo, cfo))
        {
            // The refused write was cut from the WAL at the close (SetAttemptTruncateSize), as upstream.
            Assert.AreEqual("v2", S(db.Get(B("k"))));
            db.Put(B("k"), B("v4"));
            Assert.AreEqual("v4", S(db.Get(B("k"))));
        }
    }

    /// <summary>GetMergeOperands continue_cb throwing: the exception reaches the caller, the DB stays usable.</summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task MergeOperandContinueCallbackThrowingFailsTheLookupOnly(bool async)
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { MergeOperator = new StringAppendMergeOperator(), DisableAutoCompactions = true };
        using RocksDatabase db = RocksDatabase.Open(DbPath, Opts(fs), cfo);
        db.Put(B("k"), B("base"));
        db.Flush();
        db.Merge(B("k"), B("1"));
        db.Flush();
        db.Merge(B("k"), B("2"));
        using var result = new MergeOperandsResult();
        int calls = 0;
        var throwing = new GetMergeOperandsOptions
        {
            ExpectedMaxNumberOfOperands = 10,
            ContinueCallback = _ => ++calls < 2 ? true : throw new HookException(), // throws at the operand in the file
        };
        if (async)
            await Assert.ThrowsExactlyAsync<HookException>(async () => await db.DefaultColumnFamily.TryGetMergeOperandsAsync(null, B("k"), throwing, result));
        else
            Assert.ThrowsExactly<HookException>(() => db.DefaultColumnFamily.TryGetMergeOperands(B("k"), throwing, result));
        Assert.AreEqual(2, calls);
        Assert.IsTrue(db.DefaultColumnFamily.TryGetMergeOperands(B("k"), new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 10 }, result));
        Assert.AreEqual(3, result.Count);
        Assert.AreEqual("base,1,2", S(db.Get(B("k"))));
        db.CompactRange();
        AssertOnlyLiveTableFiles(db, fs);
    }
}
