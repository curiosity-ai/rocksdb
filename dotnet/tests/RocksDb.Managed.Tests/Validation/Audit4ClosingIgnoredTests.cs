using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4 (closing, formerly ignored items): manual flush with a background error, memtable creation sequence at a
/// memtable switch, block checksum tickers, MANIFEST writer error state; checked against
/// db/db_impl/db_impl_compaction_flush.cc, db/db_impl/db_impl_write.cc, table/block_fetcher.cc and db/version_set.cc.
/// </summary>
[TestClass]
public sealed class Audit4ClosingIgnoredTests
{
    /// <summary>
    /// DBImpl::FlushMemTable (db_impl_compaction_flush.cc): the background error is not looked at before SwitchMemtable;
    /// the memtable is sealed and DBImpl::WaitForFlushMemTables reports the error ("if (!resuming_from_bg_err &amp;&amp;
    /// error_handler_.IsDBStopped()) s = error_handler_.GetBGError()" / the IsBGWorkStopped soft error branch).
    /// </summary>
    [TestMethod]
    public void ManualFlushSealsTheMemtableBeforeReportingTheBackgroundError()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
        DbImpl impl = db.DefaultColumnFamily.Db;
        db.Put(B("a"), B("1"));
        impl.SetBackgroundError(RocksDbException.IOError("injected"), BackgroundErrorReason.Flush, walRelated: false);
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-immutable-mem-table"));
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-immutable-mem-table")); // was: 0, failed before the switch
        Assert.AreEqual("0", db.GetProperty("rocksdb.num-files-at-level0"));
        Assert.AreEqual("1", GetString(db, "a"));
    }

    /// <summary>
    /// DBImpl::WaitForFlushMemTables is entered even when there is nothing to flush (FlushMemTable with an empty
    /// memtable): with FlushOptions::wait the background error is still returned; without wait the call succeeds.
    /// </summary>
    [TestMethod]
    public void ManualFlushOfAnEmptyMemtableReportsTheBackgroundErrorOnlyWhenWaiting()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
        DbImpl impl = db.DefaultColumnFamily.Db;
        impl.SetBackgroundError(RocksDbException.IOError("injected"), BackgroundErrorReason.Flush, walRelated: false);
        db.Flush(new FlushOptions { Wait = false });
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
        Assert.AreEqual(StatusCode.IOError, ex.Code);
    }

    /// <summary>
    /// DBImpl::SwitchMemtable (db_impl_write.cc): "if (cf->IsEmpty()) { ...; cf->mem()->SetCreationSeq(versions_->
    /// LastSequence()); }" for every column family: the empty memtable of another column family gets the last sequence
    /// as its creation sequence, so it does not look like the oldest memtable to the write buffer manager's flush pick.
    /// </summary>
    [TestMethod]
    public void MemtableSwitchAdvancesTheCreationSequenceOfEmptyColumnFamilies()
    {
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(NewFs()));
        ColumnFamily other = db.CreateColumnFamily("other", new ColumnFamilyOptions());
        DbImpl impl = db.DefaultColumnFamily.Db;
        ColumnFamilyData otherCfd = impl.GetColumnFamilyData("other");
        ulong before = otherCfd.Mem.CreationSequence;
        for (int i = 0; i < 5; i++) db.Put(B("k" + i), B("v"));
        db.Flush();
        Assert.AreEqual(before + 5, otherCfd.Mem.CreationSequence); // was: unchanged
        Assert.AreEqual(impl.Versions.LastSequence, otherCfd.Mem.CreationSequence);
        // A column family holding data keeps its memtable's creation sequence.
        other.Put(B("x"), B("y"));
        ulong pinned = otherCfd.Mem.CreationSequence;
        db.Put(B("z"), B("v"));
        db.Flush();
        Assert.AreEqual(pinned, otherCfd.Mem.CreationSequence);
    }

    /// <summary>
    /// DBImpl::BackgroundFlush: "if (!flush_req.atomic_flush &amp;&amp; ShouldRescheduleFlushRequestToRetainUDT(flush_req))":
    /// a flush is postponed to retain user-defined timestamps only when it is not atomic. The flush of an error recovery
    /// is atomic (ResumeImpl: force_atomic_flush), so a column family with timestamps in memtable only is flushed with
    /// the others. (Coverage test: it also passes without the "group is null" guard of BackgroundFlushAsync.)
    /// (The atomic_flush option itself is rejected for such a column family.)
    /// </summary>
    [TestMethod]
    public void RecoveryFlushIsNotPostponedToRetainTimestamps()
    {
        var options = new DbOptions { CreateIfMissing = true, FileSystem = NewFs(), AllowConcurrentMemtableWrite = false };
        options.Listeners.Add(new NoAutoRecoveryListener());
        using RocksDatabase db = RocksDatabase.Open(DbPath, options);
        DbImpl impl = db.DefaultColumnFamily.Db;
        ColumnFamily ts = db.CreateColumnFamily("ts", new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts, PersistUserDefinedTimestamps = false });
        byte[] five = new byte[8];
        five[0] = 5;
        ts.Put(B("k"), five, B("v"));
        db.Put(B("a"), B("1"));
        impl.SetBackgroundError(RocksDbException.NoSpace("injected"), BackgroundErrorReason.Flush, walRelated: false);
        // The recovery flush covers "ts", whose memtable holds a timestamp that is not below full_history_ts_low (an
        // automatic flush of it would be postponed).
        Task resume = Task.Run(() => db.Resume());
        Assert.IsTrue(resume.Wait(TimeSpan.FromSeconds(20)), "the recovery flush never finished");
        Assert.IsNull(db.BackgroundError);
        Assert.AreEqual("0", ts.GetProperty("rocksdb.num-immutable-mem-table"));
        Assert.AreEqual("1", ts.GetProperty("rocksdb.num-files-at-level0"));
        Assert.AreEqual("1", db.GetProperty("rocksdb.num-files-at-level0"));
    }

    private sealed class NoAutoRecoveryListener : EventListener
    {
        public override void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e) => e.AutoRecovery = false;
    }

    /// <summary>
    /// VersionSet::ProcessManifestWrites: after a failed MANIFEST write (here: the sync) "descriptor_log_.reset()" - the
    /// file is never appended to or synced again, the next edit starts a new MANIFEST with a full snapshot and points
    /// CURRENT at it, and the installed MANIFEST position is only reported for the new file.
    /// </summary>
    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void ManifestThatFailedIsNeverWrittenAgain(bool failSync)
    {
        var fs = new FaultInjectionFileSystem(NewFs());
        var options = new DbOptions { CreateIfMissing = true, FileSystem = fs };
        options.Listeners.Add(new NoAutoRecoveryListener());
        using (RocksDatabase db = RocksDatabase.Open(DbPath, options))
        {
            DbImpl impl = db.DefaultColumnFamily.Db;
            db.Put(B("a"), B("1"));
            ulong failed = impl.Versions.ManifestFileNumber;
            string failedPath = FileNames.DescriptorFileName(DbPath, failed);
            fs.FailMatching("MANIFEST", failSync ? FaultOps.Sync : FaultOps.Append, 1, InjectedFaultKind.NoSpace);
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Flush());
            Assert.AreEqual(StatusCode.IOError, ex.Code);
            Assert.IsNotNull(impl.Versions.IoStatus);
            Assert.AreEqual(failed, impl.Versions.InstalledManifest.Number); // the failed write is not reported as installed
            ulong installedSize = impl.Versions.InstalledManifest.Size;
            long sizeAfterFailure = fs.GetFileSize(failedPath);

            db.Resume();
            Assert.IsNull(impl.Versions.IoStatus);
            ulong replacement = impl.Versions.ManifestFileNumber;
            Assert.IsTrue(replacement > failed);
            Assert.AreEqual(replacement, impl.Versions.InstalledManifest.Number);
            db.Put(B("b"), B("2"));
            db.Flush();
            Assert.AreEqual(replacement, impl.Versions.ManifestFileNumber);
            // The failed MANIFEST (if the obsolete file purge has not deleted it yet) did not grow.
            if (fs.FileExists(failedPath)) Assert.AreEqual(sizeAfterFailure, fs.GetFileSize(failedPath));
            Assert.IsTrue(installedSize <= (ulong)sizeAfterFailure || !failSync);
        }
        using RocksDatabase reopened = RocksDatabase.Open(DbPath, new DbOptions { FileSystem = fs });
        Assert.AreEqual("1", GetString(reopened, "a"));
        Assert.AreEqual("2", GetString(reopened, "b"));
    }
}
