using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;

namespace RocksDb.Managed.CompatTests.Audit11;

/// <summary>
/// Audit #11 follow-up A, differentially against native RocksDB: how a GetUpdatesSince run ends
/// (WalIteratorImpl::NextImpl / SeekToStartSequence / UpdateCurrentWriteBatch, db/wal_iterator_impl.cc). The same
/// call sequence runs on both engines in a local directory; the batch sequence numbers read and the final status
/// (full message) must agree.
/// </summary>
[TestClass]
public sealed class Audit11FollowUpCompatTests
{
    private static byte[] U(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>The operations a scenario runs, on either engine.</summary>
    private interface IEngine : IDisposable
    {
        string Path { get; }
        void Put(string key, bool wal);
        void FlushWal();
        /// <summary>GetUpdatesSince(<paramref name="seq"/>), read while valid, then <paramref name="extraNexts"/> more
        /// Next calls with <paramref name="between"/> run before each: "s1,s2,...|status".</summary>
        string Updates(ulong seq, int extraNexts, Action? between);
    }

    private sealed class Native : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;

        public Native(string path, bool manualWalFlush)
        {
            Path = path;
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            RocksDbSharp.Native.Instance.rocksdb_options_set_manual_wal_flush(o.Handle, manualWalFlush ? (byte)1 : (byte)0);
            _db = RocksDbSharp.RocksDb.Open(o, path);
        }

        public string Path { get; }

        public void Put(string key, bool wal) =>
            _db.Put(U(key), U("v-" + key), null, wal ? new RocksDbSharp.WriteOptions() : new RocksDbSharp.WriteOptions().DisableWal(1));

        public void FlushWal() => RocksDbSharp.Native.Instance.rocksdb_flush_wal(_db.Handle, 0);

        public string Updates(ulong seq, int extraNexts, Action? between)
        {
            var sb = new StringBuilder();
            using RocksDbSharp.TransactionLogIterator it = _db.GetUpdatesSince(seq);
            for (; it.Valid(); it.Next())
            {
                using RocksDbSharp.WriteBatch b = it.GetBatch(out ulong s);
                sb.Append(s).Append(',');
            }
            for (int i = 0; i < extraNexts; i++)
            {
                between?.Invoke();
                it.Next();
                for (; it.Valid(); it.Next())
                {
                    using RocksDbSharp.WriteBatch b = it.GetBatch(out ulong s);
                    sb.Append(s).Append(',');
                }
            }
            try
            {
                it.Status();
                return sb + "|OK";
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                return sb + "|" + e.Message;
            }
        }

        public void Dispose() => _db.Dispose();
    }

    private sealed class Managed : IEngine
    {
        private readonly RocksDatabase _db;

        public Managed(string path, bool manualWalFlush)
        {
            Path = path;
            _db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, ManualWalFlush = manualWalFlush });
        }

        public string Path { get; }

        public void Put(string key, bool wal) => _db.Put(new WriteOptions { DisableWal = !wal }, U(key), U("v-" + key));

        public void FlushWal() => _db.FlushWal(sync: false);

        public string Updates(ulong seq, int extraNexts, Action? between)
        {
            var sb = new StringBuilder();
            using TransactionLogIterator it = _db.GetUpdatesSince(seq);
            for (; it.Valid; it.Next()) sb.Append(it.Current.Sequence).Append(',');
            for (int i = 0; i < extraNexts; i++)
            {
                between?.Invoke();
                it.Next();
                for (; it.Valid; it.Next()) sb.Append(it.Current.Sequence).Append(',');
            }
            return sb + "|" + (it.Error?.Message ?? "OK");
        }

        public void Dispose() => _db.Dispose();
    }

    /// <summary>Damages the payload of the last record of the newest WAL in <paramref name="dir"/> (a batch of
    /// <paramref name="recordBytes"/> bytes appended last), in place while the DB is open.</summary>
    private static void DamageLastRecord(string dir, long recordStart)
    {
        string wal = Directory.GetFiles(dir, "*.log").OrderBy(f => f, StringComparer.Ordinal).Last();
        using var f = new FileStream(wal, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        // 7-byte record header, 12-byte batch header, then the record: flip a byte of the key.
        f.Position = recordStart + 7 + 12 + 3;
        int b = f.ReadByte();
        f.Position = recordStart + 7 + 12 + 3;
        f.WriteByte((byte)(b ^ 0x40));
    }

    private static long NewestWalLength(string dir) =>
        new FileInfo(Directory.GetFiles(dir, "*.log").OrderBy(f => f, StringComparer.Ordinal).Last()).Length;

    private static string Run(Func<string, bool, IEngine> open, string dir, string scenario)
    {
        bool manual = scenario == "manual-flush-buffered";
        using IEngine e = open(dir, manual);
        switch (scenario)
        {
            case "caught-up":
                e.Put("a", true);
                e.Put("b", true);
                return e.Updates(1, 0, null);
            case "unlogged-tail":
                e.Put("a", true);
                e.Put("b", false);
                return e.Updates(1, 0, null);
            case "unlogged-tail-next":
                // Next again with nothing new: native keeps TryAgain (the iterator is spent), managed retries and ends
                // with TryAgain again.
                e.Put("a", true);
                e.Put("b", false);
                return e.Updates(1, 1, null);
            case "unstarted-next":
                // WalIteratorImpl::NextImpl(!started_): SeekToStartSequence finds nothing, then TryAgain.
                e.Put("a", true);
                e.Put("b", false);
                return e.Updates(2, 1, null);
            case "gap":
                e.Put("a", true);
                e.Put("b", false);
                e.Put("c", true);
                return e.Updates(1, 0, null);
            case "manual-flush-buffered":
                e.Put("a", true);
                e.FlushWal();
                e.Put("b", true);
                return e.Updates(1, 0, null);
            case "damaged-tail-record":
            {
                for (int i = 0; i < 3; i++) e.Put("k" + i, true);
                long start = NewestWalLength(e.Path);
                e.Put("k3", true);
                DamageLastRecord(e.Path, start);
                return e.Updates(1, 0, null);
            }
            default:
                throw new ArgumentException(scenario);
        }
    }

    /// <summary>
    /// WalIteratorImpl::NextImpl (db/wal_iterator_impl.cc, ~l. 266-278):
    /// <code>
    /// } else if (current_last_seq_ == versions_->LastSequence()) {
    ///   current_status_ = Status::OK();
    /// } else {
    ///   const char* msg = "Create a new iterator to fetch the new tail.";
    ///   current_status_ = Status::TryAgain(msg);
    /// }
    /// </code>
    /// Native and managed end each run the same way: caught up is OK; a run that stopped below the last sequence (an
    /// unlogged write at the end of the log, a record a manual WAL flush has not written, a damaged live record whose
    /// checksum fails, an unstarted iterator's Next) is TryAgain with the same message; a discontinuity is
    /// NotFound("Gap in sequence numbers").
    /// </summary>
    [TestMethod]
    [DataRow("caught-up")]
    [DataRow("unlogged-tail")]
    [DataRow("unlogged-tail-next")]
    [DataRow("unstarted-next")]
    [DataRow("gap")]
    [DataRow("manual-flush-buffered")]
    [DataRow("damaged-tail-record")]
    public void RunEndsLikeNative(string scenario)
    {
        using var dir = new TempDir("a11run");
        string native = Run(static (p, m) => new Native(p, m), Path.Combine(dir.Path, "n"), scenario);
        string managed = Run(static (p, m) => new Managed(p, m), Path.Combine(dir.Path, "m"), scenario);
        Console.WriteLine(scenario + ": native=" + native + " managed=" + managed);
        Assert.AreEqual(native, managed);
    }

    /// <summary>
    /// Documented deviation (managed TransactionLogIterator remarks): native's iterator is spent after TryAgain - "Next()
    /// has no effect" (include/rocksdb/wal_iterator.h, STATES 3; WalIteratorImpl::Next returns when
    /// !current_status_.ok()) - so the record a later FlushWAL writes is only seen by a new iterator. The managed
    /// iterator retries on Next and reads it.
    /// </summary>
    [TestMethod]
    public void NextAfterTryAgainRetriesUnlikeNativeWhereTheIteratorIsSpent()
    {
        using var dir = new TempDir("a11spent");
        using var native = new Native(Path.Combine(dir.Path, "n"), manualWalFlush: true);
        using var managed = new Managed(Path.Combine(dir.Path, "m"), manualWalFlush: true);
        foreach (IEngine e in new IEngine[] { native, managed })
        {
            e.Put("a", true);
            e.FlushWal();
            e.Put("b", true);
        }
        const string TryAgain = "Operation failed. Try again.: Create a new iterator to fetch the new tail.";
        Assert.AreEqual("1,|" + TryAgain, native.Updates(1, 1, native.FlushWal));
        Assert.AreEqual("1,2,|OK", managed.Updates(1, 1, managed.FlushWal));
        // A new native iterator reads it (what the managed Next does without re-reading the WAL from its start).
        Assert.AreEqual("1,2,|OK", native.Updates(1, 0, null));
    }
}
