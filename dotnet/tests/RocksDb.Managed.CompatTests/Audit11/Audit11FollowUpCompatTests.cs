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
        /// <summary>Flushes the memtable (switches the WAL).</summary>
        void Flush();
        /// <summary>GetUpdatesSince(<paramref name="seq"/>), read while valid, then <paramref name="extraNexts"/> more
        /// Next calls with <paramref name="between"/> run before each: "s1,s2,...|status".</summary>
        string Updates(ulong seq, int extraNexts, Action? between);
    }

    private sealed class Native : IEngine
    {
        private readonly RocksDbSharp.RocksDb _db;

        public Native(string path, bool manualWalFlush) : this(path, manualWalFlush, recycle: false)
        {
        }

        public Native(string path, bool manualWalFlush, bool recycle)
        {
            Path = path;
            var o = new RocksDbSharp.DbOptions().SetCreateIfMissing(true);
            RocksDbSharp.Native.Instance.rocksdb_options_set_manual_wal_flush(o.Handle, manualWalFlush ? (byte)1 : (byte)0);
            if (recycle) RocksDbSharp.Native.Instance.rocksdb_options_set_recycle_log_file_num(o.Handle, (UIntPtr)2);
            _db = RocksDbSharp.RocksDb.Open(o, path);
        }

        public string Path { get; }

        public void Put(string key, bool wal) =>
            _db.Put(U(key), U("v-" + key), null, wal ? new RocksDbSharp.WriteOptions() : new RocksDbSharp.WriteOptions().DisableWal(1));

        public void FlushWal() => RocksDbSharp.Native.Instance.rocksdb_flush_wal(_db.Handle, 0);

        public void Flush() => _db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));

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

        public Managed(string path, bool manualWalFlush) : this(path, manualWalFlush, recycle: false)
        {
        }

        public Managed(string path, bool manualWalFlush, bool recycle)
        {
            Path = path;
            _db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true, ManualWalFlush = manualWalFlush, RecycleLogFileNum = recycle ? 2UL : 0 });
        }

        public string Path { get; }

        public void Put(string key, bool wal) => _db.Put(new WriteOptions { DisableWal = !wal }, U(key), U("v-" + key));

        public void FlushWal() => _db.FlushWal(sync: false);

        public void Flush()
        {
            _db.Flush();
            _db.WaitForCompact();
        }

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

    private static string NewestWal(string dir) => Directory.GetFiles(dir, "*.log").OrderBy(f => f, StringComparer.Ordinal).Last();

    /// <summary>(offset, header size, payload length) of the records of this incarnation of a WAL (legacy, or recyclable
    /// with the file's own log number).</summary>
    private static List<(long Offset, int Header, int Length)> LiveRecords(string walPath)
    {
        uint logNumber = uint.Parse(Path.GetFileNameWithoutExtension(walPath), System.Globalization.CultureInfo.InvariantCulture);
        byte[] log;
        using (var f = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            log = new byte[f.Length];
            f.ReadExactly(log);
        }
        var list = new List<(long, int, int)>();
        bool? recycled = null;
        long off = 0;
        while (off + 7 <= log.Length)
        {
            int inBlock = (int)(off % 32768);
            int type = log[off + 6];
            int length = log[off + 4] | (log[off + 5] << 8);
            int header = type is >= 5 and <= 8 or 11 or 131 ? 11 : 7;
            if (32768 - inBlock < header)
            {
                off += 32768 - inBlock;
                continue;
            }
            recycled ??= header == 11;
            if ((type == 0 && length == 0) || (header == 11) != recycled) break;
            if (header == 11 && System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(log.AsSpan((int)off + 7)) != logNumber) break;
            list.Add((off, header, length));
            off += header + length;
        }
        return list;
    }

    private static void Patch(string walPath, long offset, Func<int, int> change)
    {
        using var f = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        f.Position = offset;
        int b = f.ReadByte();
        f.Position = offset;
        f.WriteByte((byte)change(b));
    }

    private static string Run(Func<string, bool, bool, IEngine> open, string dir, string scenario)
    {
        bool manual = scenario is "manual-flush-buffered" or "manual-flush-start-after-switch";
        bool recycle = scenario == "recycled-damaged-record";
        using IEngine e = open(dir, manual, recycle);
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
            case "manual-flush-start-after-switch":
                // The new WAL holds only the buffered record (not on disk: not listed). Native's iterator is OK and not
                // valid; its Next would dereference the missing log reader (no file), so it is not called.
                e.Put("a", true);
                e.FlushWal();
                e.Flush();
                e.Put("b", true);
                return e.Updates(2, 0, null);
            case "recycled-damaged-record":
            {
                // A recycled WAL ends at a bad checksum (FragmentBufferedReader: kBadRecordChecksum with recycled_ returns
                // false, no report): the records written after the damaged one are not read.
                for (int round = 0; round < 3; round++)
                {
                    for (int i = 0; i < 20; i++) e.Put("old" + i, true);
                    e.Flush();
                }
                for (int i = 0; i < 6; i++) e.Put("k" + i, true);
                string wal = NewestWal(e.Path);
                var live = LiveRecords(wal);
                if (live[^1].Header != 11) return "not a recycled WAL";
                Patch(wal, live[^3].Offset + live[^3].Header + 12 + 3, static b => b ^ 0x40);
                return e.Updates(61, 0, null);
            }
            case "length-past-end":
            {
                // FragmentBufferedReader::TryReadFragment waits for the rest of a record whose length reaches past the
                // end of the data.
                for (int i = 0; i < 4; i++) e.Put("k" + i, true);
                string wal = NewestWal(e.Path);
                var last = LiveRecords(wal)[^1];
                int grown = last.Length + 200;
                Patch(wal, last.Offset + 4, _ => grown & 0xff);
                Patch(wal, last.Offset + 5, _ => grown >> 8);
                return e.Updates(1, 0, null);
            }
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
    [DataRow("manual-flush-start-after-switch")]
    [DataRow("recycled-damaged-record")]
    [DataRow("length-past-end")]
    public void RunEndsLikeNative(string scenario)
    {
        using var dir = new TempDir("a11run");
        string native = Run(static (p, m, r) => new Native(p, m, r), Path.Combine(dir.Path, "n"), scenario);
        string managed = Run(static (p, m, r) => new Managed(p, m, r), Path.Combine(dir.Path, "m"), scenario);
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
