using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>
/// ReadOptions that change what a read may do: read_tier (the Get / MultiGet / iterator x tier x data location matrix of
/// db_test.cc / db_iterator_test.cc ReadTier tests), tailing (db/db_tailing_iter_test.cc), value_size_soft_limit
/// (db_basic_test.cc MultiGet soft limit tests), verify_checksums and deadline.
/// </summary>
[TestClass]
public sealed class DbReadOptionsTests
{
    private enum Op { Get, GetAsync, MultiGet, Iterator }

    private enum Outcome { Found, NotFound, Incomplete, NotSupported }

    /// <summary>
    /// A DB (reopened, so no data block is cached) with "sst" in a table file, "mem" only in the memtable (written with
    /// the WAL) and, when <paramref name="unpersisted"/>, "nowal" written with DisableWal.
    /// </summary>
    private static RocksDatabase OpenMatrixDb(MemoryFileSystem fs, bool unpersisted)
    {
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs)))
        {
            db.Put(B("sst"), B("s"));
            db.Put(B("sst2"), B("s2"));
            db.Flush();
        }
        RocksDatabase reopened = RocksDatabase.Open(DbPath, NewOptions(fs));
        reopened.Put(B("mem"), B("m"));
        if (unpersisted) reopened.Put(new WriteOptions { DisableWal = true }, B("nowal"), B("n"));
        return reopened;
    }

    private static Outcome Read(RocksDatabase db, Op op, ReadTier tier, string key)
    {
        var ro = new ReadOptions { ReadTier = tier };
        try
        {
            switch (op)
            {
                case Op.Get:
                    return db.Get(ro, B(key)) is null ? Outcome.NotFound : Outcome.Found;
                case Op.GetAsync:
                    return db.GetAsync(ro, B(key)).AsTask().GetAwaiter().GetResult() is null ? Outcome.NotFound : Outcome.Found;
                case Op.MultiGet:
                {
                    using var batch = new MultiGetBatch();
                    batch.Add(B(key)).Add(B("mem"));
                    db.MultiGet(ro, batch);
                    if (tier != ReadTier.PersistedTier) Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(1), "memtable key of the batch");
                    return batch.GetStatus(0) switch
                    {
                        MultiGetStatus.Found => Outcome.Found,
                        MultiGetStatus.NotFound => Outcome.NotFound,
                        _ => batch.GetError(0)!.Code == StatusCode.Incomplete ? Outcome.Incomplete : throw batch.GetError(0)!,
                    };
                }
                default:
                {
                    using Iterator it = db.NewIterator(ro);
                    it.Seek(B(key));
                    return it.Valid && it.Key.SequenceEqual(B(key)) ? Outcome.Found : Outcome.NotFound;
                }
            }
        }
        catch (RocksDbException e) when (e.Code is StatusCode.Incomplete or StatusCode.NotSupported)
        {
            return e.Code == StatusCode.Incomplete ? Outcome.Incomplete : Outcome.NotSupported;
        }
    }

    [TestMethod]
    public void ReadTierMatrix()
    {
        // (op, tier, key) -> outcome; "sst" is in an uncached table block, "mem" in the memtable, "nowal" in the
        // memtable without the WAL, "zzz" past every file (no table is consulted).
        (Op Op, ReadTier Tier, string Key, Outcome Expected)[] cases =
        [
            (Op.Get, ReadTier.ReadAllTier, "sst", Outcome.Found),
            (Op.Get, ReadTier.BlockCacheTier, "sst", Outcome.Incomplete),
            (Op.Get, ReadTier.BlockCacheTier, "mem", Outcome.Found),
            (Op.Get, ReadTier.BlockCacheTier, "zzz", Outcome.NotFound),
            (Op.Get, ReadTier.MemtableTier, "sst", Outcome.Found), // point lookups are not restricted
            (Op.Get, ReadTier.MemtableTier, "mem", Outcome.Found),
            (Op.Get, ReadTier.PersistedTier, "sst", Outcome.Found),
            (Op.Get, ReadTier.PersistedTier, "mem", Outcome.NotFound), // unpersisted data exists: memtables skipped
            (Op.Get, ReadTier.PersistedTier, "nowal", Outcome.NotFound),
            (Op.GetAsync, ReadTier.BlockCacheTier, "sst", Outcome.Incomplete),
            (Op.GetAsync, ReadTier.BlockCacheTier, "mem", Outcome.Found),
            (Op.GetAsync, ReadTier.MemtableTier, "sst", Outcome.Found),
            (Op.GetAsync, ReadTier.PersistedTier, "mem", Outcome.NotFound),
            (Op.MultiGet, ReadTier.ReadAllTier, "sst", Outcome.Found),
            (Op.MultiGet, ReadTier.BlockCacheTier, "sst", Outcome.Incomplete),
            (Op.MultiGet, ReadTier.BlockCacheTier, "zzz", Outcome.NotFound),
            (Op.MultiGet, ReadTier.MemtableTier, "sst", Outcome.Found),
            (Op.MultiGet, ReadTier.PersistedTier, "sst", Outcome.Found),
            (Op.MultiGet, ReadTier.PersistedTier, "nowal", Outcome.NotFound),
            (Op.Iterator, ReadTier.ReadAllTier, "sst", Outcome.Found),
            (Op.Iterator, ReadTier.BlockCacheTier, "sst", Outcome.Incomplete),
            (Op.Iterator, ReadTier.MemtableTier, "sst", Outcome.NotFound), // iterators skip the table files
            (Op.Iterator, ReadTier.MemtableTier, "mem", Outcome.Found),
            (Op.Iterator, ReadTier.PersistedTier, "sst", Outcome.NotSupported),
        ];
        foreach ((Op op, ReadTier tier, string key, Outcome expected) in cases)
        {
            MemoryFileSystem fs = NewFs();
            using RocksDatabase db = OpenMatrixDb(fs, unpersisted: true);
            Assert.AreEqual(expected, Read(db, op, tier, key), op + " " + tier + " " + key);
        }
    }

    [TestMethod]
    public void BlockCacheTierSucceedsOnceTheBlockIsCached()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMatrixDb(fs, unpersisted: false);
        var noIo = new ReadOptions { ReadTier = ReadTier.BlockCacheTier };
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => db.Get(noIo, B("sst")));
        Assert.AreEqual(StatusCode.Incomplete, ex.Code);
        // A no-I/O read does not fill the cache either.
        Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => db.Get(noIo, B("sst"))).Code);
        Assert.AreEqual("s", GetString(db, "sst"));
        Assert.AreEqual("s", S(db.Get(noIo, B("sst"))));
        Assert.AreEqual("s2", S(db.Get(noIo, B("sst2")))); // same block
        using Iterator it = db.NewIterator(noIo);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        Assert.AreEqual(3, n);
        // Without WAL-disabled writes, kPersistedTier reads the memtables.
        Assert.AreEqual("m", S(db.Get(new ReadOptions { ReadTier = ReadTier.PersistedTier }, B("mem"))));
    }

    [TestMethod]
    public void TailingIteratorSeesLaterWrites()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        db.Put(B("a"), B("1"));
        using Snapshot snap = db.GetSnapshot();
        using Iterator it = db.NewIterator(new ReadOptions { Tailing = true, Snapshot = snap });
        it.SeekToFirst();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("a", S(it.Key.ToArray()));
        it.Next();
        Assert.IsFalse(it.Valid);

        // Written after the iterator (and its snapshot): visible to the next Seek.
        db.Put(B("b"), B("2"));
        it.Seek(B("a"));
        it.Next();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("b", S(it.Key.ToArray()));

        // After a flush (new super version) the iterator renews itself on Next and on Seek.
        db.Put(B("c"), B("3"));
        db.Flush();
        db.Put(B("d"), B("4"));
        it.Next();
        Assert.IsTrue(it.Valid);
        Assert.AreEqual("c", S(it.Key.ToArray()));
        it.Next();
        Assert.AreEqual("d", S(it.Key.ToArray()));
        db.CompactRange(null, null);
        db.Put(B("e"), B("5"));
        it.Seek(B("d"));
        it.Next();
        Assert.AreEqual("e", S(it.Key.ToArray()));
        Assert.AreEqual("5", S(it.Value.ToArray()));

        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(it.Prev).Code);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(it.SeekToLast).Code);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => it.SeekForPrev(B("c"))).Code);

        // A regular iterator does not see later writes.
        using Iterator regular = db.NewIterator();
        db.Put(B("f"), B("6"));
        regular.Seek(B("f"));
        Assert.IsFalse(regular.Valid);
        it.Seek(B("f"));
        Assert.IsTrue(it.Valid);
    }

    [TestMethod]
    public void MultiGetValueSizeSoftLimitAbortsRemainingKeys()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
        // MultiGetBatchedValueSizeInMemory / MultiGetBatchedValueSize (db_basic_test.cc): 10-byte values.
        for (int i = 0; i < 6; i++) db.Put(B("k" + i), B(new string((char)('a' + i), 10)));
        for (int pass = 0; pass < 2; pass++)
        {
            using var batch = new MultiGetBatch();
            for (int i = 0; i < 6; i++) batch.Add(B("k" + i));
            batch.Add(B("missing"));
            db.MultiGet(new ReadOptions { ValueSizeSoftLimit = 25 }, batch);
            // k0 (10), k1 (20), k2 (30 > 25: still returned), then the rest is Aborted.
            for (int i = 0; i < 3; i++) Assert.AreEqual(MultiGetStatus.Found, batch.GetStatus(i), "pass " + pass + " key " + i);
            for (int i = 3; i < 7; i++)
            {
                Assert.AreEqual(MultiGetStatus.Error, batch.GetStatus(i), "pass " + pass + " key " + i);
                Assert.AreEqual(StatusCode.Aborted, batch.GetError(i)!.Code);
            }
            db.MultiGet(batch);
            Assert.AreEqual(6, batch.FoundCount);
            db.Flush(); // second pass: the values come from a table file
        }
    }

    [TestMethod]
    public void VerifyChecksumsIsHonouredPerRead()
    {
        string dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-verify-" + Guid.NewGuid().ToString("N"));
        try
        {
            var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.None };
            using (RocksDatabase db = RocksDatabase.Open(dir, new DbOptions { CreateIfMissing = true }, cfo))
            {
                db.Put(B("key"), B("VALUE-MARKER-0123456789"));
                db.Flush();
            }
            string sst = Directory.GetFiles(dir, "*.sst").Single();
            byte[] bytes = File.ReadAllBytes(sst);
            int at = bytes.AsSpan().IndexOf("VALUE-MARKER"u8);
            Assert.IsTrue(at > 0);
            bytes[at] = (byte)'X';
            File.WriteAllBytes(sst, bytes);
            using RocksDatabase reopened = RocksDatabase.Open(dir, new DbOptions(), cfo);
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => reopened.Get(B("key"))).Code);
            var noVerify = new ReadOptions { VerifyChecksums = false, FillCache = false };
            Assert.AreEqual("XALUE-MARKER-0123456789", S(reopened.Get(noVerify, B("key"))));
            using var batch = new MultiGetBatch();
            batch.Add(B("key"));
            reopened.MultiGet(noVerify, batch);
            Assert.AreEqual("XALUE-MARKER-0123456789", S(batch.GetValue(0).ToArray()));
            using (Iterator it = reopened.NewIterator(noVerify))
            {
                it.SeekToFirst();
                Assert.AreEqual("XALUE-MARKER-0123456789", S(it.Value.ToArray()));
            }
            // Verification is still on for the default read.
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => reopened.Get(B("key"))).Code);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [TestMethod]
    public void DeadlineTimesOutFileReads()
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = OpenMatrixDb(fs, unpersisted: false);
        var expired = new ReadOptions { Deadline = TimeSpan.FromMicroseconds(1) };
        // Memtable reads need no I/O.
        Assert.AreEqual("m", S(db.Get(expired, B("mem"))));
        Assert.AreEqual(StatusCode.TimedOut, Assert.ThrowsExactly<RocksDbException>(() => db.Get(expired, B("sst"))).Code);
        using (Iterator it = db.NewIterator(expired))
        {
            Assert.AreEqual(StatusCode.TimedOut, Assert.ThrowsExactly<RocksDbException>(() => it.Seek(B("sst"))).Code);
        }
        // MultiGet checks the deadline before looking up any key.
        using var batch = new MultiGetBatch();
        batch.Add(B("mem")).Add(B("sst"));
        db.MultiGet(expired, batch);
        Assert.AreEqual(StatusCode.TimedOut, batch.GetError(0)!.Code);
        Assert.AreEqual(StatusCode.TimedOut, batch.GetError(1)!.Code);

        // Cached blocks are served after the deadline; a future deadline does not interfere.
        Assert.AreEqual("s", GetString(db, "sst"));
        Assert.AreEqual("s", S(db.Get(expired, B("sst"))));
        var future = new ReadOptions { Deadline = DateTimeOffset.UtcNow.AddHours(1) - DateTimeOffset.UnixEpoch, IoTimeout = TimeSpan.FromSeconds(10) };
        Assert.AreEqual("s2", S(db.Get(future, B("sst2"))));
        db.MultiGet(future, batch);
        Assert.AreEqual(2, batch.FoundCount);
        // The deadline of one read does not leak into later reads on the thread.
        db.Put(B("x"), B("y"));
        db.Flush();
        Assert.AreEqual("y", GetString(db, "x"));
    }
}
