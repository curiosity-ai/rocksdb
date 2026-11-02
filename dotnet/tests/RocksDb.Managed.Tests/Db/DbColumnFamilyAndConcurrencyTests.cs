using RocksDb.Managed.Db;
using System.Diagnostics;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Column families (db/column_family_test.cc) and concurrent writers (db/db_write_test.cc spirit).</summary>
[TestClass]
public sealed class DbColumnFamilyAndConcurrencyTests
{
    [TestMethod]
    public async Task CreateWriteReopenDropColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        var mergeCf = new ColumnFamilyOptions { MergeOperator = new UInt64AddMergeOperator() };
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs)))
        {
            ColumnFamily users = await db.CreateColumnFamilyAsync("users", new ColumnFamilyOptions());
            ColumnFamily counters = db.CreateColumnFamily("counters", mergeCf);
            Assert.AreEqual(1u, users.Id);
            Assert.AreEqual(2u, counters.Id);
            RocksDbException dup = Assert.ThrowsExactly<RocksDbException>(() => db.CreateColumnFamily("users", new ColumnFamilyOptions()));
            Assert.AreEqual(StatusCode.InvalidArgument, dup.Code);

            db.Put(B("k"), B("default"));
            users.Put(B("k"), B("users"));
            await users.PutAsync(B("k2"), B("users2"));
            for (int i = 0; i < 10; i++) counters.Merge(B("c"), BitConverter.GetBytes((ulong)i));
            Assert.AreEqual(45UL, BitConverter.ToUInt64(counters.Get(B("c"))!));
            using (WriteBatch b = db.CreateWriteBatch())
            {
                b.Put(users.Id, B("batch"), B("u")).Merge(counters.Id, B("c"), BitConverter.GetBytes(5UL)).Put(B("batch"), B("d"));
                await db.WriteAsync(b);
            }
            await users.FlushAsync();
            Assert.AreEqual("default", GetString(db, "k"));
            Assert.AreEqual("users", GetString(users, "k"));
            Assert.AreEqual("u", GetString(users, "batch"));
            Assert.AreEqual(50UL, BitConverter.ToUInt64(counters.Get(B("c"))!));
            CollectionAssert.AreEqual(new[] { "default", "users", "counters" }, RocksDatabase.ListColumnFamilies(DbPath, fs));
        }

        // Opening without listing every CF fails.
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs)));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);

        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("users", new ColumnFamilyOptions()), new("counters", mergeCf)];
        int liveAfterDrop;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs))
        {
            ColumnFamily users = db.GetColumnFamily("users");
            ColumnFamily counters = db.GetColumnFamily("counters");
            Assert.AreEqual("users2", GetString(users, "k2"));
            Assert.AreEqual("d", GetString(db, "batch"));
            Assert.AreEqual(50UL, BitConverter.ToUInt64(counters.Get(B("c"))!));
            db.DropColumnFamily(users);
            Assert.IsTrue(users.IsDropped);
            RocksDbException d = Assert.ThrowsExactly<RocksDbException>(() => users.Get(B("k")));
            Assert.AreEqual(StatusCode.ColumnFamilyDropped, d.Code);
            Assert.IsFalse(db.TryGetColumnFamily("users", out _));
            Assert.AreEqual(2, db.ColumnFamilies.Count);
            RocksDbException dropDefault = Assert.ThrowsExactly<RocksDbException>(() => db.DropColumnFamily(db.DefaultColumnFamily));
            Assert.AreEqual(StatusCode.InvalidArgument, dropDefault.Code);
            liveAfterDrop = db.GetLiveFilesMetadata().Count;
            Assert.IsFalse(db.GetLiveFilesMetadata().Exists(f => f.ColumnFamilyName == "users"));
        }
        // The dropped CF's table files were deleted.
        Assert.AreEqual(liveAfterDrop, CountFiles(fs, ".sst"));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), [new("default", new ColumnFamilyOptions()), new("counters", mergeCf)]))
        {
            Assert.AreEqual(50UL, BitConverter.ToUInt64(db.GetColumnFamily("counters").Get(B("c"))!));
            CollectionAssert.AreEqual(new[] { "default", "counters" }, RocksDatabase.ListColumnFamilies(DbPath, fs));
        }
    }

    [TestMethod]
    public void MissingColumnFamilies()
    {
        MemoryFileSystem fs = NewFs();
        ColumnFamilyDescriptor[] cfs = [new("default", new ColumnFamilyOptions()), new("a", new ColumnFamilyOptions())];
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => RocksDatabase.Open(DbPath, NewOptions(fs), cfs));
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        DbOptions o = NewOptions(fs);
        o.CreateMissingColumnFamilies = true;
        using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfs)) db.GetColumnFamily("a").Put(B("x"), B("y"));
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfs)) Assert.AreEqual("y", GetString(db.GetColumnFamily("a"), "x"));
    }

    private static int CountFiles(MemoryFileSystem fs, string ext)
    {
        int n = 0;
        foreach (string f in fs.GetChildren(DbPath)) if (f.EndsWith(ext, StringComparison.Ordinal)) n++;
        return n;
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task ConcurrentWritersSyncAndAsync()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 256 * 1024, ArenaBlockSize = 16 * 1024, MaxWriteBufferNumber = 3 };
        const int threads = 8;
        var counts = new int[threads];
        await using (RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), cfo))
        {
            var sw = Stopwatch.StartNew();
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int id = t;
                tasks[t] = id % 2 == 0
                    ? Task.Run(async () =>
                    {
                        int i = 0;
                        while (sw.ElapsedMilliseconds < 1500)
                        {
                            await db.PutAsync(new WriteOptions { Sync = i % 50 == 0 }, B("t" + id + "-" + i), Value(i, id));
                            i++;
                        }
                        counts[id] = i;
                    })
                    : Task.Factory.StartNew(() =>
                    {
                        int i = 0;
                        while (sw.ElapsedMilliseconds < 1500)
                        {
                            db.Put(B("t" + id + "-" + i), Value(i, id));
                            i++;
                        }
                        counts[id] = i;
                    }, TaskCreationOptions.LongRunning);
            }
            await Task.WhenAll(tasks);
            ulong total = 0;
            for (int t = 0; t < threads; t++)
            {
                total += (ulong)counts[t];
                for (int i = 0; i < counts[t]; i += 1 + counts[t] / 200)
                {
                    CollectionAssert.AreEqual(Value(i, t), db.Get(B("t" + t + "-" + i)), "t" + t + " i" + i);
                }
            }
            Assert.AreEqual(total, db.GetLatestSequenceNumber());
        }
        using (RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo))
        {
            for (int t = 0; t < threads; t++)
            {
                for (int i = 0; i < counts[t]; i += 1 + counts[t] / 100)
                {
                    CollectionAssert.AreEqual(Value(i, t), db.Get(B("t" + t + "-" + i)), "t" + t + " i" + i);
                }
            }
        }
    }

    [TestMethod]
    [Timeout(60_000, CooperativeCancellation = true)]
    public async Task ConcurrentReadersDuringWritesAndFlushes()
    {
        MemoryFileSystem fs = NewFs();
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 64 * 1024, ArenaBlockSize = 8 * 1024, MaxWriteBufferNumber = 4 };
        await using RocksDatabase db = await RocksDatabase.OpenAsync(DbPath, NewOptions(fs), cfo);
        const int n = 20000;
        int written = 0;
        Task writer = Task.Run(() =>
        {
            for (int i = 0; i < n; i++)
            {
                db.Put(Key(i), Value(i, 0));
                Volatile.Write(ref written, i + 1);
            }
        });
        var readers = new Task[3];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r;
            readers[r] = Task.Run(() =>
            {
                var rnd = new Random(seed);
                while (Volatile.Read(ref written) < n)
                {
                    int w = Volatile.Read(ref written);
                    if (w == 0) continue;
                    int i = rnd.Next(w);
                    CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)), "key " + i);
                }
            });
        }
        await Task.WhenAll(readers);
        await writer;
        for (int i = 0; i < n; i += 97) CollectionAssert.AreEqual(Value(i, 0), db.Get(Key(i)));
    }
}
