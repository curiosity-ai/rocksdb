using System.Text;
using RocksDb.Managed.Env;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Features;

/// <summary>
/// LazyWideColumns::MultiResolve / LazyWideColumnsBatch::MultiResolve (db/wide/db_lazy_entity_test.cc
/// MultiResolve cases): batched reads of lazily resolved blob columns, within one result and across keys.
/// </summary>
[TestClass]
public sealed class LazyMultiResolveTests
{
    private static WideColumn C(string name, string value) => new(B(name), B(value));

    private static string Large(int n, char c) => new(c, n);

    private static string Str(ReadOnlyMemory<byte> m) => Encoding.UTF8.GetString(m.Span);

    private static (RocksDatabase Db, Statistics Stats) OpenBlobDb()
    {
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        var cfo = new ColumnFamilyOptions { EnableBlobFiles = true, MinBlobSize = 32 };
        return (RocksDatabase.Open(DbPath, o, cfo), st);
    }

    [TestMethod]
    public void SingleResultResolvesManyReadsWithOneFetchPerColumn()
    {
        (RocksDatabase db, Statistics st) = OpenBlobDb();
        using (db)
        {
            string big = Large(100, 'b') + "0123456789" + Large(90, 'e');
            db.PutEntity(B("k"), [C("", "default"), C("big", big), C("z", Large(50, 'z'))]);
            db.Flush();
            using var lazy = new LazyWideColumns();
            Assert.IsTrue(db.DefaultColumnFamily.TryGetEntityLazy(null, B("k"), lazy));
            int bigIdx = lazy.IndexOf("big"u8), zIdx = lazy.IndexOf("z"u8);
            LazyColumnReadRequest[] reads =
            [
                new(0),                                   // inline default column
                new(zIdx),                                // whole blob
                new(zIdx, 10, 5),                         // range of the now cached blob: no further I/O
                new(bigIdx, 100, 10),                     // partial read of an uncompressed blob
                new(bigIdx, 195, LazyColumnReadRequest.WholeColumn), // clamped partial tail
                new(bigIdx, 1000, 10),                    // past the end: empty
                new(7),                                   // not a column of this result
            ];
            lazy.MultiResolve(reads);
            Assert.AreEqual("default", Str(reads[0].Result));
            Assert.AreEqual(Large(50, 'z'), Str(reads[1].Result));
            Assert.AreEqual(Large(5, 'z'), Str(reads[2].Result));
            Assert.AreEqual("0123456789", Str(reads[3].Result));
            Assert.AreEqual(Large(5, 'e'), Str(reads[4].Result));
            Assert.IsTrue(reads[5].Succeeded);
            Assert.AreEqual(0, reads[5].Result.Length);
            Assert.IsFalse(reads[6].Succeeded);
            Assert.AreEqual(StatusCode.InvalidArgument, reads[6].Error!.Code);
            for (int i = 0; i < 6; i++) Assert.IsTrue(reads[i].Succeeded, i.ToString());
            Assert.AreEqual(2UL, st.GetTickerCount(Ticker.BlobDbLazyPartialReadCount));
            // The whole fetch of z happened once
            ulong whole = st.GetTickerCount(Ticker.BlobDbLazyReadCount);
            lazy.MultiResolve(new LazyColumnReadRequest[] { new(zIdx), new(zIdx, 1, 1) });
            Assert.AreEqual(whole, st.GetTickerCount(Ticker.BlobDbLazyReadCount));
        }
    }

    [TestMethod]
    public void BatchResolvesAcrossKeysDeduplicatingWholeFetches()
    {
        (RocksDatabase db, Statistics st) = OpenBlobDb();
        using (db)
        {
            for (int i = 0; i < 6; i++)
            {
                db.PutEntity(Key(i), [C("a", Large(40 + i, (char)('a' + i))), C("b", Large(60 + i, (char)('A' + i))), C("s", "small" + i)]);
            }
            db.Flush();
            using var keys = new MultiGetBatch();
            keys.Add(Key(3)).Add(B("missing")).Add(Key(0)).Add(Key(5)).Add(Key(1));
            using var result = new LazyWideColumnsBatch();
            db.DefaultColumnFamily.MultiGetEntityLazy(null, keys, result);
            ulong before = st.GetTickerCount(Ticker.BlobDbLazyReadCount);

            var reads = new List<LazyColumnReadRequest>();
            // Whole reads, several per column (deduplicated), in shuffled key order
            int[] entries = [0, 2, 3, 4];
            int[] keyOf = [3, -1, 0, 5, 1];
            foreach (int e in entries)
            {
                reads.Add(new LazyColumnReadRequest(e, 0, 0, LazyColumnReadRequest.WholeColumn));
                reads.Add(new LazyColumnReadRequest(e, 0, 2, 3));
                reads.Add(new LazyColumnReadRequest(e, 2, 0, LazyColumnReadRequest.WholeColumn)); // inline
            }
            // A partial read of b of entry 3 and a force-verified read of b of entry 4
            reads.Add(new LazyColumnReadRequest(3, 1, 10, 4));
            reads.Add(new LazyColumnReadRequest(4, 1, 0, 2) { ForceVerify = true });
            reads.Add(new LazyColumnReadRequest(4, 1, 5, 2));
            // Invalid targets: a not-found entry, an entry out of range, a column out of range
            reads.Add(new LazyColumnReadRequest(1, 0, 0, LazyColumnReadRequest.WholeColumn));
            reads.Add(new LazyColumnReadRequest(9, 0, 0, LazyColumnReadRequest.WholeColumn));
            reads.Add(new LazyColumnReadRequest(0, 3, 0, LazyColumnReadRequest.WholeColumn));
            LazyColumnReadRequest[] arr = reads.ToArray();
            result.MultiResolve(arr);

            int r = 0;
            foreach (int e in entries)
            {
                int k = keyOf[e];
                Assert.AreEqual(Large(40 + k, (char)('a' + k)), Str(arr[r++].Result), "entry " + e);
                Assert.AreEqual(Large(3, (char)('a' + k)), Str(arr[r++].Result));
                Assert.AreEqual("small" + k, Str(arr[r++].Result));
            }
            Assert.AreEqual(Large(4, (char)('A' + 5)), Str(arr[r++].Result));
            Assert.AreEqual(Large(2, (char)('A' + 1)), Str(arr[r++].Result));
            Assert.AreEqual(Large(2, (char)('A' + 1)), Str(arr[r++].Result));
            for (; r < arr.Length; r++) Assert.AreEqual(StatusCode.InvalidArgument, arr[r].Error!.Code, r.ToString());
            // One whole fetch per distinct (entry, column): 4 x "a" + entry 4's verified "b", plus the one partial read.
            Assert.AreEqual(before + 6, st.GetTickerCount(Ticker.BlobDbLazyReadCount));
            Assert.AreEqual(1UL, st.GetTickerCount(Ticker.BlobDbLazyPartialReadCount));
        }
    }

    [TestMethod]
    public void BatchReportsPerReadFailures()
    {
        (RocksDatabase db, _) = OpenBlobDb();
        using (db)
        {
            db.PutEntity(B("k1"), [C("v", Large(40, 'x')), C("i", "inline")]);
            db.PutEntity(B("k2"), [C("v", Large(40, 'y'))]);
            db.Flush();
            using var keys = new MultiGetBatch();
            keys.Add(B("k1")).Add(B("k2"));
            using var result = new LazyWideColumnsBatch();
            // Caches the table block (the lazy lookup reads no blob).
            db.DefaultColumnFamily.MultiGetEntityLazy(null, keys, result);
            result.Reset();
            // Block cache tier: blob reads that would need I/O fail with Incomplete, inline reads succeed.
            db.DefaultColumnFamily.MultiGetEntityLazy(new ReadOptions { ReadTier = ReadTier.BlockCacheTier }, keys, result);
            int v1 = result[0].IndexOf("v"u8), i1 = result[0].IndexOf("i"u8);
            LazyColumnReadRequest[] reads =
            [
                new(0, v1, 0, LazyColumnReadRequest.WholeColumn),
                new(0, i1, 0, LazyColumnReadRequest.WholeColumn),
                new(1, 0, 3, 4),
            ];
            result.MultiResolve(reads);
            Assert.AreEqual(StatusCode.Incomplete, reads[0].Error!.Code);
            Assert.AreEqual("inline", Str(reads[1].Result));
            Assert.AreEqual(StatusCode.Incomplete, reads[2].Error!.Code);
            result.Reset();
            Assert.ThrowsExactly<ObjectDisposedException>(() =>
            {
                result.Dispose();
                result.MultiResolve(reads);
            });
        }
    }
}
