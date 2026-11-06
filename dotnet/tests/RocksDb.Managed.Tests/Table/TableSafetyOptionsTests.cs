using System.Buffers;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// BlockBasedTableOptions verify_compression, detect_filter_construct_corruption and read_amp_bytes_per_bit
/// (DBTest2.ReadAmpBitmap of db_etc2_test.cc, DBFilterConstructionCorruptionTestWithParam of db_bloom_filter_test.cc).
/// </summary>
[TestClass]
public sealed class TableSafetyOptionsTests
{
    [TestMethod]
    public void VerifyCompressionRoundTripsEveryBlock()
    {
        foreach (Managed.Compression.CompressionType type in new[] { Managed.Compression.CompressionType.Snappy, Managed.Compression.CompressionType.LZ4,
                     Managed.Compression.CompressionType.Zstd })
        {
            MemoryFileSystem fs = NewFs();
            var cfo = new ColumnFamilyOptions { Compression = type };
            cfo.TableOptions.VerifyCompression = true;
            cfo.TableOptions.EnableIndexCompression = true;
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            for (int i = 0; i < 2000; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            db.CompactRange(null, null);
            for (int i = 0; i < 2000; i += 97) Assert.AreEqual(S(Value(i, 1)), S(db.Get(Key(i))), type.ToString());
        }
    }

    /// <summary>A filter policy whose filters match nothing: what a corrupted filter construction looks like.</summary>
    private sealed class BrokenFilterPolicy : FilterPolicy
    {
        public override string CompatibilityName => "test.BrokenFilter";
        public override IFilterBitsBuilder CreateBuilder(int formatVersion) => new Builder();
        public override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents) => new Reader();

        private sealed class Builder : IFilterBitsBuilder
        {
            private int _n;
            public int EstimatedEntries => _n;
            public void AddKey(ReadOnlySpan<byte> key) => _n++;
            public void Finish(IBufferWriter<byte> output)
            {
                output.Write(new byte[] { 0, 0, 0, 0 });
                _n = 0;
            }
            public void Dispose()
            {
            }
        }

        private sealed class Reader : IFilterBitsReader
        {
            public bool MayMatch(ReadOnlySpan<byte> key) => false;
        }
    }

    [TestMethod]
    public void DetectFilterConstructCorruption()
    {
        // A sound filter passes the post-verification (full and partitioned filters).
        foreach (bool partitioned in new[] { false, true })
        {
            MemoryFileSystem fs = NewFs();
            var cfo = new ColumnFamilyOptions();
            cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
            cfo.TableOptions.DetectFilterConstructCorruption = true;
            if (partitioned)
            {
                cfo.TableOptions.IndexType = IndexType.TwoLevelIndexSearch;
                cfo.TableOptions.PartitionFilters = true;
                cfo.TableOptions.MetadataBlockSize = 256;
            }
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            for (int i = 0; i < 1000; i++) db.Put(Key(i), Value(i, 1));
            db.Flush();
            Assert.AreEqual(S(Value(7, 1)), S(db.Get(Key(7))));
        }

        // A filter that does not contain its keys fails the flush with Corruption ... but only with the option.
        foreach (bool detect in new[] { true, false })
        {
            MemoryFileSystem fs = NewFs();
            var cfo = new ColumnFamilyOptions();
            cfo.TableOptions.FilterPolicy = new BrokenFilterPolicy();
            cfo.TableOptions.DetectFilterConstructCorruption = detect;
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), cfo);
            db.Put(B("k"), B("v"));
            if (detect)
            {
                RocksDbException e = Assert.ThrowsExactly<RocksDbException>(db.Flush);
                Assert.AreEqual(StatusCode.Corruption, e.Code);
                StringAssert.Contains(e.Message, "Corrupted filter content");
            }
            else
            {
                db.Flush();
            }
        }
    }

    private static int VarintLength(int v)
    {
        int n = 1;
        while (v >= 0x80)
        {
            v >>= 7;
            n++;
        }
        return n;
    }

    /// <summary>GetEncodedEntrySize (db_etc2_test.cc) for a key without shared bytes.</summary>
    private static ulong EncodedEntrySize(int keySize, int valueSize) => (ulong)(1 + VarintLength(keySize) + VarintLength(valueSize) + keySize + valueSize);

    [TestMethod]
    public void ReadAmpBitmap()
    {
        foreach (uint bytesPerBit in new uint[] { 1, 16 })
        {
            MemoryFileSystem fs = NewFs();
            var st = new Statistics();
            DbOptions o = NewOptions(fs);
            o.Statistics = st;
            var cfo = new ColumnFamilyOptions { Compression = Managed.Compression.CompressionType.None };
            // Disable delta encoding to make it easier to calculate read amplification; a huge block cache.
            cfo.TableOptions.UseDeltaEncoding = false;
            cfo.TableOptions.BlockCache = Managed.Cache.BlockCache.CreateLru(1L << 30);
            cfo.TableOptions.ReadAmpBytesPerBit = bytesPerBit;
            const int numEntries = 10000;
            var rnd = new Random(301);
            var values = new byte[numEntries][];
            using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
            {
                for (int i = 0; i < numEntries; i++)
                {
                    values[i] = new byte[100];
                    rnd.NextBytes(values[i]);
                    db.Put(Key(i), values[i]);
                }
                db.Flush();
            }
            st.Reset();
            using (RocksDatabase db = RocksDatabase.Open(DbPath, o, cfo))
            {
                ulong totalUseful = 0;
                var read = new HashSet<int>();
                for (int i = 0; i < numEntries * 5; i++)
                {
                    int k = rnd.Next(numEntries);
                    CollectionAssert.AreEqual(values[k], db.Get(Key(k)));
                    if (read.Add(k)) totalUseful += EncodedEntrySize(Key(k).Length + 8, 100);
                    if (i % 997 != 0) continue;
                    double total = st.GetTickerCount(Ticker.ReadAmpTotalReadBytes);
                    double expected = totalUseful / total;
                    double actual = st.GetTickerCount(Ticker.ReadAmpEstimateUsefulBytes) / total;
                    // Error between reported read amp and real read amp should be less than 2%.
                    Assert.IsTrue(Math.Abs(expected - actual) * 100 <= 2, "bytes_per_bit " + bytesPerBit + ": " + expected + " vs " + actual);
                }
                using (Iterator it = db.NewIterator())
                {
                    for (it.SeekToFirst(); it.Valid; it.Next()) CollectionAssert.AreEqual(db.Get(it.Key)!, it.Value.ToArray());
                }
                ulong useful = st.GetTickerCount(Ticker.ReadAmpEstimateUsefulBytes);
                ulong totalRead = st.GetTickerCount(Ticker.ReadAmpTotalReadBytes);
                Assert.IsTrue(totalRead > 0);
                // Read amp is on average 100% since we read all what we loaded in memory.
                if (bytesPerBit == 1) Assert.AreEqual(totalRead, useful);
                else Assert.AreEqual(1.0, useful * 1.0 / totalRead, 0.01);
            }
        }
    }

    [TestMethod]
    public void ReadAmpOffRecordsNothing()
    {
        MemoryFileSystem fs = NewFs();
        var st = new Statistics();
        DbOptions o = NewOptions(fs);
        o.Statistics = st;
        using RocksDatabase db = RocksDatabase.Open(DbPath, o);
        for (int i = 0; i < 100; i++) db.Put(Key(i), Value(i, 0));
        db.Flush();
        for (int i = 0; i < 100; i++) db.Get(Key(i));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.ReadAmpTotalReadBytes));
        Assert.AreEqual(0UL, st.GetTickerCount(Ticker.ReadAmpEstimateUsefulBytes));
    }
}
