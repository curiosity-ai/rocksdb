using System.Text;
using RocksDb.Managed.Env;
using RocksDb.Managed.Tests.Db;
using static RocksDb.Managed.Tests.Db.DbTestUtil;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>
/// Prefix seek through the whole DB stack (DBIter + MergingIterator + LevelIterator + table prefix filters):
/// prefix_same_as_start, legacy prefix mode, total_order_seek and auto_prefix_mode against a model, with data spread over
/// the memtable, L0 and sorted levels, deletes and merges, and direction changes. Port of the intent of
/// db/db_test2.cc / db_bloom_filter_test.cc prefix tests.
/// </summary>
[TestClass]
public sealed class PrefixSeekTests
{
    private const int PrefixLength = 3;

    private static ColumnFamilyOptions CfOptions(bool partitioned) => new()
    {
        PrefixExtractor = SliceTransform.CreateFixedPrefix(PrefixLength),
        WriteBufferSize = 32 << 10,
        TargetFileSizeBase = 8 << 10,
        DisableAutoCompactions = true,
        MergeOperator = new StringAppendMergeOperator(),
        TableOptions = new BlockBasedTableOptions
        {
            FilterPolicy = "bloomfilter:10:false",
            BlockSize = 512,
            IndexType = partitioned ? IndexType.TwoLevelIndexSearch : IndexType.BinarySearch,
            PartitionFilters = partitioned,
            MetadataBlockSize = 256,
        },
    };

    private static string P(int p) => "p" + p.ToString("D2", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Builds the DB: half the prefixes are present; L1+ files, L0 files and memtable data with deletes and merges.</summary>
    private static SortedDictionary<string, string> Populate(RocksDatabase db, int seed)
    {
        var rnd = new Random(seed);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int phase = 0; phase < 3; phase++)
        {
            for (int i = 0; i < 1500; i++)
            {
                int p = rnd.Next(50) * 2; // even prefixes only
                string k = P(p) + "/" + rnd.Next(400).ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
                switch (rnd.Next(10))
                {
                    case 0:
                        db.Delete(B(k));
                        model.Remove(k);
                        break;
                    case 1:
                        db.Merge(B(k), B("m" + i));
                        model[k] = model.TryGetValue(k, out string? old) ? old + ",m" + i : "m" + i;
                        break;
                    default:
                        string v = "v" + phase + "-" + i + new string((char)('a' + i % 26), 40);
                        db.Put(B(k), B(v));
                        model[k] = v;
                        break;
                }
            }
            if (phase == 0)
            {
                db.Flush();
                db.CompactRange();
            }
            else if (phase == 1)
            {
                db.Flush();
            }
        }
        return model;
    }

    private static List<string> Scan(Iterator it, int max, bool reverse)
    {
        var r = new List<string>();
        for (; it.Valid && r.Count < max; )
        {
            r.Add(Encoding.ASCII.GetString(it.Key) + "=" + Encoding.ASCII.GetString(it.Value));
            if (reverse) it.Prev();
            else it.Next();
        }
        return r;
    }

    private static List<string> Expected(SortedDictionary<string, string> model, string target, bool samePrefix, bool reverse, int max)
    {
        var all = new List<KeyValuePair<string, string>>(model);
        var r = new List<string>();
        if (!reverse)
        {
            foreach (KeyValuePair<string, string> kv in all)
            {
                if (string.CompareOrdinal(kv.Key, target) < 0) continue;
                if (samePrefix && string.CompareOrdinal(kv.Key, 0, target, 0, PrefixLength) != 0) break;
                r.Add(kv.Key + "=" + kv.Value);
                if (r.Count >= max) break;
            }
        }
        else
        {
            for (int i = all.Count - 1; i >= 0; i--)
            {
                KeyValuePair<string, string> kv = all[i];
                if (string.CompareOrdinal(kv.Key, target) > 0) continue;
                if (samePrefix && string.CompareOrdinal(kv.Key, 0, target, 0, PrefixLength) != 0) break;
                r.Add(kv.Key + "=" + kv.Value);
                if (r.Count >= max) break;
            }
        }
        return r;
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void PrefixSameAsStartMatchesModel(bool partitioned)
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions(partitioned));
        SortedDictionary<string, string> model = Populate(db, partitioned ? 2 : 1);
        int sorted = 0;
        for (int l = 1; l < 7; l++) sorted += int.Parse(db.GetProperty("rocksdb.num-files-at-level" + l) ?? "0", System.Globalization.CultureInfo.InvariantCulture);
        Assert.IsTrue(sorted > 1, "sorted-run files " + sorted);

        var ro = new ReadOptions { PrefixSameAsStart = true };
        for (int p = 0; p < 100; p++)
        {
            foreach (string target in new[] { P(p), P(p) + "/150", P(p) + "/999" })
            {
                using (Iterator it = db.NewIterator(ro))
                {
                    it.Seek(B(target));
                    CollectionAssert.AreEqual(Expected(model, target, true, false, int.MaxValue), Scan(it, int.MaxValue, false), "seek " + target);
                }
                using (Iterator it = db.NewIterator(ro))
                {
                    it.SeekForPrev(B(target));
                    CollectionAssert.AreEqual(Expected(model, target, true, true, int.MaxValue), Scan(it, int.MaxValue, true), "seekforprev " + target);
                }
            }
        }

        // Direction changes within a prefix (ReverseToForward / ReverseToBackward re-seek in prefix mode).
        for (int p = 0; p < 100; p += 2)
        {
            string target = P(p) + "/200";
            List<string> fwd = Expected(model, target, true, false, int.MaxValue);
            if (fwd.Count < 3) continue;
            using Iterator it = db.NewIterator(ro);
            it.Seek(B(target));
            it.Next();
            it.Next();
            it.Prev();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(fwd[1], Encoding.ASCII.GetString(it.Key) + "=" + Encoding.ASCII.GetString(it.Value), target);
            it.Prev();
            Assert.AreEqual(fwd[0], Encoding.ASCII.GetString(it.Key) + "=" + Encoding.ASCII.GetString(it.Value), target);
            it.Next();
            Assert.AreEqual(fwd[1], Encoding.ASCII.GetString(it.Key) + "=" + Encoding.ASCII.GetString(it.Value), target);
        }

        // SeekToFirst with prefix_same_as_start: iteration stops at the end of the first key's prefix.
        using (Iterator it = db.NewIterator(ro))
        {
            it.SeekToFirst();
            string first = model.Keys.First();
            CollectionAssert.AreEqual(Expected(model, first, true, false, int.MaxValue), Scan(it, int.MaxValue, false));
        }
    }

    /// <summary>
    /// Legacy prefix mode (prefix extractor, no total_order_seek): results within the seek prefix are exact; total_order_seek
    /// and auto_prefix_mode keep full total-order semantics.
    /// </summary>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public void LegacyAndTotalOrderModes(bool partitioned)
    {
        MemoryFileSystem fs = NewFs();
        using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs), CfOptions(partitioned));
        SortedDictionary<string, string> model = Populate(db, 7);

        for (int p = 0; p < 100; p += 3)
        {
            string target = P(p) + "/100";
            List<string> inPrefix = Expected(model, target, true, false, int.MaxValue);
            using (Iterator it = db.NewIterator(new ReadOptions()))
            {
                it.Seek(B(target));
                List<string> got = Scan(it, int.MaxValue, false);
                // Within the prefix: exact. Past it: whatever the (filtered) children still return, but in order.
                CollectionAssert.AreEqual(inPrefix, got.GetRange(0, Math.Min(inPrefix.Count, got.Count)), "legacy " + target);
                Assert.IsTrue(got.Count >= inPrefix.Count);
            }
            using (Iterator it = db.NewIterator(new ReadOptions { TotalOrderSeek = true }))
            {
                it.Seek(B(target));
                CollectionAssert.AreEqual(Expected(model, target, false, false, 50), Scan(it, 50, false), "total order " + target);
            }
            using (Iterator it = db.NewIterator(new ReadOptions { AutoPrefixMode = true, IterateUpperBound = B(P(p + 1)) }))
            {
                it.Seek(B(target));
                CollectionAssert.AreEqual(inPrefix, Scan(it, int.MaxValue, false), "auto prefix " + target);
            }
            using (Iterator it = db.NewIterator(new ReadOptions { AutoPrefixMode = true }))
            {
                it.Seek(B(target));
                CollectionAssert.AreEqual(Expected(model, target, false, false, 50), Scan(it, 50, false), "auto prefix, no bound " + target);
            }
        }
        using (Iterator it = db.NewIterator(new ReadOptions { TotalOrderSeek = true }))
        {
            it.SeekToFirst();
            Assert.AreEqual(model.Count, Scan(it, int.MaxValue, false).Count);
        }
        // Point lookups go through the filters of every level.
        foreach (KeyValuePair<string, string> kv in model) Assert.AreEqual(kv.Value, GetString(db, kv.Key), kv.Key);
        for (int p = 1; p < 100; p += 2) Assert.IsNull(GetString(db, P(p) + "/001"));
    }
}
