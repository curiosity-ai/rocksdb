using System.Text;
using RocksDb.Managed.CompatTests.Infrastructure;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Plain;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.CompatTests.Table;

/// <summary>
/// PlainTable and CuckooTable compatibility with native RocksDB (rocksdb_options_set_plain_table_factory /
/// rocksdb_options_set_cuckoo_table_factory): whole databases written by either side are read by the other (Get, prefix
/// and full iteration after flush and compaction), native-written OPTIONS files load and re-serialize byte-identically,
/// and SstFileWriter output for the same keys has identical data, index and bloom blocks.
/// </summary>
[TestClass]
public sealed class PlainCuckooCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private sealed record PlainSettings(bool Prefix, PlainTableEncodingType Encoding, bool StoreIndex, uint FixedKeyLen, int BloomBits, ulong Sparseness)
    {
        public double Ratio => Prefix ? 0.75 : 0;

        public RocksDbSharp.ColumnFamilyOptions Native()
        {
            var cf = new RocksDbSharp.ColumnFamilyOptions();
            if (Prefix) cf.SetPrefixExtractor(RocksDbSharp.SliceTransform.CreateFixedPrefix(4));
            cf.SetPlainTableFactory(FixedKeyLen, BloomBits, Ratio, (int)Sparseness, 0, (char)(byte)Encoding, false, StoreIndex);
            s_keepAlive.Add(cf);
            return cf;
        }

        public ColumnFamilyOptions Managed()
        {
            var cf = new ColumnFamilyOptions
            {
                TableFactory = TableFactoryType.PlainTable,
                PlainTableOptions = new PlainTableOptions
                {
                    EncodingType = Encoding, StoreIndexInFile = StoreIndex, UserKeyLength = FixedKeyLen, BloomBitsPerKey = BloomBits,
                    HashTableRatio = Ratio, IndexSparseness = Sparseness,
                },
            };
            if (Prefix) cf.PrefixExtractor = SliceTransform.CreateFixedPrefix(4);
            return cf;
        }
    }

    private static readonly PlainSettings[] s_plainSettings =
    [
        new(true, PlainTableEncodingType.Plain, false, 0, 10, 16),
        new(true, PlainTableEncodingType.Prefix, false, 0, 10, 3),
        new(true, PlainTableEncodingType.Prefix, true, 0, 10, 16),
        new(true, PlainTableEncodingType.Plain, true, 10, 0, 1),
        new(false, PlainTableEncodingType.Plain, false, 0, 10, 16),
        new(false, PlainTableEncodingType.Plain, true, 10, 6, 4),
    ];

    /// <summary>Keys "pNN/xxxxx" (10 bytes, 4-byte prefixes "pNN/") and values of varying length.</summary>
    private static SortedDictionary<string, string> RandomData(Random rnd, int n)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        while (d.Count < n)
        {
            string k = "p" + rnd.Next(30).ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/" + rnd.Next(1000000).ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
            d[k] = "v" + rnd.Next() + new string('x', rnd.Next(80));
        }
        return d;
    }

    private static byte[] B(string s) => Encoding.ASCII.GetBytes(s);

    private static void VerifyNative(RocksDbSharp.RocksDb db, SortedDictionary<string, string> data, bool prefix)
    {
        foreach (KeyValuePair<string, string> kv in data) Assert.AreEqual(kv.Value, db.Get(kv.Key));
        Assert.IsNull(db.Get("p01/missing"));
        var ro = new RocksDbSharp.ReadOptions();
        if (prefix) ro.SetPrefixSameAsStart(true);
        for (int p = 0; p < 30; p += 7)
        {
            string pre = "p" + p.ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/";
            var expected = new List<string>();
            foreach (string k in data.Keys) if (k.StartsWith(pre, StringComparison.Ordinal)) expected.Add(k);
            var actual = new List<string>();
            using RocksDbSharp.Iterator it = db.NewIterator(null, ro);
            for (it.Seek(pre); it.Valid(); it.Next())
            {
                string k = it.StringKey();
                if (!prefix && !k.StartsWith(pre, StringComparison.Ordinal)) break;
                actual.Add(k);
            }
            CollectionAssert.AreEqual(expected, actual, pre);
        }
    }

    private static void VerifyManaged(RocksDatabase db, SortedDictionary<string, string> data, bool prefix)
    {
        foreach (KeyValuePair<string, string> kv in data) Assert.AreEqual(kv.Value, Str(db.Get(B(kv.Key))), kv.Key);
        Assert.IsNull(db.Get("p01/missing"u8));
        for (int p = 0; p < 30; p += 7)
        {
            string pre = "p" + p.ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/";
            var expected = new List<string>();
            foreach (string k in data.Keys) if (k.StartsWith(pre, StringComparison.Ordinal)) expected.Add(k);
            var actual = new List<string>();
            using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = prefix });
            for (it.Seek(B(pre)); it.Valid; it.Next())
            {
                string k = Encoding.ASCII.GetString(it.Key);
                if (!prefix && !k.StartsWith(pre, StringComparison.Ordinal)) break;
                actual.Add(k);
            }
            CollectionAssert.AreEqual(expected, actual, pre);
        }
    }

    private static string? Str(byte[]? b) => b is null ? null : Encoding.ASCII.GetString(b);

    [TestMethod]
    public void PlainTable_NativeDbReadByManaged_AndManagedDbReadByNative()
    {
        var rnd = new Random(20261103);
        foreach (PlainSettings s in s_plainSettings)
        {
            SortedDictionary<string, string> data = RandomData(rnd, 1500);
            // Native writes (two flushes + compaction + an unflushed tail in the WAL).
            using (var dir = new TempDir("plain-n2m"))
            {
                var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetAllowMmapReads(true);
                using (var db = RocksDbSharp.RocksDb.Open(nopts, dir.Path, new RocksDbSharp.ColumnFamilies(s.Native())))
                {
                    int i = 0;
                    foreach (KeyValuePair<string, string> kv in data)
                    {
                        db.Put(kv.Key, kv.Value);
                        if (++i % 600 == 0) db.Flush(new RocksDbSharp.FlushOptions());
                    }
                    if (s.Prefix) db.CompactRange((byte[]?)null, null);
                }
                // The managed side loads the native OPTIONS file (table_factory=PlainTable + its section).
                string optionsPath = Directory.GetFiles(dir.Path, "OPTIONS-*")[^1];
                OptionsFile native = OptionsFile.Load(LocalFileSystem.Instance, optionsPath, preserveUnknownOptions: true);
                ColumnFamilyOptions cf = native.FindColumnFamily("default")!;
                Assert.AreEqual(TableFactoryType.PlainTable, cf.TableFactory, s.ToString());
                Assert.AreEqual(s.Encoding, cf.PlainTableOptions.EncodingType, s.ToString());
                Assert.AreEqual(s.StoreIndex, cf.PlainTableOptions.StoreIndexInFile, s.ToString());
                Assert.AreEqual(s.FixedKeyLen, cf.PlainTableOptions.UserKeyLength, s.ToString());
                using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions { AllowMmapReads = true }, cf);
                VerifyManaged(mdb, data, s.Prefix);
            }
            // Managed writes.
            using (var dir = new TempDir("plain-m2n"))
            {
                using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true }, s.Managed()))
                {
                    int i = 0;
                    foreach (KeyValuePair<string, string> kv in data)
                    {
                        db.Put(B(kv.Key), B(kv.Value));
                        if (++i % 600 == 0) db.Flush();
                    }
                    db.Flush();
                    if (s.Prefix) db.CompactRange();
                }
                var nopts = new RocksDbSharp.DbOptions().SetAllowMmapReads(true);
                using var ndb = RocksDbSharp.RocksDb.Open(nopts, dir.Path, new RocksDbSharp.ColumnFamilies(s.Native()));
                VerifyNative(ndb, data, s.Prefix);
            }
        }
    }

    [TestMethod]
    public void PlainTable_SstFileWriter_IdenticalDataIndexAndBloom()
    {
        var rnd = new Random(7);
        foreach (PlainSettings s in s_plainSettings)
        {
            SortedDictionary<string, string> data = RandomData(rnd, 700);
            using var dir = new TempDir("plain-sst");
            string nativePath = System.IO.Path.Combine(dir.Path, "native.sst");
            using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), s.Native()))
            {
                w.Open(nativePath);
                foreach (KeyValuePair<string, string> kv in data) w.Add(kv.Key, kv.Value);
                w.Finish();
            }
            string managedPath = System.IO.Path.Combine(dir.Path, "managed.sst");
            ColumnFamilyOptions mcf = s.Managed();
            using (var w = new SstFileWriter(mcf))
            {
                w.Open(managedPath);
                foreach (KeyValuePair<string, string> kv in data) w.Put(B(kv.Key), B(kv.Value));
                w.Finish();
            }
            byte[] nf = File.ReadAllBytes(nativePath), mf = File.ReadAllBytes(managedPath);
            var ro = new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = mcf.PlainTableOptions, PrefixExtractor = mcf.PrefixExtractor };
            using var nr = (PlainTableReader)TableReaderFactory.Open(LocalFileSystem.Instance.OpenRandomAccess(nativePath), ro);
            using var mr = (PlainTableReader)TableReaderFactory.Open(LocalFileSystem.Instance.OpenRandomAccess(managedPath), ro);
            TableProperties np = nr.Properties, mp = mr.Properties;
            Assert.AreEqual(np.DataSize, mp.DataSize, s.ToString());
            CollectionAssert.AreEqual(nf.AsSpan(0, (int)np.DataSize).ToArray(), mf.AsSpan(0, (int)mp.DataSize).ToArray(), "data " + s);
            Assert.AreEqual(np.IndexSize, mp.IndexSize, s.ToString());
            Assert.AreEqual(np.FilterSize, mp.FilterSize, s.ToString());
            // Bloom + index meta blocks follow the data (store_index_in_file).
            int metaLen = (int)(np.IndexSize + np.FilterSize);
            CollectionAssert.AreEqual(nf.AsSpan((int)np.DataSize, metaLen).ToArray(), mf.AsSpan((int)mp.DataSize, metaLen).ToArray(), "index/bloom " + s);
            Assert.AreEqual(np.NumEntries, mp.NumEntries);
            Assert.AreEqual(np.RawKeySize, mp.RawKeySize);
            Assert.AreEqual(np.RawValueSize, mp.RawValueSize);
            Assert.AreEqual(np.FormatVersion, mp.FormatVersion);
            Assert.AreEqual(np.FixedKeyLen, mp.FixedKeyLen);
            Assert.AreEqual(np.PrefixExtractorName, mp.PrefixExtractorName);
            foreach (string name in new[] { PlainTablePropertyNames.EncodingType, PlainTablePropertyNames.BloomVersion, PlainTablePropertyNames.NumBloomBlocks,
                         TablePropertiesNames.ExternalSstFileVersion, "plain_table_hash_table_size", "plain_table_sub_index_size" })
            {
                Assert.AreEqual(np.UserCollectedProperties.ContainsKey(name), mp.UserCollectedProperties.ContainsKey(name), name);
                if (np.UserCollectedProperties.TryGetValue(name, out byte[]? v)) CollectionAssert.AreEqual(v, mp.UserCollectedProperties[name], name);
            }
            // Footers are identical (format_version 0, legacy magic, no checksum).
            Assert.AreEqual(Coding.DecodeFixed64(nf.AsSpan(nf.Length - 8)), Coding.DecodeFixed64(mf.AsSpan(mf.Length - 8)));
        }
    }

    // ---- cuckoo --------------------------------------------------------------------------------------------------

    private static RocksDbSharp.ColumnFamilyOptions NativeCuckoo(CuckooTableOptions o)
    {
        RocksDbSharp.Native n = RocksDbSharp.Native.Instance;
        IntPtr co = n.rocksdb_cuckoo_options_create();
        n.rocksdb_cuckoo_options_set_hash_ratio(co, o.HashTableRatio);
        n.rocksdb_cuckoo_options_set_max_search_depth(co, o.MaxSearchDepth);
        n.rocksdb_cuckoo_options_set_cuckoo_block_size(co, o.CuckooBlockSize);
        n.rocksdb_cuckoo_options_set_identity_as_first_hash(co, (byte)(o.IdentityAsFirstHash ? 1 : 0));
        n.rocksdb_cuckoo_options_set_use_module_hash(co, (byte)(o.UseModuleHash ? 1 : 0));
        var cf = new RocksDbSharp.ColumnFamilyOptions();
        n.rocksdb_options_set_cuckoo_table_factory(cf.Handle, co);
        n.rocksdb_cuckoo_options_destroy(co);
        s_keepAlive.Add(cf);
        return cf;
    }

    private static readonly CuckooTableOptions[] s_cuckooSettings =
    [
        new(),
        new() { UseModuleHash = false, CuckooBlockSize = 1, HashTableRatio = 0.5 },
        new() { IdentityAsFirstHash = true, CuckooBlockSize = 3 },
    ];

    private static SortedDictionary<string, string> CuckooData(Random rnd, int n)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        while (d.Count < n) d["k" + rnd.Next(10_000_000).ToString("D7", System.Globalization.CultureInfo.InvariantCulture)] = "val" + rnd.Next(100000).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
        return d;
    }

    [TestMethod]
    public void CuckooTable_NativeDbReadByManaged_AndManagedDbReadByNative()
    {
        var rnd = new Random(42);
        foreach (CuckooTableOptions o in s_cuckooSettings)
        {
            SortedDictionary<string, string> data = CuckooData(rnd, 1000);
            using (var dir = new TempDir("cuckoo-n2m"))
            {
                var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetAllowMmapReads(true);
                using (var db = RocksDbSharp.RocksDb.Open(nopts, dir.Path, new RocksDbSharp.ColumnFamilies(NativeCuckoo(o))))
                {
                    foreach (KeyValuePair<string, string> kv in data) db.Put(kv.Key, kv.Value);
                    db.Flush(new RocksDbSharp.FlushOptions());
                }
                string optionsPath = Directory.GetFiles(dir.Path, "OPTIONS-*")[^1];
                ColumnFamilyOptions cf = OptionsFile.Load(LocalFileSystem.Instance, optionsPath, preserveUnknownOptions: true).FindColumnFamily("default")!;
                Assert.AreEqual(TableFactoryType.CuckooTable, cf.TableFactory);
                Assert.AreEqual(o.UseModuleHash, cf.CuckooTableOptions.UseModuleHash);
                Assert.AreEqual(o.CuckooBlockSize, cf.CuckooTableOptions.CuckooBlockSize);
                using RocksDatabase mdb = RocksDatabase.Open(dir.Path, new DbOptions { AllowMmapReads = true }, cf);
                foreach (KeyValuePair<string, string> kv in data) Assert.AreEqual(kv.Value, Str(mdb.Get(B(kv.Key))));
                using Iterator it = mdb.NewIterator();
                var keys = new List<string>();
                for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(Encoding.ASCII.GetString(it.Key));
                CollectionAssert.AreEqual(new List<string>(data.Keys), keys);
            }
            using (var dir = new TempDir("cuckoo-m2n"))
            {
                var cf = new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = o };
                using (RocksDatabase db = RocksDatabase.Open(dir.Path, new DbOptions { CreateIfMissing = true, AllowMmapReads = true }, cf))
                {
                    foreach (KeyValuePair<string, string> kv in data) db.Put(B(kv.Key), B(kv.Value));
                    db.Flush();
                }
                using var ndb = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetAllowMmapReads(true), dir.Path, new RocksDbSharp.ColumnFamilies(NativeCuckoo(o)));
                foreach (KeyValuePair<string, string> kv in data) Assert.AreEqual(kv.Value, ndb.Get(kv.Key));
                Assert.IsNull(ndb.Get("k-missing"));
            }
        }
    }

    [TestMethod]
    public void CuckooTable_SstFileWriter_IdenticalHashTable()
    {
        var rnd = new Random(9);
        foreach (CuckooTableOptions o in s_cuckooSettings)
        {
            SortedDictionary<string, string> data = CuckooData(rnd, 800);
            using var dir = new TempDir("cuckoo-sst");
            string nativePath = System.IO.Path.Combine(dir.Path, "native.sst");
            using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), NativeCuckoo(o)))
            {
                w.Open(nativePath);
                foreach (KeyValuePair<string, string> kv in data) w.Add(kv.Key, kv.Value);
                w.Finish();
            }
            string managedPath = System.IO.Path.Combine(dir.Path, "managed.sst");
            using (var w = new SstFileWriter(new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = o }))
            {
                w.Open(managedPath);
                foreach (KeyValuePair<string, string> kv in data) w.Put(B(kv.Key), B(kv.Value));
                w.Finish();
            }
            byte[] nf = File.ReadAllBytes(nativePath), mf = File.ReadAllBytes(managedPath);
            var ro = new TableReaderOptions { TableFactory = TableFactoryType.CuckooTable, AllowMmapReads = true };
            using TableReader nr = TableReaderFactory.Open(LocalFileSystem.Instance.OpenRandomAccess(nativePath), ro);
            using TableReader mr = TableReaderFactory.Open(LocalFileSystem.Instance.OpenRandomAccess(managedPath), ro);
            Assert.AreEqual(nr.Properties.DataSize, mr.Properties.DataSize);
            CollectionAssert.AreEqual(nf.AsSpan(0, (int)nr.Properties.DataSize).ToArray(), mf.AsSpan(0, (int)mr.Properties.DataSize).ToArray(), "hash table " + o.ToOptionsString());
            foreach (KeyValuePair<string, byte[]> kv in nr.Properties.UserCollectedProperties)
            {
                if (!kv.Key.StartsWith("rocksdb.cuckoo.", StringComparison.Ordinal)) continue;
                CollectionAssert.AreEqual(kv.Value, mr.Properties.UserCollectedProperties[kv.Key], kv.Key);
            }
            Assert.AreEqual(Coding.DecodeFixed64(nf.AsSpan(nf.Length - 8)), Coding.DecodeFixed64(mf.AsSpan(mf.Length - 8)));
            // Every key resolves through the native file in the managed reader.
            foreach (KeyValuePair<string, string> kv in data)
            {
                byte[] lk = new byte[kv.Key.Length + 8];
                DbFormat.AppendInternalKey(lk, B(kv.Key), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                var buf = new System.Buffers.ArrayBufferWriter<byte>();
                Assert.IsTrue(nr.Get(lk, buf, TableReadFlags.None).Found);
                Assert.AreEqual(kv.Value, Encoding.ASCII.GetString(buf.WrittenSpan));
            }
        }
    }

    [TestMethod]
    public void OptionsFile_NativeTableSections_RoundTripByteIdentical()
    {
        using var dir = new TempDir("plain-opts");
        var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetAllowMmapReads(true);
        PlainSettings s = s_plainSettings[2];
        using (RocksDbSharp.RocksDb.Open(nopts, dir.Path, new RocksDbSharp.ColumnFamilies(s.Native()))) { }
        string optionsPath = Directory.GetFiles(dir.Path, "OPTIONS-*")[^1];
        string text = File.ReadAllText(optionsPath);
        OptionsFile parsed = OptionsFile.Parse(text, preserveUnknownOptions: true);
        string section = Section(text, "[TableOptions/PlainTable \"default\"]");
        Assert.AreEqual(section, Section(parsed.Serialize(), "[TableOptions/PlainTable \"default\"]"));
    }

    private static string Section(string text, string title)
    {
        int i = text.IndexOf(title, StringComparison.Ordinal);
        Assert.IsTrue(i >= 0, title);
        int j = text.IndexOf("\n[", i + title.Length, StringComparison.Ordinal);
        return j < 0 ? text[i..] : text[i..j];
    }
}
