using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Cuckoo;
using RocksDb.Managed.Table.Plain;
using RocksDb.Managed.Tests.Db;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// Plain and cuckoo table formats (table/plain/*, table/cuckoo/*): randomized table-level model tests against a sorted
/// oracle, footer / magic dispatch, error contracts that mirror upstream, and DB-level flush / compaction / reopen with
/// a dictionary oracle.
/// </summary>
[TestClass]
public sealed class PlainCuckooTableTests
{
    private static readonly TableFormatOptions s_bbtFormat = TableFactory.CreateFormatOptions(new BlockBasedTableOptions());

    private sealed record Entry(byte[] UserKey, ulong Seq, EntryType Type, byte[] Value)
    {
        public byte[] InternalKey()
        {
            byte[] k = new byte[UserKey.Length + 8];
            DbFormat.AppendInternalKey(k, UserKey, Seq, Type);
            return k;
        }
    }

    private static int CompareEntries(Entry a, Entry b)
    {
        int c = a.UserKey.AsSpan().SequenceCompareTo(b.UserKey);
        if (c != 0) return c;
        return b.Seq.CompareTo(a.Seq); // newer first
    }

    private static byte[] Build(TableBuilderOptions tbo, IReadOnlyList<Entry> entries)
    {
        var f = new MemWritableFile();
        using (TableBuilder b = TableBuilder.Create(f, s_bbtFormat, tbo))
        {
            foreach (Entry e in entries) b.Add(e.InternalKey(), e.Value);
            b.Finish();
            Assert.AreEqual((ulong)f.Size, b.FileSize);
        }
        return f.ToArray();
    }

    private static byte[] LookupKey(byte[] userKey, ulong seq)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, seq, DbFormat.ValueTypeForSeek);
        return k;
    }

    /// <summary>Random user keys "pNN/..." with a 4-byte prefix (some prefixes with many keys), sorted, with 1..3 versions.</summary>
    private static List<Entry> RandomEntries(Random rnd, int n, int fixedKeyLen, bool allowSeqZero, bool withMerge)
    {
        var keys = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        while (keys.Count < n)
        {
            string prefix = "p" + rnd.Next(rnd.Next(2) == 0 ? 5 : 60).ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/";
            string s = prefix + rnd.Next(1_000_000).ToString(System.Globalization.CultureInfo.InvariantCulture);
            if (fixedKeyLen > 0) s = s.PadRight(fixedKeyLen, 'z')[..fixedKeyLen];
            else if (rnd.Next(10) == 0) s += new string('k', rnd.Next(100)); // keys longer than the inline size limit
            keys.Add(Encoding.ASCII.GetBytes(s));
        }
        var list = new List<Entry>();
        ulong seq = 1000;
        foreach (byte[] k in keys)
        {
            int versions = 1 + rnd.Next(3);
            for (int v = 0; v < versions; v++)
            {
                EntryType t = rnd.Next(5) switch { 0 => EntryType.Deletion, 1 when withMerge => EntryType.Merge, _ => EntryType.Value };
                ulong s = allowSeqZero && v == versions - 1 && t == EntryType.Value && rnd.Next(2) == 0 ? 0 : seq--;
                byte[] value = t == EntryType.Deletion ? [] : Encoding.ASCII.GetBytes("v" + rnd.Next() + new string('x', rnd.Next(rnd.Next(10) == 0 ? 300 : 20)));
                list.Add(new Entry(k, s, t, value));
                if (s == 0) break;
            }
        }
        list.Sort(CompareEntries);
        return list;
    }

    private static void AssertEntry(Entry expected, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        CollectionAssert.AreEqual(expected.InternalKey(), internalKey.ToArray());
        CollectionAssert.AreEqual(expected.Value, value.ToArray());
    }

    private static void VerifyPlainTable(PlainTableReader reader, List<Entry> entries, bool prefixMode, Random rnd)
    {
        // Full scan
        using (InternalIterator it = reader.NewInternalIterator(TableReadFlags.None, new ReadOptions { TotalOrderSeek = !prefixMode }, null))
        {
            int i = 0;
            for (it.SeekToFirst(); it.Valid; it.Next()) AssertEntry(entries[i++], it.Key, it.Value);
            Assert.AreEqual(entries.Count, i);
        }
        // Point lookups: every version of every key, and missing keys.
        var buffer = new ArrayBufferWriter<byte>();
        for (int i = 0; i < entries.Count; i++)
        {
            Entry e = entries[i];
            buffer.Clear();
            TableGetResult r = reader.Get(LookupKey(e.UserKey, e.Seq), buffer, TableReadFlags.None);
            Assert.IsTrue(r.Found, "key " + Encoding.ASCII.GetString(e.UserKey) + " seq " + e.Seq);
            Assert.AreEqual(e.Seq, r.Sequence);
            Assert.AreEqual(e.Type, r.Type);
            CollectionAssert.AreEqual(e.Value, buffer.WrittenSpan.ToArray());
            // All versions visible at the newest snapshot, in order.
            if (i == 0 || !entries[i - 1].UserKey.AsSpan().SequenceEqual(e.UserKey))
            {
                var seen = new List<ulong>();
                reader.Get(LookupKey(e.UserKey, DbFormat.MaxSequenceNumber), ref seen, static (ref List<ulong> s, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) =>
                {
                    s.Add(DbFormat.ExtractFooter(k) >> 8);
                    return true;
                }, TableReadFlags.None);
                var expected = new List<ulong>();
                for (int j = i; j < entries.Count && entries[j].UserKey.AsSpan().SequenceEqual(e.UserKey); j++) expected.Add(entries[j].Seq);
                CollectionAssert.AreEqual(expected, seen);
            }
        }
        for (int m = 0; m < 200; m++)
        {
            byte[] missing = Encoding.ASCII.GetBytes("p" + rnd.Next(60).ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/missing" + m);
            if (entries.Exists(e => e.UserKey.AsSpan().SequenceEqual(missing))) continue;
            Assert.IsFalse(reader.Get(LookupKey(missing, DbFormat.MaxSequenceNumber), null, TableReadFlags.None).Found);
        }
        // Seeks: to existing internal keys (and in prefix mode staying within the prefix).
        using (InternalIterator it = reader.NewInternalIterator(TableReadFlags.None, new ReadOptions(), null))
        {
            for (int s = 0; s < 300; s++)
            {
                int i = rnd.Next(entries.Count);
                it.Seek(entries[i].InternalKey());
                Assert.IsTrue(it.Valid);
                AssertEntry(entries[i], it.Key, it.Value);
                it.Next();
                if (i + 1 < entries.Count && (!prefixMode || entries[i + 1].UserKey.AsSpan(0, 4).SequenceEqual(entries[i].UserKey.AsSpan(0, 4))))
                    AssertEntry(entries[i + 1], it.Key, it.Value);
            }
        }
    }

    [TestMethod]
    public void PlainTable_RandomModel_AllOptionCombinations()
    {
        int seed = Environment.TickCount;
        TestContext?.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        SliceTransform prefix4 = SliceTransform.CreateFixedPrefix(4);
        for (int round = 0; round < 48; round++)
        {
            bool prefixMode = rnd.Next(3) != 0;
            var opts = new PlainTableOptions
            {
                EncodingType = prefixMode && rnd.Next(2) == 0 ? PlainTableEncodingType.Prefix : PlainTableEncodingType.Plain,
                UserKeyLength = rnd.Next(4) == 0 ? 12u : 0u,
                StoreIndexInFile = rnd.Next(2) == 0,
                BloomBitsPerKey = rnd.Next(3) == 0 ? 0 : 10,
                HashTableRatio = prefixMode ? (rnd.Next(2) == 0 ? 0.75 : 0.2) : 0,
                IndexSparseness = (ulong)(new[] { 0, 1, 3, 16 })[rnd.Next(4)],
            };
            uint locality = (uint)rnd.Next(2);
            string what = "round " + round + " seed " + seed + " prefix " + prefixMode + " opts " + opts.ToOptionsString() + " locality " + locality;
            List<Entry> entries = RandomEntries(rnd, 1 + rnd.Next(400), (int)opts.UserKeyLength, allowSeqZero: true, withMerge: true);
            var tbo = new TableBuilderOptions
            {
                TableFactory = TableFactoryType.PlainTable,
                PlainTableOptions = opts,
                PrefixExtractor = prefixMode ? prefix4 : null,
                BloomLocality = locality,
            };
            byte[] file = Build(tbo, entries);
            var ro = new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, PrefixExtractor = prefixMode ? prefix4 : null, BloomLocality = locality };
            using var reader = (PlainTableReader)TableReaderFactory.Open(new MemRandomAccessFile(file), ro);
            Assert.AreEqual((ulong)entries.Count, reader.Properties.NumEntries, what);
            Assert.AreEqual(opts.EncodingType == PlainTableEncodingType.Prefix ? 1UL : 0UL, reader.Properties.FormatVersion, what);
            try
            {
                VerifyPlainTable(reader, entries, prefixMode, rnd);
            }
            catch (Exception ex)
            {
                throw new AssertFailedException(what + ": " + ex.Message, ex);
            }
            // The adaptive factory opens it by magic number.
            using TableReader adaptive = TableReaderFactory.Open(new MemRandomAccessFile(file), ro with { TableFactory = TableFactoryType.AdaptiveTable });
            Assert.IsInstanceOfType<PlainTableReader>(adaptive);
        }
    }

    [TestMethod]
    public void PlainTable_FooterIsLegacyFormatVersion0()
    {
        List<Entry> entries = [new Entry("abcd1"u8.ToArray(), 5, EntryType.Value, "v"u8.ToArray())];
        byte[] file = Build(new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = new PlainTableOptions { HashTableRatio = 0 } }, entries);
        Assert.AreEqual(TableMagicNumbers.LegacyPlainTable, Coding.DecodeFixed64(file.AsSpan(file.Length - 8)));
        Footer f = Footer.Decode(file.AsSpan(file.Length - Footer.EncodedLength), (ulong)(file.Length - Footer.EncodedLength), 0);
        Assert.AreEqual(TableMagicNumbers.PlainTable, f.TableMagicNumber);
        Assert.AreEqual(0, f.FormatVersion);
        Assert.AreEqual(ChecksumType.Crc32c, f.ChecksumType);
        // The block-based reader rejects it with the bad magic number error, like BlockBasedTable::Open.
        var ex = Assert.ThrowsExactly<RocksDbException>(() => BlockBasedTableReader.Open(new MemRandomAccessFile(file), new TableReaderOptions()));
        Assert.AreEqual(StatusCode.Corruption, ex.Code);
        StringAssert.Contains(ex.Message, "Bad table magic number");
    }

    [TestMethod]
    public void PlainTable_UpstreamErrorContracts()
    {
        SliceTransform prefix4 = SliceTransform.CreateFixedPrefix(4);
        List<Entry> entries = [new Entry("abcd1"u8.ToArray(), 5, EntryType.Value, "v"u8.ToArray()), new Entry("abcd2"u8.ToArray(), 4, EntryType.Value, "w"u8.ToArray())];
        var tbo = new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = new PlainTableOptions(), PrefixExtractor = prefix4 };
        byte[] file = Build(tbo, entries);
        var ro = new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = new PlainTableOptions(), PrefixExtractor = prefix4 };

        // Prefix extractor missing / different from the one the table was built with.
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => TableReaderFactory.Open(new MemRandomAccessFile(file), ro with { PrefixExtractor = null })).Code);
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => TableReaderFactory.Open(new MemRandomAccessFile(file), ro with { PrefixExtractor = SliceTransform.CreateFixedPrefix(3) })).Code);
        // Total order mode needs hash_table_ratio = 0.
        byte[] totalOrder = Build(tbo with { PrefixExtractor = null }, entries);
        Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(
            () => TableReaderFactory.Open(new MemRandomAccessFile(totalOrder), ro with { PrefixExtractor = null })).Code);

        using TableReader reader = TableReaderFactory.Open(new MemRandomAccessFile(file), ro);
        // total_order_seek on a prefix-mode table: iterators can be created (compaction) but not sought.
        using (InternalIterator it = reader.NewInternalIterator(TableReadFlags.None, new ReadOptions { TotalOrderSeek = true }, prefix4))
        {
            it.SeekToFirst();
            Assert.IsTrue(it.Valid);
            Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(() => it.Seek(entries[0].InternalKey())).Code);
            Assert.IsFalse(it.Valid);
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => it.SeekToLast()).Code);
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => it.SeekForPrev(entries[0].InternalKey())).Code);
        }
        // Range deletions are not supported by the builder.
        var f = new MemWritableFile();
        using (TableBuilder b = TableBuilder.Create(f, s_bbtFormat, tbo))
        {
            byte[] rd = new byte[13];
            DbFormat.AppendInternalKey(rd, "abcd1"u8, 3, EntryType.RangeDeletion);
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => b.Add(rd, "abcd9"u8)).Code);
        }
        // Full scan mode: SeekToFirst/Next only.
        using TableReader full = TableReaderFactory.Open(new MemRandomAccessFile(file), ro with { PlainTableOptions = new PlainTableOptions { FullScanMode = true } });
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => full.Get(LookupKey("abcd1"u8.ToArray(), 10), null, TableReadFlags.None)).Code);
        using InternalIterator fit = full.NewInternalIterator(TableReadFlags.None, null, prefix4);
        int n = 0;
        for (fit.SeekToFirst(); fit.Valid; fit.Next()) n++;
        Assert.AreEqual(2, n);
    }

    [TestMethod]
    public void PlainTable_KeyEncodingBytes()
    {
        // Format notes of table/plain/plain_table_factory.h: kPlain with variable length = varint size + internal key;
        // a seq-0 value replaces the 8-byte footer with 0xFF.
        List<Entry> entries = [new Entry("ab"u8.ToArray(), 0, EntryType.Value, "x"u8.ToArray()), new Entry("ac"u8.ToArray(), 7, EntryType.Deletion, [])];
        byte[] file = Build(new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = new PlainTableOptions { HashTableRatio = 0 } }, entries);
        byte[] expected = [2, (byte)'a', (byte)'b', 0xFF, 1, (byte)'x', 2, (byte)'a', (byte)'c', 0x00, 7, 0, 0, 0, 0, 0, 0, 0];
        CollectionAssert.AreEqual(expected, file.AsSpan(0, expected.Length).ToArray());

        // kPrefix: "FK 8 00000001", "PF 4 SF 5 00021", "SF 4 0002" (with internal key bytes).
        var pe = SliceTransform.CreateFixedPrefix(4);
        entries =
        [
            new Entry("00000001"u8.ToArray(), 0, EntryType.Value, []),
            new Entry("000000021"u8.ToArray(), 0, EntryType.Value, []),
            new Entry("00000002"u8.ToArray(), 0, EntryType.Value, []),
        ];
        entries.Sort(CompareEntries);
        file = Build(new TableBuilderOptions
        {
            TableFactory = TableFactoryType.PlainTable,
            PrefixExtractor = pe,
            PlainTableOptions = new PlainTableOptions { EncodingType = PlainTableEncodingType.Prefix },
        }, entries);
        var ms = new List<byte>();
        ms.Add(0x08); ms.AddRange("00000001"u8.ToArray()); ms.Add(0xFF); ms.Add(0);
        ms.Add(0x40 | 4); ms.Add(0x80 | 4); ms.AddRange("0002"u8.ToArray()); ms.Add(0xFF); ms.Add(0);
        ms.Add(0x80 | 5); ms.AddRange("00021"u8.ToArray()); ms.Add(0xFF); ms.Add(0);
        CollectionAssert.AreEqual(ms.ToArray(), file.AsSpan(0, ms.Count).ToArray());
        using TableReader r = TableReaderFactory.Open(new MemRandomAccessFile(file), new TableReaderOptions
        {
            TableFactory = TableFactoryType.PlainTable,
            PrefixExtractor = pe,
            PlainTableOptions = new PlainTableOptions(),
        });
        VerifyPlainTable((PlainTableReader)r, entries, true, new Random(1));
    }

    private static List<Entry> RandomCuckooEntries(Random rnd, int n, int keyLen, int valueLen, bool lastLevel)
    {
        var keys = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
        while (keys.Count < n)
        {
            byte[] k = new byte[keyLen];
            rnd.NextBytes(k);
            keys.Add(k);
        }
        var list = new List<Entry>();
        ulong seq = 100_000;
        foreach (byte[] k in keys)
        {
            bool del = !lastLevel && rnd.Next(6) == 0;
            byte[] v = new byte[del ? 0 : valueLen];
            rnd.NextBytes(v);
            list.Add(new Entry(k, lastLevel ? 0 : seq--, del ? EntryType.Deletion : EntryType.Value, v));
        }
        return list;
    }

    [TestMethod]
    public void CuckooTable_RandomModel()
    {
        int seed = Environment.TickCount;
        TestContext?.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        for (int round = 0; round < 40; round++)
        {
            bool lastLevel = rnd.Next(2) == 0;
            bool identity = rnd.Next(3) == 0;
            int keyLen = identity ? 8 : 4 + rnd.Next(12);
            var opts = new CuckooTableOptions
            {
                UseModuleHash = rnd.Next(2) == 0,
                IdentityAsFirstHash = identity,
                CuckooBlockSize = (uint)(1 + rnd.Next(6)),
                HashTableRatio = rnd.Next(2) == 0 ? 0.9 : 0.5,
                MaxSearchDepth = (uint)(rnd.Next(2) == 0 ? 100 : 5),
            };
            string what = "round " + round + " seed " + seed + " " + opts.ToOptionsString() + " lastLevel " + lastLevel + " keyLen " + keyLen;
            List<Entry> entries = RandomCuckooEntries(rnd, 1 + rnd.Next(1000), keyLen, rnd.Next(3) == 0 ? 0 : 1 + rnd.Next(20), lastLevel);
            var tbo = new TableBuilderOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = opts };
            byte[] file;
            try
            {
                file = Build(tbo, entries);
            }
            catch (RocksDbException ex) when (ex.Code == StatusCode.NotSupported && ex.Message.Contains("collisions", StringComparison.Ordinal))
            {
                continue; // a legitimate upstream outcome for small search depths
            }
            var ro = new TableReaderOptions { TableFactory = TableFactoryType.CuckooTable, AllowMmapReads = true };
            using TableReader reader = TableReaderFactory.Open(new MemRandomAccessFile(file), ro);
            Assert.IsInstanceOfType<CuckooTableReader>(reader);
            var buffer = new ArrayBufferWriter<byte>();
            foreach (Entry e in entries)
            {
                buffer.Clear();
                TableGetResult r = reader.Get(LookupKey(e.UserKey, DbFormat.MaxSequenceNumber), buffer, TableReadFlags.None);
                Assert.IsTrue(r.Found, what);
                Assert.AreEqual(lastLevel ? DbFormat.MaxSequenceNumber : e.Seq, r.Sequence, what);
                Assert.AreEqual(e.Type, r.Type, what);
                if (e.Type == EntryType.Value) CollectionAssert.AreEqual(e.Value, buffer.WrittenSpan.ToArray(), what);
            }
            for (int m = 0; m < 100; m++)
            {
                byte[] k = new byte[keyLen];
                rnd.NextBytes(k);
                if (entries.Exists(e => e.UserKey.AsSpan().SequenceEqual(k))) continue;
                Assert.IsFalse(reader.Get(LookupKey(k, DbFormat.MaxSequenceNumber), null, TableReadFlags.None).Found, what);
            }
            using InternalIterator it = reader.NewInternalIterator(TableReadFlags.None, null, null);
            int i = 0;
            for (it.SeekToFirst(); it.Valid; it.Next())
            {
                Entry e = entries[i++];
                Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).SequenceEqual(e.UserKey), what);
                Assert.AreEqual(lastLevel ? 0 : e.Seq, DbFormat.ExtractFooter(it.Key) >> 8, what);
            }
            Assert.AreEqual(entries.Count, i, what);
            it.SeekToLast();
            for (i = entries.Count - 1; i >= 0; i--, it.Prev())
            {
                Assert.IsTrue(it.Valid);
                Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).SequenceEqual(entries[i].UserKey), what);
            }
            Assert.IsFalse(it.Valid);
            for (int s = 0; s < 50; s++)
            {
                int j = rnd.Next(entries.Count);
                it.Seek(entries[j].InternalKey());
                Assert.IsTrue(DbFormat.ExtractUserKey(it.Key).SequenceEqual(entries[j].UserKey), what);
            }
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => it.SeekForPrev(entries[0].InternalKey())).Code);
        }
    }

    [TestMethod]
    public void CuckooTable_UpstreamErrorContracts()
    {
        var tbo = new TableBuilderOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = new CuckooTableOptions() };
        List<Entry> entries = [new Entry("aaaa"u8.ToArray(), 2, EntryType.Value, "1"u8.ToArray()), new Entry("bbbb"u8.ToArray(), 1, EntryType.Value, "2"u8.ToArray())];
        byte[] file = Build(tbo, entries);
        // Cuckoo tables can only be read memory-mapped.
        RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => TableReaderFactory.Open(new MemRandomAccessFile(file), new TableReaderOptions { TableFactory = TableFactoryType.CuckooTable }));
        Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);
        StringAssert.Contains(ex.Message, "File is not mmaped");
        Assert.AreEqual(TableMagicNumbers.CuckooTable, Coding.DecodeFixed64(file.AsSpan(file.Length - 8)));
        Assert.AreEqual(1u, Coding.DecodeFixed32(file.AsSpan(file.Length - 12)));
        // Wrong factory: bad magic number.
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(
            () => TableReaderFactory.Open(new MemRandomAccessFile(file), new TableReaderOptions { TableFactory = TableFactoryType.PlainTable })).Code);
        using (TableReader adaptive = TableReaderFactory.Open(new MemRandomAccessFile(file), new TableReaderOptions { TableFactory = TableFactoryType.AdaptiveTable, AllowMmapReads = true }))
            Assert.IsInstanceOfType<CuckooTableReader>(adaptive);

        void AddFails(Entry first, Entry second, string message)
        {
            using TableBuilder b = TableBuilder.Create(new MemWritableFile(), s_bbtFormat, tbo);
            b.Add(first.InternalKey(), first.Value);
            RocksDbException e = Assert.ThrowsExactly<RocksDbException>(() => b.Add(second.InternalKey(), second.Value));
            Assert.AreEqual(StatusCode.NotSupported, e.Code);
            StringAssert.Contains(e.Message, message);
        }
        AddFails(entries[0], new Entry("bbbbb"u8.ToArray(), 1, EntryType.Value, "2"u8.ToArray()), "all keys have to be the same size");
        AddFails(entries[0], new Entry("bbbb"u8.ToArray(), 1, EntryType.Value, "22"u8.ToArray()), "all values have to be the same size");
        AddFails(entries[0], new Entry("bbbb"u8.ToArray(), 1, EntryType.Merge, "2"u8.ToArray()), "Unsupported key type");
        AddFails(entries[0], new Entry("bbbb"u8.ToArray(), 1, EntryType.RangeDeletion, "c"u8.ToArray()), "Unsupported key type");
        // Two versions of one user key.
        using (TableBuilder b = TableBuilder.Create(new MemWritableFile(), s_bbtFormat, tbo))
        {
            b.Add(new Entry("aaaa"u8.ToArray(), 3, EntryType.Value, "1"u8.ToArray()).InternalKey(), "1"u8);
            b.Add(new Entry("aaaa"u8.ToArray(), 2, EntryType.Value, "1"u8.ToArray()).InternalKey(), "1"u8);
            Assert.AreEqual(StatusCode.NotSupported, Assert.ThrowsExactly<RocksDbException>(() => b.Finish()).Code);
        }
    }

    [TestMethod]
    public void MurmurHash64A_KnownValues()
    {
        // Values of the canonical MurmurHash64A (util/murmurhash.cc) compiled with gcc on x86-64.
        Assert.AreEqual(0x1e68d17c457bf117UL, Hash.MurmurHash64A("hello"u8, 0));
        Assert.AreEqual(0x31b9a63a69a09bd4UL, Hash.MurmurHash64A("hello"u8, unchecked(816922183u * 3u)));
        Assert.AreEqual(0x0d1aef3befc62d7eUL, Hash.MurmurHash64A("0123456789abcdefXYZ"u8, 816922183u));
        Assert.AreEqual(0x93a92d1a91a24bc7UL, Hash.MurmurHash64A("0123456789abcdef"u8, 0));
    }

    // ---- DB level ------------------------------------------------------------------------------------------------

    private static ColumnFamilyOptions PlainCf(PlainTableEncodingType enc, bool storeIndex) => new()
    {
        TableFactory = TableFactoryType.PlainTable,
        PrefixExtractor = SliceTransform.CreateFixedPrefix(4),
        PlainTableOptions = new PlainTableOptions { EncodingType = enc, StoreIndexInFile = storeIndex },
        MergeOperator = new StringAppendMergeOperator(),
        WriteBufferSize = 64 * 1024,
    };

    [TestMethod]
    public void PlainTable_Db_RandomOpsFlushCompactReopen()
    {
        int seed = Environment.TickCount;
        TestContext?.WriteLine("seed=" + seed);
        var rnd = new Random(seed);
        foreach ((PlainTableEncodingType enc, bool storeIndex) in new[] { (PlainTableEncodingType.Plain, false), (PlainTableEncodingType.Prefix, true), (PlainTableEncodingType.Prefix, false) })
        {
            var fs = DbTestUtil.NewFs();
            var oracle = new SortedDictionary<string, string>(StringComparer.Ordinal);
            for (int reopen = 0; reopen < 3; reopen++)
            {
                using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), PlainCf(enc, storeIndex));
                for (int op = 0; op < 3000; op++)
                {
                    string k = "p" + rnd.Next(40).ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/" + rnd.Next(300).ToString(System.Globalization.CultureInfo.InvariantCulture);
                    int r = rnd.Next(10);
                    if (r < 6)
                    {
                        string v = "v" + rnd.Next(1000) + new string('x', rnd.Next(40));
                        db.Put(DbTestUtil.B(k), DbTestUtil.B(v));
                        oracle[k] = v;
                    }
                    else if (r < 8)
                    {
                        db.Delete(DbTestUtil.B(k));
                        oracle.Remove(k);
                    }
                    else
                    {
                        db.Merge(DbTestUtil.B(k), "m"u8);
                        oracle[k] = oracle.TryGetValue(k, out string? old) ? old + ",m" : "m";
                    }
                    if (op % 1000 == 999) db.Flush();
                }
                if (reopen == 1) db.CompactRange();
                VerifyDb(db, oracle, rnd);
            }
        }
    }

    private static void VerifyDb(RocksDatabase db, SortedDictionary<string, string> oracle, Random rnd)
    {
        foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key), kv.Key);
        for (int i = 0; i < 100; i++)
        {
            string k = "p" + rnd.Next(40).ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/x" + i;
            Assert.IsNull(DbTestUtil.GetString(db, k));
        }
        // Prefix iteration of every prefix.
        for (int p = 0; p < 40; p++)
        {
            string prefix = "p" + p.ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/";
            var expected = new List<string>();
            foreach (string k in oracle.Keys) if (k.StartsWith(prefix, StringComparison.Ordinal)) expected.Add(k + "=" + oracle[k]);
            var actual = new List<string>();
            using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true });
            for (it.Seek(DbTestUtil.B(prefix)); it.Valid; it.Next())
                actual.Add(Encoding.UTF8.GetString(it.Key) + "=" + Encoding.UTF8.GetString(it.Value));
            CollectionAssert.AreEqual(expected, actual, prefix);
        }
    }

    [TestMethod]
    public void PlainTable_Db_TotalOrderMode()
    {
        var fs = DbTestUtil.NewFs();
        var cf = new ColumnFamilyOptions
        {
            TableFactory = TableFactoryType.PlainTable,
            PlainTableOptions = new PlainTableOptions { HashTableRatio = 0 },
        };
        var oracle = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), cf))
        {
            for (int i = 0; i < 2000; i++)
            {
                string k = "k" + (i * 7919 % 2000).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
                db.Put(DbTestUtil.B(k), DbTestUtil.B("v" + i));
                oracle[k] = "v" + i;
                if (i % 500 == 499) db.Flush();
            }
            db.CompactRange();
        }
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), cf))
        {
            foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
            using Iterator it = db.NewIterator();
            var keys = new List<string>();
            for (it.Seek("k01000"u8); it.Valid; it.Next()) keys.Add(Encoding.UTF8.GetString(it.Key));
            Assert.AreEqual(1000, keys.Count);
            Assert.AreEqual("k01000", keys[0]);
        }
    }

    [TestMethod]
    public void CuckooTable_Db_FlushCompactGet()
    {
        var fs = DbTestUtil.NewFs();
        var cf = new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable };
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.AllowMmapReads = true;
        var oracle = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, cf))
        {
            for (int f = 0; f < 3; f++)
            {
                for (int i = 0; i < 500; i++)
                {
                    string k = "key" + ((i * 31 + f * 7) % 800).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
                    string v = "val" + (f * 1000 + i).ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
                    db.Put(DbTestUtil.B(k), DbTestUtil.B(v));
                    oracle[k] = v;
                }
                db.Flush();
            }
            foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
            db.CompactRange();
            foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
            using Iterator it = db.NewIterator();
            var keys = new List<string>();
            for (it.SeekToFirst(); it.Valid; it.Next()) keys.Add(Encoding.UTF8.GetString(it.Key));
            CollectionAssert.AreEqual(new List<string>(oracle.Keys), keys);
        }
        // Without allow_mmap_reads the tables cannot be opened (DB::Open loads the table handlers).
        Assert.AreEqual(StatusCode.InvalidArgument, Assert.ThrowsExactly<RocksDbException>(
            () => RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs), cf).Dispose()).Code);
    }

    [TestMethod]
    public void AdaptiveTable_Db_ReadsMixedFormats()
    {
        var fs = DbTestUtil.NewFs();
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.AllowMmapReads = true;
        var oracle = new Dictionary<string, string>();
        void WriteWith(ColumnFamilyOptions cf, string tag)
        {
            using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, cf);
            for (int i = 0; i < 200; i++)
            {
                string k = tag + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
                db.Put(DbTestUtil.B(k), DbTestUtil.B("v" + k));
                oracle[k] = "v" + k;
            }
            db.Flush();
        }
        WriteWith(new ColumnFamilyOptions(), "bbt/");
        WriteWith(new ColumnFamilyOptions { TableFactory = TableFactoryType.AdaptiveTable, AdaptiveTableWriteFactory = TableFactoryType.PlainTable,
            PlainTableOptions = new PlainTableOptions { HashTableRatio = 0 } }, "pln/");
        WriteWith(new ColumnFamilyOptions { TableFactory = TableFactoryType.AdaptiveTable, AdaptiveTableWriteFactory = TableFactoryType.CuckooTable,
            PlainTableOptions = new PlainTableOptions { HashTableRatio = 0 } }, "cko/");
        using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, new ColumnFamilyOptions
        {
            TableFactory = TableFactoryType.AdaptiveTable,
            PlainTableOptions = new PlainTableOptions { HashTableRatio = 0 },
        }))
        {
            foreach (KeyValuePair<string, string> kv in oracle) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
        }
        // The block-based factory rejects the plain / cuckoo files (bad magic number), like upstream.
        Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(
            () => RocksDatabase.Open(DbTestUtil.DbPath, o, new ColumnFamilyOptions()).Dispose()).Code);
    }

    [TestMethod]
    public void TableOptionsSections_RoundTripNativeText()
    {
        // As written by native RocksDB 11.12 (rocksdb_options_set_plain_table_factory / set_cuckoo_table_factory).
        const string plain = "  store_index_in_file=false\n  encoding_type=kPrefix\n  huge_page_tlb_size=0\n  hash_table_ratio=0.750000\n"
            + "  full_scan_mode=false\n  bloom_bits_per_key=10\n  index_sparseness=16\n  user_key_len=0\n  ";
        const string cuckoo = "  use_module_hash=true\n  identity_as_first_hash=false\n  max_search_depth=100\n  cuckoo_block_size=5\n  hash_table_ratio=0.900000\n  ";
        var file = new OptionsFile();
        var p = new ColumnFamilyOptions { TableFactory = TableFactoryType.PlainTable };
        p.PlainTableOptions.EncodingType = PlainTableEncodingType.Prefix;
        file.ColumnFamilies.Add(new ColumnFamilyDescriptor("default", p));
        file.ColumnFamilies.Add(new ColumnFamilyDescriptor("c", new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable }));
        string text = file.Serialize();
        StringAssert.Contains(text, "[TableOptions/PlainTable \"default\"]\n" + plain);
        StringAssert.Contains(text, "[TableOptions/CuckooTable \"c\"]\n" + cuckoo);
        StringAssert.Contains(text, "table_factory=PlainTable");
        OptionsFile parsed = OptionsFile.Parse(text, preserveUnknownOptions: false);
        Assert.AreEqual(text, parsed.Serialize());
        Assert.AreEqual(PlainTableEncodingType.Prefix, parsed.ColumnFamilies[0].Options.PlainTableOptions.EncodingType);
        Assert.AreEqual(TableFactoryType.CuckooTable, parsed.ColumnFamilies[1].Options.TableFactory);
    }

    // ---- ingestion of external plain / cuckoo files ------------------------------------------------------------

    private static string WriteExternal(RocksDb.Managed.Env.IFileSystem fs, ColumnFamilyOptions cf, string path, SortedDictionary<string, string> data)
    {
        using var w = new SstFileWriter(cf, fs);
        w.Open(path);
        foreach (KeyValuePair<string, string> kv in data) w.Put(DbTestUtil.B(kv.Key), DbTestUtil.B(kv.Value));
        w.Finish();
        return path;
    }

    private static SortedDictionary<string, string> IngestData(int from, int count, string tag)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (int i = from; i < from + count; i++)
            d["key" + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture)] = tag + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
        return d;
    }

    private static void AssertComparatorMismatch(Action ingest)
    {
        RocksDbException e = Assert.ThrowsExactly<RocksDbException>(ingest);
        Assert.AreEqual(StatusCode.InvalidArgument, e.Code);
        StringAssert.Contains(e.Message, "leveldb.BytewiseComparator: does not match existing comparator");
    }

    /// <summary>
    /// ExternalSstFileIngestionJob opens external files with the column family's table factory (a file of another format
    /// fails with "Bad table magic number" in either direction) and then applies upstream's validation, which rejects
    /// plain and cuckoo files: their builders record no comparator name (InvalidArgument "... does not match existing
    /// comparator "), and SstFileWriter cuckoo files lack the external-file version property (the cuckoo builder runs no
    /// collectors: Corruption "External file version not found", unless allow_db_generated_files).
    /// </summary>
    [TestMethod]
    public void Ingestion_PlainAndCuckooExternalFiles_UpstreamValidation()
    {
        var cuckoo = new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable };
        foreach (ColumnFamilyOptions cf in new[] { PlainCf(PlainTableEncodingType.Prefix, storeIndex: true), PlainCf(PlainTableEncodingType.Plain, false), cuckoo })
        {
            var fs = DbTestUtil.NewFs();
            DbOptions o = DbTestUtil.NewOptions(fs);
            o.AllowMmapReads = true;
            fs.CreateDirectoryIfMissing("/ext");
            string fa = WriteExternal(fs, cf, "/ext/a.sst", IngestData(0, 400, "a"));
            using (RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, cf))
            {
                if (cf.TableFactory == TableFactoryType.CuckooTable)
                {
                    RocksDbException noVersion = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles([fa], new IngestExternalFileOptions()));
                    Assert.AreEqual(StatusCode.Corruption, noVersion.Code);
                    StringAssert.Contains(noVersion.Message, "External file version not found");
                    AssertComparatorMismatch(() => db.IngestExternalFiles([fa], new IngestExternalFileOptions { AllowDbGeneratedFiles = true }));
                }
                else
                {
                    AssertComparatorMismatch(() => db.IngestExternalFiles([fa], new IngestExternalFileOptions()));
                }
                // A block-based file cannot be opened by a plain / cuckoo column family's factory.
                string bbt = WriteExternal(fs, new ColumnFamilyOptions(), "/ext/bbt.sst", IngestData(5000, 10, "c"));
                RocksDbException bad = Assert.ThrowsExactly<RocksDbException>(() => db.IngestExternalFiles([bbt], new IngestExternalFileOptions()));
                Assert.AreEqual(StatusCode.Corruption, bad.Code);
                StringAssert.Contains(bad.Message, "Bad table magic number");
                Assert.IsNull(db.Get(DbTestUtil.B("key00001")));
                Assert.AreEqual(0, db.GetLiveFilesMetadata().Count);
            }
            // ... nor a plain / cuckoo file by a block-based column family's.
            var fs2 = DbTestUtil.NewFs();
            fs2.CreateDirectoryIfMissing("/ext");
            string f2 = WriteExternal(fs2, cf, "/ext/x.sst", IngestData(0, 10, "x"));
            using RocksDatabase bdb = RocksDatabase.Open(DbTestUtil.DbPath, DbTestUtil.NewOptions(fs2), new ColumnFamilyOptions());
            RocksDbException magic = Assert.ThrowsExactly<RocksDbException>(() => bdb.IngestExternalFiles([f2], new IngestExternalFileOptions()));
            Assert.AreEqual(StatusCode.Corruption, magic.Code);
            StringAssert.Contains(magic.Message, "Bad table magic number");
        }
    }

    /// <summary>
    /// The adaptive factory opens external files of any format by their magic number: block-based files are ingested
    /// into an adaptive column family; plain and cuckoo files reach upstream's validation (and are rejected there).
    /// </summary>
    [TestMethod]
    public void Ingestion_AdaptiveTableOpensEveryFormat()
    {
        var fs = DbTestUtil.NewFs();
        DbOptions o = DbTestUtil.NewOptions(fs);
        o.AllowMmapReads = true;
        fs.CreateDirectoryIfMissing("/ext");
        SortedDictionary<string, string> d0 = IngestData(0, 200, "f0");
        string bbt = WriteExternal(fs, new ColumnFamilyOptions(), "/ext/f0.sst", d0);
        string plain = WriteExternal(fs, PlainCf(PlainTableEncodingType.Plain, true), "/ext/f1.sst", IngestData(1000, 200, "f1"));
        string cuckoo = WriteExternal(fs, new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable }, "/ext/f2.sst", IngestData(2000, 200, "f2"));
        var cf = PlainCf(PlainTableEncodingType.Plain, true);
        cf.TableFactory = TableFactoryType.AdaptiveTable;
        using RocksDatabase db = RocksDatabase.Open(DbTestUtil.DbPath, o, cf);
        db.IngestExternalFiles([bbt], new IngestExternalFileOptions());
        foreach (KeyValuePair<string, string> kv in d0) Assert.AreEqual(kv.Value, DbTestUtil.GetString(db, kv.Key));
        AssertComparatorMismatch(() => db.IngestExternalFiles([plain], new IngestExternalFileOptions()));
        AssertComparatorMismatch(() => db.IngestExternalFiles([cuckoo], new IngestExternalFileOptions { AllowDbGeneratedFiles = true }));
        Assert.AreEqual(1, db.GetLiveFilesMetadata().Count);
    }

    public TestContext? TestContext { get; set; }
}
