using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Cuckoo;
using RocksDb.Managed.Table.Plain;
using RocksDb.Managed.Table.TrieIndex;
using RocksDb.Managed.Tests.Table;
using RocksDb.Managed.Tracing;
using RocksDb.Managed.Util;
using static RocksDb.Managed.Tests.Db.DbTestUtil;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #4, slice AltTables: plain / cuckoo tables, the LOUDS trie index and the trace formats / replayer. Each test
/// names the C++ function it pins down.
/// </summary>
[TestClass]
public sealed class Audit4AltTablesTests
{
    private static readonly TableFormatOptions s_bbtFormat = TableFactory.CreateFormatOptions(new BlockBasedTableOptions());

    private static byte[] IKey(ReadOnlySpan<byte> userKey, ulong seq, EntryType type)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, seq, type);
        return k;
    }

    private static byte[] Build(TableBuilderOptions tbo, IReadOnlyList<(byte[] Key, byte[] Value)> entries)
    {
        var f = new MemWritableFile();
        using (TableBuilder b = TableBuilder.Create(f, s_bbtFormat, tbo))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
        }
        return f.ToArray();
    }

    /// <summary>
    /// Overwrites the start of a property value in the properties block. Keys are delta-encoded there (one restart
    /// point), so the entry is located by the longest name suffix that is directly followed by the current value.
    /// </summary>
    private static byte[] PatchProperty(byte[] file, string name, byte[] currentValue, ReadOnlySpan<byte> newValuePrefix)
    {
        byte[] patched = (byte[])file.Clone();
        byte[] nameBytes = Encoding.ASCII.GetBytes(name);
        for (int start = 0; start < nameBytes.Length; start++)
        {
            byte[] pattern = [.. nameBytes.AsSpan(start), .. currentValue];
            int at = patched.AsSpan().LastIndexOf(pattern);
            if (at < 0) continue;
            newValuePrefix.CopyTo(patched.AsSpan(at + nameBytes.Length - start));
            return patched;
        }
        Assert.Fail("property not found: " + name);
        return patched;
    }

    /// <summary>Runs <paramref name="action"/> on malformed input: it must fail with Corruption, not another exception.</summary>
    private static void AssertCorruption(Action action)
    {
        try
        {
            action();
        }
        catch (RocksDbException e)
        {
            Assert.AreEqual(StatusCode.Corruption, e.Code, e.Message);
            return;
        }
        Assert.Fail("malformed input was accepted");
    }

    // ---- PlainTable -------------------------------------------------------------------------------------------

    /// <summary>
    /// PlainTableBuilder's constructor (plain_table_builder.cc): format_version comes from the configured
    /// encoding_type (`(encoding_type == kPlain) ? 0 : 1`), while the encoding-type property is
    /// encoder_.GetEncodingType(), which falls back to kPlain without a prefix extractor (plain_table_key_coding.h).
    /// So kPrefix without a prefix extractor records format_version 1 and encoding type 0.
    /// </summary>
    [TestMethod]
    public void PlainTable_PrefixEncodingWithoutExtractor_FormatVersionFollowsConfiguredEncoding()
    {
        var opts = new PlainTableOptions { EncodingType = PlainTableEncodingType.Prefix, HashTableRatio = 0 };
        var entries = new List<(byte[], byte[])>
        {
            (IKey("abc1"u8, 7, EntryType.Value), "v1"u8.ToArray()),
            (IKey("abc2"u8, 0, EntryType.Value), "v2"u8.ToArray()),
        };
        byte[] file = Build(new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts }, entries);
        using var reader = (PlainTableReader)TableReaderFactory.Open(new MemRandomAccessFile(file),
            new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts });
        Assert.AreEqual(1UL, reader.Properties.FormatVersion);
        Assert.AreEqual(PlainTableEncodingType.Plain, reader.EncodingType);
        Assert.AreEqual(0u, Coding.DecodeFixed32(reader.Properties.UserCollectedProperties[PlainTablePropertyNames.EncodingType]));
        // The keys were written in kPlain and read back as such.
        var buffer = new ArrayBufferWriter<byte>();
        Assert.IsTrue(reader.Get(IKey("abc2"u8, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), buffer, TableReadFlags.None).Found);
        CollectionAssert.AreEqual("v2"u8.ToArray(), buffer.WrittenSpan.ToArray());
    }

    /// <summary>
    /// PlainTableKeyDecoder::ReadInternalKey (plain_table_key_coding.cc): file_reader_.Read(file_offset,
    /// user_key_size + 1) fails for a key size the data cannot hold. A corrupt size of 2^32-1 must not wrap the
    /// "+ 1" around to a zero-length read (then indexed at -1): kPlain varint size and kPrefix inline-size escape.
    /// </summary>
    [TestMethod]
    public void PlainTable_KeySizeNearUInt32Max_IsCorruption()
    {
        // kPlain, variable length: varint32 0xFFFFFFFF, then some bytes.
        byte[] plain = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        var d1 = new PlainTableKeyDecoder(plain, (uint)plain.Length, PlainTableEncodingType.Plain, PlainTableOptions.VariableLength);
        AssertCorruption(() => d1.NextKey(0, out _));
        // kPrefix: full key flag byte with the 0x3F escape, varint32 0xFFFFFFFF - 0x3F.
        byte[] prefix = [0x3F, 0xC0, 0xFF, 0xFF, 0xFF, 0x0F, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        var d2 = new PlainTableKeyDecoder(prefix, (uint)prefix.Length, PlainTableEncodingType.Prefix, PlainTableOptions.VariableLength);
        AssertCorruption(() => d2.NextKey(0, out _));
        // Sizes whose "+ 8" would wrap fail the same way.
        byte[] plain2 = [0xFA, 0xFF, 0xFF, 0xFF, 0x0F, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        var d3 = new PlainTableKeyDecoder(plain2, (uint)plain2.Length, PlainTableEncodingType.Plain, PlainTableOptions.VariableLength);
        AssertCorruption(() => d3.NextKey(0, out _));
    }

    /// <summary>
    /// PlainTableReader::PopulateIndex -> PlainTableBloomV1::SetRawData(data, size * 8, num_blocks): the stored
    /// num_blocks property addresses cache lines of the bloom block. A property naming more blocks than the block
    /// holds (out-of-bounds reads upstream) must be rejected as Corruption, not surface as an index error in Get.
    /// </summary>
    [TestMethod]
    public void PlainTable_BloomNumBlocksBeyondBloomBlock_IsCorruption()
    {
        var opts = new PlainTableOptions { HashTableRatio = 0, StoreIndexInFile = true, BloomBitsPerKey = 10 };
        var entries = new List<(byte[], byte[])>();
        for (int i = 0; i < 100; i++) entries.Add((IKey(Encoding.ASCII.GetBytes("key" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)), 5, EntryType.Value), "v"u8.ToArray()));
        byte[] file = Build(new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, BloomLocality = 1 }, entries);
        var ro = new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, BloomLocality = 1 };
        byte[] numBlocks;
        using (TableReader ok = TableReaderFactory.Open(new MemRandomAccessFile(file), ro))
        {
            foreach ((byte[] k, _) in entries) Assert.IsTrue(ok.Get(k, null, TableReadFlags.None).Found);
            numBlocks = ok.Properties.UserCollectedProperties[PlainTablePropertyNames.NumBloomBlocks];
        }
        // varint32 num_blocks (3 cache lines for 1000 bits) -> 127.
        CollectionAssert.AreEqual(new byte[] { 3 }, numBlocks);
        byte[] bad = PatchProperty(file, PlainTablePropertyNames.NumBloomBlocks, numBlocks, [0x7F]);
        AssertCorruption(() =>
        {
            using TableReader r = TableReaderFactory.Open(new MemRandomAccessFile(bad), ro);
            foreach ((byte[] k, _) in entries) r.Get(k, null, TableReadFlags.None);
        });
    }

    // ---- CuckooTable ------------------------------------------------------------------------------------------

    private static byte[] BuildCuckoo(out List<(byte[] Key, byte[] Value)> entries)
    {
        entries = new List<(byte[], byte[])>();
        for (int i = 0; i < 50; i++)
            entries.Add((IKey(Encoding.ASCII.GetBytes("k" + i.ToString("D5", System.Globalization.CultureInfo.InvariantCulture)), (ulong)(100 + i), EntryType.Value), "valu"u8.ToArray()));
        return Build(new TableBuilderOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = new CuckooTableOptions { CuckooBlockSize = 5 } }, entries);
    }

    private static Dictionary<string, byte[]> ReadEverything(byte[] file, List<(byte[] Key, byte[] Value)> entries)
    {
        using TableReader r = TableReaderFactory.Open(new MemRandomAccessFile(file), new TableReaderOptions { TableFactory = TableFactoryType.CuckooTable, AllowMmapReads = true });
        foreach ((byte[] k, _) in entries) r.Get(k, null, TableReadFlags.None);
        using InternalIterator it = r.NewInternalIterator(TableReadFlags.None, null, null);
        for (it.SeekToFirst(); it.Valid; it.Next()) _ = it.Value.Length;
        return r.Properties.UserCollectedProperties;
    }

    /// <summary>
    /// CuckooTableReader's constructor (cuckoo_table_reader.cc): bucket_length_ = key_length_ + value_length_ and the
    /// table spans table_size_ + cuckoo_block_size_ - 1 buckets. Property values for which these computations wrap
    /// around (32-bit bucket length, 64-bit bucket count) describe a table the file cannot hold: Corruption, not an
    /// out-of-range slice in Get / the iterator.
    /// </summary>
    [TestMethod]
    public void CuckooTable_PropertiesThatOverflowTheTableGeometry_AreCorruption()
    {
        byte[] file = BuildCuckoo(out List<(byte[] Key, byte[] Value)> entries);
        Dictionary<string, byte[]> props = ReadEverything(file, entries); // sanity: the unpatched file reads
        // value length 0xFFFFFFFF: key_length_ + value_length_ wraps to key_length_ - 1.
        byte[] badValueLength = PatchProperty(file, CuckooTablePropertyNames.ValueLength, props[CuckooTablePropertyNames.ValueLength], [0xFF, 0xFF, 0xFF, 0xFF]);
        AssertCorruption(() => ReadEverything(badValueLength, entries));
        // hash table size 2^64 - 1: table_size_ + cuckoo_block_size_ - 1 wraps to 3 buckets.
        byte[] badTableSize = PatchProperty(file, CuckooTablePropertyNames.HashTableSize, props[CuckooTablePropertyNames.HashTableSize], [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        AssertCorruption(() => ReadEverything(badTableSize, entries));
    }

    // ---- LOUDS trie -------------------------------------------------------------------------------------------

    /// <summary>
    /// LoudsTrieBuilder / LoudsTrieIterator (utilities/trie_index/louds_trie.cc) with keys made of 0x00 / 0xff bytes
    /// (many prefix keys; the first label of a node is 0x00, the last 0xff): Seek is a lower bound, also for the
    /// empty target and for targets past the last key, Next / Prev walk the sorted order.
    /// </summary>
    [TestMethod]
    public void LoudsTrie_EmptyKeyAndExtremeBytes_MatchSortedModel()
    {
        var rnd = new Random(4242);
        for (int round = 0; round < 40; round++)
        {
            var set = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (a, b) => a.AsSpan().SequenceCompareTo(b)));
            int n = 1 + rnd.Next(round < 5 ? 3 : 300);
            byte[] alphabet = round % 2 == 0 ? [0x00, 0xFF] : [0x00, 0x01, 0x7F, 0xFE, 0xFF];
            for (int i = 0; i < n; i++)
            {
                byte[] k = new byte[rnd.Next(1, 10)];
                for (int j = 0; j < k.Length; j++) k[j] = alphabet[rnd.Next(alphabet.Length)];
                set.Add(k);
            }
            List<byte[]> keys = [.. set];
            byte[] data;
            using (var b = new LoudsTrieBuilder())
            {
                for (int i = 0; i < keys.Count; i++) b.AddKey(keys[i], new TrieBlockHandle((ulong)i * 100, 50));
                b.Finish();
                data = b.SerializedData.ToArray();
            }
            LoudsTrie trie = LoudsTrie.Read(data);
            Assert.AreEqual((ulong)keys.Count, trie.NumKeys);
            var it = new LoudsTrieIterator(trie);
            string ctx = "round " + round;
            // Forward and backward scans.
            Assert.IsTrue(it.SeekToFirst(), ctx);
            for (int i = 0; i < keys.Count; i++)
            {
                CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), ctx + " next " + i);
                Assert.AreEqual((ulong)i * 100, it.Value.Offset, ctx);
                Assert.AreEqual(i + 1 < keys.Count, it.Next(), ctx);
            }
            Assert.IsTrue(it.SeekToLast(), ctx);
            for (int i = keys.Count - 1; i >= 0; i--)
            {
                CollectionAssert.AreEqual(keys[i], it.Key.ToArray(), ctx + " prev " + i);
                Assert.AreEqual(i > 0, it.Prev(), ctx);
            }
            // Lower-bound seeks: every key, every key's prefixes / successors, random targets.
            var targets = new List<byte[]> { Array.Empty<byte>(), new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF } };
            foreach (byte[] k in keys)
            {
                targets.Add(k);
                targets.Add([.. k, 0x00]);
                targets.Add([.. k, 0xFF]);
                if (k.Length > 0) targets.Add(k[..^1]);
            }
            for (int i = 0; i < 100; i++)
            {
                byte[] t = new byte[rnd.Next(0, 12)];
                for (int j = 0; j < t.Length; j++) t[j] = rnd.Next(3) == 0 ? (byte)rnd.Next(256) : alphabet[rnd.Next(alphabet.Length)];
                targets.Add(t);
            }
            foreach (byte[] t in targets)
            {
                int lb = 0;
                while (lb < keys.Count && keys[lb].AsSpan().SequenceCompareTo(t) < 0) lb++;
                string what = ctx + " target " + Convert.ToHexString(t);
                Assert.AreEqual(lb < keys.Count, it.Seek(t), what);
                if (lb == keys.Count) continue;
                CollectionAssert.AreEqual(keys[lb], it.Key.ToArray(), what);
                Assert.AreEqual((ulong)lb * 100, it.Value.Offset, what);
                if (rnd.Next(2) == 0)
                {
                    Assert.AreEqual(lb + 1 < keys.Count, it.Next(), what);
                    if (lb + 1 < keys.Count) CollectionAssert.AreEqual(keys[lb + 1], it.Key.ToArray(), what);
                }
                else
                {
                    Assert.AreEqual(lb > 0, it.Prev(), what);
                    if (lb > 0) CollectionAssert.AreEqual(keys[lb - 1], it.Key.ToArray(), what);
                }
            }
        }
    }

    // ---- Trace format / replayer ------------------------------------------------------------------------------

    private static byte[] Record(ulong ts, TraceType type, ReadOnlySpan<byte> payload)
    {
        var w = new ArrayBufferWriter<byte>();
        TraceFormat.EncodeTrace(w, ts, type, payload);
        return w.WrittenSpan.ToArray();
    }

    private static byte[] GetPayload(uint cfId, ReadOnlySpan<byte> key)
    {
        var w = new ArrayBufferWriter<byte>();
        Coding.PutFixed64(w, (1UL << (int)TracePayloadType.GetCfId) | (1UL << (int)TracePayloadType.GetKey));
        Coding.PutFixed32(w, cfId);
        Coding.PutLengthPrefixedSlice(w, key);
        return w.WrittenSpan.ToArray();
    }

    /// <summary>A trace reader over in-memory encoded records (a user-supplied TraceReader).</summary>
    private sealed class ListTraceReader(List<byte[]> records) : TraceReader
    {
        private int _next;

        public override bool TryRead(IBufferWriter<byte> destination)
        {
            if (_next >= records.Count) return false;
            destination.Write(records[_next++]);
            return true;
        }

        public override void Reset() => _next = 0;
    }

    /// <summary>
    /// TracerHelper::DecodeTraceRecord, kTraceMultiGet (trace_replay.cc): multiget_size comes from the record. A size
    /// the cf_ids / keys payloads cannot hold is a malformed record (Corruption), not an allocation failure.
    /// </summary>
    [TestMethod]
    public void Trace_MultiGetSizeBeyondPayload_IsCorruption()
    {
        var w = new ArrayBufferWriter<byte>();
        Coding.PutFixed64(w, (1UL << (int)TracePayloadType.MultiGetSize) | (1UL << (int)TracePayloadType.MultiGetCfIds) | (1UL << (int)TracePayloadType.MultiGetKeys));
        Coding.PutFixed32(w, 0xFFFFFFFF);
        Coding.PutLengthPrefixedSlice(w, new byte[4]);
        Coding.PutLengthPrefixedSlice(w, [1, (byte)'k']);
        byte[] payload = w.WrittenSpan.ToArray();
        AssertCorruption(() => TraceFormat.DecodeRecord(new TraceEntry(1, TraceType.MultiGet, payload), 2));
        // Two keys announced, one present.
        Coding.EncodeFixed32(payload.AsSpan(8), 2);
        AssertCorruption(() => TraceFormat.DecodeRecord(new TraceEntry(1, TraceType.MultiGet, payload), 2));
        // The well-formed record decodes.
        Coding.EncodeFixed32(payload.AsSpan(8), 1);
        var rec = (MultiGetQueryTraceRecord)TraceFormat.DecodeRecord(new TraceEntry(1, TraceType.MultiGet, payload), 2);
        Assert.AreEqual(1, rec.Keys.Count);
        CollectionAssert.AreEqual("k"u8.ToArray(), rec.Keys[0].ToArray());
    }

    /// <summary>
    /// ReplayerImpl::Replay (replayer_impl.cc): a record TracerHelper::DecodeTrace cannot frame is
    /// Status::Incomplete, which ends the read loop like the end of the data; with no failed record Replay returns OK
    /// ("if (s.IsIncomplete() &amp;&amp; !record_failed) { trace_end_ = true; return Status::OK(); }").
    /// </summary>
    [TestMethod]
    public void Replay_RecordShorterThanItsMetadata_EndsTheReplayLikeEndOfData()
    {
        foreach (uint threads in new uint[] { 1, 3 })
        {
            MemoryFileSystem fs = NewFs();
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
            db.Put(B("a"), B("1"));
            var records = new List<byte[]>
            {
                Record(10, TraceType.Begin, TraceFormat.QueryHeaderPayload()),
                Record(11, TraceType.Get, GetPayload(0, B("a"))),
                new byte[] { 1, 2, 3, 4, 5 }, // shorter than kTraceMetadataSize
                Record(12, TraceType.Get, GetPayload(0, B("a"))),
            };
            using Replayer r = db.NewDefaultReplayer(new ListTraceReader(records));
            r.Prepare();
            int results = 0;
            r.Replay(new ReplayOptions { NumThreads = threads, FastForward = 1000 }, (e, res) =>
            {
                Assert.IsNull(e);
                Interlocked.Increment(ref results);
            });
            Assert.AreEqual(1, results, "threads " + threads);
            // The trace has ended: Replay again reports Incomplete "Trace end.".
            Assert.AreEqual(StatusCode.Incomplete, Assert.ThrowsExactly<RocksDbException>(() => r.Replay(new ReplayOptions(), null)).Code);
        }
    }

    /// <summary>
    /// ReplayerImpl::Replay + TraceExecutionHandler::Handle(GetQueryTraceRecord): a record of a type the query replayer
    /// does not know reports NotSupported to the callback and the replay continues; a Get on a column family the
    /// handler has no handle for fails the replay with Corruption "Invalid Column Family ID.".
    /// </summary>
    [TestMethod]
    public void Replay_UnknownRecordTypeContinues_UnknownColumnFamilyFails()
    {
        foreach (uint threads in new uint[] { 1, 3 })
        {
            MemoryFileSystem fs = NewFs();
            using RocksDatabase db = RocksDatabase.Open(DbPath, NewOptions(fs));
            db.Put(B("a"), B("1"));
            var records = new List<byte[]>
            {
                Record(10, TraceType.Begin, TraceFormat.QueryHeaderPayload()),
                Record(11, TraceType.BlockDataBlock, [1, 2, 3]),
                Record(12, TraceType.Get, GetPayload(0, B("a"))),
                Record(13, TraceType.Get, GetPayload(77, B("a"))),
                Record(14, TraceType.End, default),
            };
            using Replayer r = db.NewDefaultReplayer(new ListTraceReader(records));
            r.Prepare();
            var codes = new System.Collections.Concurrent.ConcurrentBag<StatusCode>();
            RocksDbException ex = Assert.ThrowsExactly<RocksDbException>(() => r.Replay(new ReplayOptions { NumThreads = threads, FastForward = 1000 },
                (e, res) => codes.Add(e?.Code ?? StatusCode.Ok)));
            Assert.AreEqual(StatusCode.Corruption, ex.Code, "threads " + threads);
            StringAssert.Contains(ex.Message, "Invalid Column Family ID.");
            List<StatusCode> sorted = [.. codes];
            sorted.Sort();
            List<StatusCode> expected = [StatusCode.NotSupported, StatusCode.Ok, StatusCode.Corruption];
            expected.Sort();
            CollectionAssert.AreEqual(expected, sorted, "threads " + threads);
        }
    }
}
