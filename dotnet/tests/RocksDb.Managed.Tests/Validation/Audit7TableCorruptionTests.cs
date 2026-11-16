using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Cuckoo;
using RocksDb.Managed.Table.Plain;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Tests.Table;
using CompressionType = RocksDb.Managed.Compression.CompressionType;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Validation;

/// <summary>
/// Audit #7, area TableCorruption: untrusted SST bytes on the read path. Corrupt or truncated table files must fail
/// with Corruption (or the status upstream returns), never with an unrelated exception type or a hang. Each test
/// names the C++ function whose behavior it pins.
/// </summary>
[TestClass]
public sealed class Audit7TableCorruptionTests
{
    private sealed class Batch(List<byte[]> lookupKeys) : ITableMultiGetBatch
    {
        public ReadOnlySpan<byte> InternalKey(int key) => lookupKeys[key];
        public ReadOnlyMemory<byte> InternalKeyMemory(int key) => lookupKeys[key];
        public bool Visit(int key, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value) => true;
        public void SetIncomplete(int key) { }
    }

    private static readonly TableEntryVisitor<int> s_count =
        static (ref int n, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) =>
        {
            n++;
            return true;
        };

    private sealed record Built(byte[] Data, TableReaderOptions ReaderOptions, List<byte[]> Lookups, string Trace);

    private static Built BuildRandomTable(Random rnd, int seed)
    {
        IndexType indexType = (IndexType)rnd.Next(4);
        SliceTransform? pe = indexType == IndexType.HashSearch || rnd.Next(3) == 0 ? SliceTransform.CreateFixedPrefix(3) : null;
        int fv = rnd.Next(2, 9);
        FilterPolicy? policy = rnd.Next(4) switch
        {
            0 => null,
            1 => new RibbonFilterPolicy(10, -1),
            _ => new BloomFilterPolicy(rnd.Next(1, 20)),
        };
        CompressionType ctype = (new[] { CompressionType.None, CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })[rnd.Next(5)];
        ChecksumType checksum = rnd.Next(2) == 0 ? ChecksumType.NoChecksum : (ChecksumType)rnd.Next(1, 5);
        var format = new TableFormatOptions
        {
            BlockSize = rnd.Next(64, 600),
            BlockRestartInterval = rnd.Next(1, 9),
            IndexBlockRestartInterval = indexType == IndexType.HashSearch ? 1 : rnd.Next(1, 4),
            MetadataBlockSize = rnd.Next(32, 200),
            FormatVersion = fv,
            Checksum = checksum,
            SeparateKeyValueInDataBlock = fv >= 8 && rnd.Next(2) == 0,
            OptimizeKeyCommonPrefix = (OptimizeKeyCommonPrefix)rnd.Next(3),
            IndexType = indexType,
            DataBlockIndexType = (DataBlockIndexType)rnd.Next(2),
            UseDeltaEncoding = rnd.Next(4) != 0,
            FilterPolicy = policy,
            WholeKeyFiltering = pe is null || rnd.Next(2) == 0,
            PartitionFilters = indexType == IndexType.TwoLevelIndexSearch && policy is not null && rnd.Next(2) == 0,
        };
        var compression = new CompressionOptions { MaxDictBytes = ctype != CompressionType.None && rnd.Next(3) == 0 ? 256u : 0u };

        var entries = new List<(byte[] Key, byte[] Value)>();
        int n = rnd.Next(1, 150);
        var lookups = new List<byte[]>();
        for (int i = 0; i < n; i++)
        {
            byte[] uk = System.Text.Encoding.ASCII.GetBytes("k" + (i * 7).ToString("D5", System.Globalization.CultureInfo.InvariantCulture));
            EntryType t = rnd.Next(10) switch { 0 => EntryType.Deletion, 1 => EntryType.Merge, 2 => EntryType.RangeDeletion, _ => EntryType.Value };
            byte[] value;
            if (t == EntryType.RangeDeletion) value = System.Text.Encoding.ASCII.GetBytes("k" + (i * 7 + 3).ToString("D5", System.Globalization.CultureInfo.InvariantCulture));
            else
            {
                value = new byte[rnd.Next(0, 60)];
                if (rnd.Next(2) == 0) rnd.NextBytes(value); else value.AsSpan().Fill((byte)'v');
            }
            entries.Add((TableTestUtil.IKey(uk, (ulong)(100 + i), t), value));
            if (rnd.Next(3) == 0) lookups.Add(TableTestUtil.IKey(uk, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
        }
        lookups.Add(TableTestUtil.IKey("a"u8, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
        lookups.Add(TableTestUtil.IKey("zz"u8, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
        lookups.Sort((a, b) => InternalKeyComparator.Bytewise.Compare(a, b));
        byte[] data = TableTestUtil.BuildTable(entries, format,
            new TableBuilderOptions { PrefixExtractor = pe, CompressionType = ctype, CompressionOptions = compression });
        var ro = new TableReaderOptions { FilterPolicy = policy, PrefixExtractor = pe, MaxAutoReadaheadSize = rnd.Next(2) == 0 ? 0 : 256 * 1024 };
        string trace = $"seed={seed} index={indexType} fv={fv} checksum={checksum} comp={ctype} dict={compression.MaxDictBytes} filter={policy?.GetType().Name} "
            + $"pf={format.PartitionFilters} hash={format.DataBlockIndexType} sep={format.SeparateKeyValueInDataBlock} pe={pe is not null} n={n} size={data.Length}";
        return new Built(data, ro, lookups, trace);
    }

    private static byte[] Mutate(Random rnd, byte[] data, out string what)
    {
        byte[] d = (byte[])data.Clone();
        int kind = rnd.Next(7);
        // Bias positions towards the tail (footer, metaindex, properties, index) half of the time.
        int Pos() => d.Length == 0 ? 0 : rnd.Next(2) == 0 ? rnd.Next(d.Length) : Math.Max(0, d.Length - 1 - rnd.Next(Math.Min(d.Length, 400)));
        switch (kind)
        {
            case 0:
            {
                int flips = rnd.Next(1, 4);
                var sb = new System.Text.StringBuilder("flip");
                for (int i = 0; i < flips && d.Length > 0; i++)
                {
                    int p = Pos();
                    int bit = rnd.Next(8);
                    d[p] ^= (byte)(1 << bit);
                    sb.Append(' ').Append(p).Append(':').Append(bit);
                }
                what = sb.ToString();
                return d;
            }
            case 1:
            {
                int p = Pos();
                if (d.Length == 0) { what = "noop"; return d; }
                byte b = (byte)rnd.Next(256);
                d[p] = b;
                what = $"set {p}={b}";
                return d;
            }
            case 2:
            {
                int len = rnd.Next(2) == 0 ? rnd.Next(d.Length + 1) : Math.Max(0, d.Length - rnd.Next(1, 80));
                what = $"truncate {len}";
                return d.AsSpan(0, len).ToArray();
            }
            case 3:
            {
                // A run of 0xFF (over-long varints, huge lengths) or 0x00.
                if (d.Length == 0) { what = "noop"; return d; }
                int p = Pos();
                int len = Math.Min(d.Length - p, rnd.Next(1, 12));
                byte b = rnd.Next(2) == 0 ? (byte)0xFF : (byte)0;
                d.AsSpan(p, len).Fill(b);
                what = $"fill {p}+{len}={b}";
                return d;
            }
            case 4:
            {
                // Random bytes over a range.
                if (d.Length == 0) { what = "noop"; return d; }
                int p = Pos();
                int len = Math.Min(d.Length - p, rnd.Next(1, 40));
                rnd.NextBytes(d.AsSpan(p, len));
                what = $"random {p}+{len}";
                return d;
            }
            case 5:
            {
                // Insert or remove bytes (shifts every later offset).
                int p = Pos();
                int len = rnd.Next(1, 16);
                if (rnd.Next(2) == 0 && p + len <= d.Length)
                {
                    what = $"remove {p}+{len}";
                    byte[] r = new byte[d.Length - len];
                    d.AsSpan(0, p).CopyTo(r);
                    d.AsSpan(p + len).CopyTo(r.AsSpan(p));
                    return r;
                }
                what = $"insert {p}+{len}";
                byte[] ins = new byte[d.Length + len];
                d.AsSpan(0, p).CopyTo(ins);
                rnd.NextBytes(ins.AsSpan(p, len));
                d.AsSpan(p).CopyTo(ins.AsSpan(p + len));
                return ins;
            }
            default:
            {
                // Overwrite an aligned fixed32/fixed64 near the tail with an extreme value (counts, offsets).
                if (d.Length < 8) { what = "noop"; return d; }
                int p = Math.Max(0, Pos() - 8);
                ulong v = rnd.Next(4) switch { 0 => ulong.MaxValue, 1 => 0x7FFFFFFF, 2 => 0x80000000, _ => (ulong)rnd.Next() };
                int len = rnd.Next(2) == 0 ? 4 : 8;
                if (len == 4) System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(d.AsSpan(p, 4), (uint)v);
                else System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(d.AsSpan(p, 8), v);
                what = $"word {p}+{len}={v:X}";
                return d;
            }
        }
    }

    /// <summary>Every read operation the table reader offers, with bounded loops (a hang shows up as the watchdog).</summary>
    private static async Task ExerciseAsync(byte[] data, Built b, bool verifyChecksums, Random rnd)
    {
        TableReaderOptions ro = b.ReaderOptions with { VerifyChecksums = verifyChecksums };
        TableReadFlags flags = verifyChecksums ? TableReadFlags.None : TableReadFlags.SkipChecksumVerification;
        TableReader reader = rnd.Next(4) == 0
            ? TableReaderFactory.Open(new MemRandomAccessFile(data), ro with { TableFactory = TableFactoryType.AdaptiveTable })
            : rnd.Next(2) == 0
                ? await BlockBasedTableReader.OpenAsync(new MemRandomAccessFile(data), ro)
                : BlockBasedTableReader.Open(new MemRandomAccessFile(data), ro);
        try
        {
            _ = reader.Properties.NumEntries;
            _ = reader.ApproximateMemoryUsage;
            foreach (byte[] lk in b.Lookups)
            {
                int cnt = 0;
                reader.Get(lk, ref cnt, s_count, flags);
                _ = reader.Get(lk, new ArrayBufferWriter<byte>(), flags);
                _ = reader.InternalKeyMayMatch(lk, flags);
                _ = reader.ApproximateOffsetOf(lk);
            }
            if (b.Lookups.Count > 0)
            {
                await reader.GetAsync(b.Lookups[0], new ArrayBufferWriter<byte>(), flags, CancellationToken.None);
                _ = reader.ApproximateSize(b.Lookups[0], b.Lookups[^1]);
            }
            int[] keys = Enumerable.Range(0, b.Lookups.Count).ToArray();
            await reader.MultiGetAsync(new Batch(b.Lookups), keys, keys.Length, new bool[keys.Length], flags, rnd.Next(2) == 0, CancellationToken.None);
            _ = reader.ApproximateKeyAnchors();

            using (InternalIterator it = reader.NewInternalIterator(flags, null, b.ReaderOptions.PrefixExtractor))
            {
                int steps = 0;
                for (it.SeekToFirst(); it.Valid && steps < 5000; it.Next()) steps++;
                Assert.IsTrue(steps < 5000, "forward scan did not terminate");
                steps = 0;
                for (it.SeekToLast(); it.Valid && steps < 5000; it.Prev()) steps++;
                Assert.IsTrue(steps < 5000, "backward scan did not terminate");
                foreach (byte[] lk in b.Lookups)
                {
                    it.Seek(lk);
                    if (it.Valid) { _ = it.Key.Length; _ = it.Value.Length; it.Next(); }
                    it.SeekForPrev(lk);
                    if (it.Valid) { _ = it.Key.Length; it.Prev(); }
                }
            }
            BlockIter? rd = reader.NewRangeTombstoneIterator();
            if (rd is not null)
            {
                try
                {
                    int steps = 0;
                    for (rd.SeekToFirst(); rd.Valid && steps < 5000; rd.Next()) { _ = rd.Key.Length; _ = rd.Value.Length; steps++; }
                }
                finally
                {
                    rd.Dispose();
                }
            }
            if (reader is BlockBasedTableReader bbt) bbt.VerifyChecksum();
        }
        finally
        {
            reader.Dispose();
        }
    }

    /// <summary>
    /// Mutation fuzz of whole SST files: bit flips, byte overwrites, 0xFF / 0x00 runs, truncation, inserted / removed
    /// bytes over tables of every index type, format_version 2..8, filters (legacy and fast local Bloom, Ribbon,
    /// partitioned), compression with and without a dictionary, and checksums on (and kNoChecksum, so the block
    /// parsers see the damage). Pins BlockBasedTable::Open / ReadFooterFromFile / Footer::DecodeFrom /
    /// BlockHandle::DecodeFrom / Block::Block / DataBlockIter::ParseNextKey / DecodeEntry / DecompressBlockData /
    /// ReadTablePropertiesHelper / GetBuiltinFilterBitsReader: only Corruption (or NotSupported for an unknown
    /// format) may escape, and every operation terminates.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public async Task MutatedTablesFailWithCorruptionOnly()
    {
        int baseSeed = Environment.GetEnvironmentVariable("AUDIT7_TABLE_SEED") is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture) : 7070;
        int tables = Environment.GetEnvironmentVariable("AUDIT7_TABLE_ROUNDS") is { } r ? int.Parse(r, System.Globalization.CultureInfo.InvariantCulture) : 200;
        const int mutationsPerTable = 40;
        var codes = new Dictionary<StatusCode, int>();
        var failures = new List<string>();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int t = 0; t < tables && failures.Count < 20; t++)
        {
            int seed = baseSeed + t;
            var rnd = new Random(seed);
            Built b = BuildRandomTable(rnd, seed);
            // The unmodified table must read cleanly.
            try
            {
                await ExerciseAsync(b.Data, b, verifyChecksums: true, new Random(seed));
            }
            catch (Exception e)
            {
                throw new AssertFailedException("unmodified table: " + b.Trace, e);
            }
            for (int m = 0; m < mutationsPerTable; m++)
            {
                byte[] d = Mutate(rnd, b.Data, out string what);
                bool verify = rnd.Next(2) == 0;
                int opSeed = rnd.Next();
                try
                {
                    await ExerciseAsync(d, b, verify, new Random(opSeed));
                }
                catch (RocksDbException e) when (e.Code is StatusCode.Corruption or StatusCode.NotSupported
                    // BlockBasedTable::Open: a damaged footer feature bit or standard_index_is_stub property.
                    || (e.Code == StatusCode.InvalidArgument && e.Message.Contains("stub standard index", StringComparison.Ordinal))
                    // A block offset of 2^63 or more: pread's EINVAL (PosixRandomAccessFile::Read).
                    || (e.Code == StatusCode.IOError && e.Message.StartsWith("IO error: While pread offset", StringComparison.Ordinal)))
                {
                    codes[e.Code] = codes.GetValueOrDefault(e.Code) + 1;
                }
                catch (Exception e) when (e is not AssertFailedException)
                {
                    failures.Add($"{b.Trace} m={m} {what} verify={verify} opSeed={opSeed}: {e.GetType().Name}: {e.Message}\n{e.StackTrace}");
                }
            }
        }
        Console.WriteLine($"elapsed={sw.Elapsed} codes=" + string.Join(",", codes.Select(kv => kv.Key + ":" + kv.Value)));
        if (failures.Count > 0) Assert.Fail(failures.Count + " failures:\n" + string.Join("\n\n", failures.Take(8)));
    }

    private static (byte[]? Data, TableReaderOptions Options, List<byte[]> Lookups, string Trace) BuildPlainOrCuckoo(Random rnd, int seed)
    {
        bool cuckoo = rnd.Next(3) == 0;
        SliceTransform prefix4 = SliceTransform.CreateFixedPrefix(4);
        int keyLen = 12;
        var keys = new SortedSet<string>(StringComparer.Ordinal);
        int n = rnd.Next(1, cuckoo ? 200 : 300);
        while (keys.Count < n)
        {
            string k = "p" + rnd.Next(rnd.Next(2) == 0 ? 5 : 60).ToString("D2", System.Globalization.CultureInfo.InvariantCulture) + "/" + rnd.Next(1_000_000).ToString(System.Globalization.CultureInfo.InvariantCulture);
            keys.Add(k.PadRight(keyLen, 'z')[..keyLen]);
        }
        var entries = new List<(byte[] Key, byte[] Value)>();
        var lookups = new List<byte[]>();
        ulong seq = 5000;
        int cuckooValueLen = rnd.Next(0, 20);
        foreach (string k in keys)
        {
            byte[] uk = System.Text.Encoding.ASCII.GetBytes(k);
            byte[] v = new byte[cuckoo ? cuckooValueLen : rnd.Next(0, 40)];
            rnd.NextBytes(v);
            entries.Add((TableTestUtil.IKey(uk, seq--, EntryType.Value), v));
            if (rnd.Next(3) == 0) lookups.Add(TableTestUtil.IKey(uk, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
        }
        lookups.Add(TableTestUtil.IKey("p00/zzzzzzzz"u8, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek));
        TableBuilderOptions tbo;
        TableReaderOptions ro;
        string trace;
        if (cuckoo)
        {
            var opts = new CuckooTableOptions { UseModuleHash = rnd.Next(2) == 0, CuckooBlockSize = (uint)rnd.Next(1, 6), IdentityAsFirstHash = rnd.Next(4) == 0 };
            tbo = new TableBuilderOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = opts };
            ro = new TableReaderOptions { TableFactory = TableFactoryType.CuckooTable, AllowMmapReads = true };
            trace = $"seed={seed} cuckoo {opts.ToOptionsString()} n={n}";
        }
        else
        {
            bool prefixMode = rnd.Next(3) != 0;
            var opts = new PlainTableOptions
            {
                EncodingType = prefixMode && rnd.Next(2) == 0 ? PlainTableEncodingType.Prefix : PlainTableEncodingType.Plain,
                UserKeyLength = rnd.Next(3) == 0 ? (uint)keyLen : 0u,
                StoreIndexInFile = rnd.Next(2) == 0,
                BloomBitsPerKey = rnd.Next(3) == 0 ? 0 : 10,
                HashTableRatio = prefixMode ? 0.75 : 0,
                IndexSparseness = (ulong)(new[] { 0, 1, 3, 16 })[rnd.Next(4)],
                FullScanMode = !prefixMode && rnd.Next(5) == 0,
            };
            tbo = new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, PrefixExtractor = prefixMode ? prefix4 : null };
            ro = new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, PrefixExtractor = prefixMode ? prefix4 : null };
            trace = $"seed={seed} plain prefix={prefixMode} {opts.ToOptionsString()} n={n}";
        }
        var file = new MemWritableFile();
        try
        {
            using TableBuilder b = TableBuilder.Create(file, TableFactory.CreateFormatOptions(new BlockBasedTableOptions()), tbo);
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
        }
        catch (RocksDbException e) when (e.Code == StatusCode.NotSupported && e.Message.Contains("collisions", StringComparison.Ordinal))
        {
            return (null, ro, lookups, trace); // too many cuckoo collisions: a legitimate builder outcome
        }
        return (file.ToArray(), ro, lookups, trace);
    }

    /// <summary>
    /// Mutation fuzz of plain and cuckoo tables (no block checksums in either format): PlainTableReader::Open /
    /// PopulateIndex / Next (PlainTableKeyDecoder), CuckooTableReader::CuckooTableReader / Get / iterator, and the
    /// adaptive factory's magic number dispatch. Only Corruption / NotSupported / InvalidArgument (upstream's statuses
    /// for bad geometry, unknown formats and option mismatches) may escape, and every operation terminates.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public void MutatedPlainAndCuckooTablesFailWithStatusOnly()
    {
        int baseSeed = Environment.GetEnvironmentVariable("AUDIT7_TABLE_SEED") is { } s ? int.Parse(s, System.Globalization.CultureInfo.InvariantCulture) : 9090;
        int tables = Environment.GetEnvironmentVariable("AUDIT7_TABLE_ROUNDS") is { } r ? int.Parse(r, System.Globalization.CultureInfo.InvariantCulture) : 100;
        var failures = new List<string>();
        var codes = new Dictionary<StatusCode, int>();
        for (int t = 0; t < tables && failures.Count < 20; t++)
        {
            int seed = baseSeed + t;
            var rnd = new Random(seed);
            (byte[]? built, TableReaderOptions ro, List<byte[]> lookups, string trace) = BuildPlainOrCuckoo(rnd, seed);
            if (built is not { } data) continue;
            bool fullScan = ro.PlainTableOptions?.FullScanMode == true; // Get / Seek are InvalidArgument in full scan mode (upstream)
            for (int m = -1; m < 40; m++)
            {
                string what = "none";
                byte[] d = m < 0 ? data : Mutate(rnd, data, out what);
                bool adaptive = rnd.Next(3) == 0;
                try
                {
                    using TableReader reader = TableReaderFactory.Open(new MemRandomAccessFile(d), adaptive ? ro with { TableFactory = TableFactoryType.AdaptiveTable } : ro);
                    foreach (byte[] lk in fullScan ? [] : lookups)
                    {
                        _ = reader.Get(lk, new ArrayBufferWriter<byte>(), TableReadFlags.None);
                        _ = reader.InternalKeyMayMatch(lk, TableReadFlags.None);
                        _ = reader.ApproximateOffsetOf(lk);
                    }
                    using InternalIterator it = reader.NewInternalIterator(TableReadFlags.None, null, ro.PrefixExtractor);
                    int steps = 0;
                    for (it.SeekToFirst(); it.Valid && steps < 5000; it.Next()) { _ = it.Key.Length; _ = it.Value.Length; steps++; }
                    Assert.IsTrue(steps < 5000, "scan did not terminate");
                    foreach (byte[] lk in fullScan ? [] : lookups)
                    {
                        it.Seek(lk);
                        if (it.Valid) { _ = it.Key.Length; it.Next(); }
                    }
                    if (m < 0) Assert.IsTrue(steps > 0, trace);
                }
                catch (RocksDbException e) when (m >= 0 && e.Code is StatusCode.Corruption or StatusCode.NotSupported or StatusCode.InvalidArgument)
                {
                    codes[e.Code] = codes.GetValueOrDefault(e.Code) + 1;
                }
                catch (Exception e) when (e is not AssertFailedException)
                {
                    failures.Add($"{trace} m={m} {what} adaptive={adaptive}: {e.GetType().Name}: {e.Message}\n{e.StackTrace}");
                }
            }
        }
        Console.WriteLine("codes=" + string.Join(",", codes.Select(kv => kv.Key + ":" + kv.Value)));
        if (failures.Count > 0) Assert.Fail(failures.Count + " failures:\n" + string.Join("\n\n", failures.Take(8)));
    }

    // ---- deterministic pins of the fixes ---------------------------------------------------------------------------

    private static List<(byte[] Key, byte[] Value)> TextEntries(int n)
    {
        var entries = new List<(byte[], byte[])>(n);
        for (int i = 0; i < n; i++)
        {
            entries.Add((TableTestUtil.IKey("key" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture), (ulong)(i + 1)),
                System.Text.Encoding.ASCII.GetBytes("{\"id\":" + i + ",\"name\":\"customer-" + (i % 97) + "\",\"country\":\"Portugal\"}")));
        }
        return entries;
    }

    /// <summary>
    /// BlockBasedTable::VerifyChecksum / VerifyChecksumInBlocks / VerifyChecksumInMetaBlocks
    /// (block_based_table_reader.cc): blocks are read with "false /* decompress */", so a valid dictionary-compressed
    /// table verifies cleanly (the managed reader decompressed them without the dictionary: Corruption). A flipped bit
    /// in a data block is still reported.
    /// </summary>
    [TestMethod]
    public void VerifyChecksumDoesNotDecompressDictionaryBlocks()
    {
        foreach (CompressionType t in new[] { CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })
        {
            byte[] data = TableTestUtil.BuildTable(TextEntries(2000), new TableFormatOptions { BlockSize = 512, Checksum = ChecksumType.Crc32c },
                new TableBuilderOptions { CompressionType = t, CompressionOptions = new CompressionOptions { MaxDictBytes = 2048 } });
            using (BlockBasedTableReader reader = TableTestUtil.Open(data))
            {
                Assert.IsTrue(reader.CompressionDictionary.Length > 0, t + ": the table has a dictionary");
                reader.VerifyChecksum();
            }
            data[100] ^= 1; // inside the first data block
            using (BlockBasedTableReader reader = TableTestUtil.Open(data))
            {
                Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(reader.VerifyChecksum).Code, t.ToString());
            }
        }
    }

    /// <summary>
    /// Decompressor::ExtractUncompressedSize + DecompressBlockData (util/compression.cc, table/format.cc): a stored
    /// uncompressed size no payload can produce (just under 2 GiB, above Array.MaxLength, above int.MaxValue for snappy's
    /// own header) is Corruption, without allocating it; upstream allocates it and the decompressor fails. Before the
    /// fix: OutOfMemoryException, ArgumentOutOfRangeException, or a ~2 GiB allocation kept in the buffer pool.
    /// </summary>
    [TestMethod]
    public void HugeStoredUncompressedSizeIsCorruptionWithoutAllocating()
    {
        foreach (CompressionType t in new[] { CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })
        foreach (ulong size in new ulong[] { 0x40000000, 0x7FFFFFC0, 0x7FFFFFF0, 0xFFFFFFFF, ulong.MaxValue })
        {
            byte[] buf = new byte[30];
            int n = RocksDb.Managed.Util.Coding.EncodeVarint64(buf, size);
            if (t == CompressionType.Snappy && size > uint.MaxValue) continue; // not a varint32: snappy's own header error
            long before = GC.GetAllocatedBytesForCurrentThread();
            var e = Assert.ThrowsExactly<RocksDbException>(() => BlockFetcher.Decompress(t, buf.AsSpan(0, n + 8), 7).Release(), t + " " + size);
            Assert.AreEqual(StatusCode.Corruption, e.Code, t + " " + size);
            Assert.IsTrue(GC.GetAllocatedBytesForCurrentThread() - before < 1 << 20, t + " " + size + ": allocated the claimed size");
            var w = new ArrayBufferWriter<byte>();
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(
                () => RocksDb.Managed.Compression.BlockCompression.Decompress(t, buf.AsSpan(0, n + 8), 7, w, new RocksDb.Managed.Compression.CompressionDictionary("dictionary-bytes"u8.ToArray()))).Code, t + " dict " + size);
        }
    }

    /// <summary>
    /// The expansion bound behind <see cref="HugeStoredUncompressedSizeIsCorruptionWithoutAllocating"/> never rejects a
    /// valid stream: maximally compressible blocks (runs of one byte, up to 4 MiB) round trip for every codec.
    /// </summary>
    [TestMethod]
    public void HighRatioBlocksStillDecompress()
    {
        foreach (CompressionType t in new[] { CompressionType.Snappy, CompressionType.LZ4, CompressionType.Zstd, CompressionType.Zlib })
        foreach (int len in new[] { 1, 64, 4096, 1 << 20, 4 << 20 })
        {
            byte[] raw = new byte[len];
            var compressed = new ArrayBufferWriter<byte>();
            if (!RocksDb.Managed.Compression.BlockCompression.TryCompress(t, raw, 7, RocksDb.Managed.Compression.BlockCompression.DefaultCompressionLevel, 1023, compressed)) continue;
            Block b = BlockFetcher.Decompress(t, compressed.WrittenSpan, 7);
            try
            {
                Assert.AreEqual(len, b.Data.Length, t + " " + len + " (" + compressed.WrittenCount + " compressed bytes)");
            }
            finally
            {
                b.Release();
            }
        }
    }

    /// <summary>Rewrites a format_version 2..5 footer's metaindex handle (keeps the index handle).</summary>
    private static void RewriteFooterMetaindex(byte[] file, ulong offset, ulong size)
    {
        Span<byte> handles = file.AsSpan(file.Length - 53 + 1, 40);
        int n = RocksDb.Managed.Util.Coding.DecodeVarint64(handles, out _);
        n += RocksDb.Managed.Util.Coding.DecodeVarint64(handles[n..], out _);
        int m = RocksDb.Managed.Util.Coding.DecodeVarint64(handles[n..], out ulong indexOffset);
        RocksDb.Managed.Util.Coding.DecodeVarint64(handles[(n + m)..], out ulong indexSize);
        Span<byte> fresh = stackalloc byte[40];
        fresh.Clear();
        int w = RocksDb.Managed.Util.Coding.EncodeVarint64(fresh, offset);
        w += RocksDb.Managed.Util.Coding.EncodeVarint64(fresh[w..], size);
        w += RocksDb.Managed.Util.Coding.EncodeVarint64(fresh[w..], indexOffset);
        RocksDb.Managed.Util.Coding.EncodeVarint64(fresh[w..], indexSize);
        fresh.CopyTo(handles);
    }

    /// <summary>
    /// ReadFooterFromFile + BlockFetcher::ReadBlockContents (table/format.cc, block_fetcher.cc) on a footer whose
    /// metaindex handle is past the end of the file: "truncated block read" Corruption; an offset of 2^63 or more is
    /// the IOError pread's EINVAL gives (PosixRandomAccessFile::Read; native ingestion reports exactly these, see
    /// Audit7TableCorruptionCompatTests.FooterHandlesBeyondTheFileMatchNative); a size that cannot be allocated or
    /// wraps is Corruption (native aborts). Sync and async open. Before the fix: ArgumentOutOfRangeException from the
    /// file read (negative offset), also in the async tail prefetch.
    /// </summary>
    [TestMethod]
    public async Task FooterHandleBeyondTheFileFailsWithNativeStatus()
    {
        byte[] original = TableTestUtil.BuildTable(TextEntries(100), new TableFormatOptions { FormatVersion = 5, BlockSize = 512 });
        (ulong Offset, ulong Size, StatusCode Code)[] cases =
        [
            ((ulong)original.Length + 100, 64, StatusCode.Corruption),
            (long.MaxValue - 10, 64, StatusCode.Corruption),
            ((1UL << 63) + 5, 64, StatusCode.IOError),
            (ulong.MaxValue - 2, 64, StatusCode.IOError),
            (0, 1UL << 40, StatusCode.Corruption),
            (10, ulong.MaxValue - 3, StatusCode.Corruption),
            (0, int.MaxValue - 2, StatusCode.Corruption),
        ];
        foreach ((ulong offset, ulong size, StatusCode code) in cases)
        {
            byte[] d = (byte[])original.Clone();
            RewriteFooterMetaindex(d, offset, size);
            string what = "offset " + offset + " size " + size;
            Assert.AreEqual(code, Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.Open(d), what).Code, what);
            Assert.AreEqual(code, (await Assert.ThrowsExactlyAsync<RocksDbException>(
                async () => (await BlockBasedTableReader.OpenAsync(new MemRandomAccessFile(d), TableReaderOptions.Default)).Dispose(), what)).Code, what + " async");
            Assert.AreEqual(code, Assert.ThrowsExactly<RocksDbException>(
                () => BlockFetcher.ReadBlock(new MemRandomAccessFile(d), TableTestUtil.Open(original).Footer, new BlockHandle(offset, size), true).Release(), what).Code, what + " ReadBlock");
        }
    }

    /// <summary>
    /// Empty and short files (ReadFooterFromFile: "file is too short (N bytes) to be an sstable"), for every reader
    /// entry point.
    /// </summary>
    [TestMethod]
    public async Task EmptyAndShortFilesAreCorruption()
    {
        byte[] valid = TableTestUtil.BuildTable(TextEntries(10), new TableFormatOptions());
        foreach (int len in new[] { 0, 1, 8, Footer.MinEncodedLength - 1, Footer.MinEncodedLength, Footer.EncodedLength - 1 })
        {
            byte[] d = valid.AsSpan(valid.Length - len).ToArray();
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => TableTestUtil.Open(d), "len " + len).Code);
            Assert.AreEqual(StatusCode.Corruption, (await Assert.ThrowsExactlyAsync<RocksDbException>(
                async () => (await BlockBasedTableReader.OpenAsync(new MemRandomAccessFile(d), TableReaderOptions.Default)).Dispose())).Code);
            foreach (TableFactoryType f in new[] { TableFactoryType.AdaptiveTable, TableFactoryType.PlainTable, TableFactoryType.CuckooTable })
            {
                var e = Assert.ThrowsExactly<RocksDbException>(() => TableReaderFactory.Open(new MemRandomAccessFile(d), new TableReaderOptions { TableFactory = f, AllowMmapReads = true }));
                Assert.AreEqual(StatusCode.Corruption, e.Code, f + " len " + len + ": " + e.Message);
            }
        }
    }

    /// <summary>
    /// PlainTableReader::GetOffset / PlainTableIndex::GetSubIndexBasePtrAndUpperBound (table/plain/plain_table_reader.cc,
    /// plain_table_index.h): a stored prefix index whose bucket points outside the sub index, or at a sub index entry
    /// with zero records, is Corruption. Upstream reads it unchecked (undefined behavior, not drivable natively); the
    /// port threw ArgumentOutOfRangeException.
    /// </summary>
    [TestMethod]
    public void PlainTableSubIndexOutOfRangeIsCorruption()
    {
        SliceTransform prefix4 = SliceTransform.CreateFixedPrefix(4);
        var opts = new PlainTableOptions { StoreIndexInFile = true, HashTableRatio = 0.75, IndexSparseness = 1, BloomBitsPerKey = 0 };
        var entries = new List<(byte[], byte[])>();
        for (int p = 0; p < 3; p++)
            for (int i = 0; i < 20; i++)
                entries.Add((TableTestUtil.IKey("pp" + p + "/" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture), 100), "v"u8.ToArray()));
        var file = new MemWritableFile();
        using (TableBuilder b = TableBuilder.Create(file, TableFactory.CreateFormatOptions(new BlockBasedTableOptions()),
                   new TableBuilderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, PrefixExtractor = prefix4 }))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
        }
        byte[] data = file.ToArray();
        var ro = new TableReaderOptions { TableFactory = TableFactoryType.PlainTable, PlainTableOptions = opts, PrefixExtractor = prefix4 };
        byte[] lookup = TableTestUtil.IKey("pp1/0005", DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        using (TableReader ok = TableReaderFactory.Open(new MemRandomAccessFile(data), ro))
        {
            Assert.IsTrue(ok.Get(lookup, null, TableReadFlags.None).Found);
        }
        Footer footer = TableMetaReader.ReadFooter(new MemRandomAccessFile(data), TableMagicNumbers.PlainTable);
        BlockHandle h = TableMetaReader.ReadMetaIndex(data, footer)[PlainTableIndex.PlainTableIndexBlock];
        ReadOnlySpan<byte> idx = data.AsSpan((int)h.Offset, (int)h.Size);
        int n1 = RocksDb.Managed.Util.Coding.DecodeVarint32(idx, out uint indexSize);
        int n2 = RocksDb.Managed.Util.Coding.DecodeVarint32(idx[n1..], out _);
        int buckets = (int)h.Offset + n1 + n2;
        int subIndex = buckets + (int)indexSize * 4;
        Assert.IsTrue(h.Offset + h.Size > (ulong)subIndex, "the index has a sub index");

        foreach ((string name, uint bucketValue, bool zeroCount) in new[] { ("offset past the sub index", 0x80000000u | 0x7FFFFFF0u, false), ("zero records", 0x80000000u, true) })
        {
            byte[] d = (byte[])data.Clone();
            for (int b = 0; b < indexSize; b++) System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(d.AsSpan(buckets + b * 4), bucketValue);
            if (zeroCount) d[subIndex] = 0;
            using TableReader r = TableReaderFactory.Open(new MemRandomAccessFile(d), ro);
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => r.Get(lookup, null, TableReadFlags.None), name).Code, name);
            using InternalIterator it = r.NewInternalIterator(TableReadFlags.None, null, prefix4);
            Assert.AreEqual(StatusCode.Corruption, Assert.ThrowsExactly<RocksDbException>(() => it.Seek(lookup), name).Code, name + " seek");
        }
    }

    /// <summary>
    /// GetBuiltinFilterBitsReader / GetBloomBitsReader / GetRibbonBitsReader (filter_policy.cc): reserved or invalid
    /// metadata in a full filter is read as "always may match" (no false negatives), for every key of a kNoChecksum
    /// table (native agrees: Audit7TableCorruptionCompatTests.ReservedFilterMetadataMatchesNative).
    /// </summary>
    [TestMethod]
    public void ReservedFilterMetadataNeverHidesKeys()
    {
        (string Name, int FromEnd, byte[] Bytes)[] cases =
        [
            ("num_probes 0", 5, [0]), ("marker -3", 5, [0xFD]), ("sub-impl 1", 4, [1]), ("num_probes 31", 3, [31]),
            ("block bytes 128", 3, [0x20 | 6]), ("reserved bytes", 2, [0, 1]), ("ribbon 1 block", 5, [0xFE, 0, 1, 0, 0]),
            ("ribbon 0 blocks", 5, [0xFE, 0, 0, 0, 0]), ("legacy, no line solution", 5, [6, 5, 0, 0, 0]), ("legacy, 0 lines", 5, [6, 0, 0, 0, 0]),
        ];
        var policy = new BloomFilterPolicy(10);
        List<(byte[] Key, byte[] Value)> entries = TextEntries(300);
        byte[] data = TableTestUtil.BuildTable(entries, new TableFormatOptions { FormatVersion = 5, Checksum = ChecksumType.NoChecksum, FilterPolicy = policy });
        BlockHandle filter;
        using (BlockBasedTableReader r = TableTestUtil.Open(data, new TableReaderOptions { FilterPolicy = policy }))
        {
            filter = r.MetaBlocks[r.MetaBlocks.Keys.Single(k => k.StartsWith(BlockBasedTableConstants.FullFilterBlockPrefix, StringComparison.Ordinal))];
        }
        foreach ((string name, int fromEnd, byte[] patch) in cases)
        {
            byte[] d = (byte[])data.Clone();
            patch.CopyTo(d.AsSpan((int)(filter.Offset + filter.Size) - fromEnd));
            using BlockBasedTableReader r = TableTestUtil.Open(d, new TableReaderOptions { FilterPolicy = policy });
            foreach ((byte[] k, byte[] _) in entries)
            {
                byte[] lk = TableTestUtil.IKey(DbFormat.ExtractUserKey(k), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
                Assert.IsTrue(r.Get(lk, null, TableReadFlags.None).Found, name + ": " + TableTestUtil.UserKeyOf(k));
            }
        }
    }

    /// <summary>
    /// CuckooTableIterator::InitIfNeeded (table/cuckoo/cuckoo_table_reader.cc: "sorted_bucket_ids_.reserve(
    /// reader_->GetTableProperties()->num_entries)"): a damaged rocksdb.num.entries property must not size an
    /// allocation. Upstream's reserve of ~2^31 uint32s throws out of the iterator (not drivable natively: cuckoo
    /// tables need mmap reads and the C API has no table reader); the port threw OutOfMemoryException from the
    /// List capacity. The capacity is now capped by the bucket count validated at open; the scan returns the entries.
    /// </summary>
    [TestMethod]
    public void CuckooHugeNumEntriesPropertyDoesNotSizeTheIteratorAllocation()
    {
        var entries = new List<(byte[], byte[])>();
        for (int i = 0; i < 50; i++) entries.Add((TableTestUtil.IKey("ck" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture), 7), "valu"u8.ToArray()));
        var file = new MemWritableFile();
        using (TableBuilder b = TableBuilder.Create(file, TableFactory.CreateFormatOptions(new BlockBasedTableOptions()),
                   new TableBuilderOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = new CuckooTableOptions() }))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
        }
        byte[] data = file.ToArray();
        Footer footer = TableMetaReader.ReadFooter(new MemRandomAccessFile(data), TableMagicNumbers.CuckooTable);
        Dictionary<string, BlockHandle> meta = TableMetaReader.ReadMetaIndex(data, footer);
        BlockHandle propsHandle = meta[BlockBasedTableConstants.PropertiesBlockName];
        TableProperties props = TableMetaReader.ReadProperties(data, meta);

        // Rewrite the tail (properties, metaindex, footer) with num_entries = 2^31 - 1, like CuckooTableBuilder::Finish.
        props.NumEntries = int.MaxValue;
        var output = new ArrayBufferWriter<byte>();
        output.Write(data.AsSpan(0, (int)propsHandle.Offset));
        using var metaIndex = new MetaIndexBuilder();
        using (var pb = new PropertyBlockBuilder())
        {
            props.AddTo(pb);
            foreach (KeyValuePair<string, byte[]> kv in props.UserCollectedProperties) pb.Add(kv.Key, kv.Value);
            ReadOnlySpan<byte> block = pb.Finish();
            metaIndex.Add(BlockBasedTableConstants.PropertiesBlockName, new BlockHandle((ulong)output.WrittenCount, (ulong)block.Length));
            output.Write(block);
        }
        var metaHandle = new BlockHandle((ulong)output.WrittenCount, 0);
        ReadOnlySpan<byte> mi = metaIndex.Finish();
        metaHandle = metaHandle with { Size = (ulong)mi.Length };
        output.Write(mi);
        Span<byte> f = stackalloc byte[Footer.EncodedLength];
        output.Write(f[..Footer.BuildForMagic(f, TableMagicNumbers.CuckooTable, 1, ChecksumType.NoChecksum, metaHandle, BlockHandle.Null)]);
        byte[] damaged = output.WrittenSpan.ToArray();

        using TableReader reader = TableReaderFactory.Open(new MemRandomAccessFile(damaged), new TableReaderOptions { TableFactory = TableFactoryType.CuckooTable, AllowMmapReads = true });
        Assert.AreEqual((ulong)int.MaxValue, reader.Properties.NumEntries);
        long before = GC.GetAllocatedBytesForCurrentThread();
        using InternalIterator it = reader.NewInternalIterator(TableReadFlags.None, null, null);
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) n++;
        Assert.AreEqual(entries.Count, n);
        Assert.IsTrue(GC.GetAllocatedBytesForCurrentThread() - before < 16 << 20, "allocation sized by num_entries");
    }
}
