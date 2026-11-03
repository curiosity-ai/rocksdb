using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>
/// Parallel data block compression (CompressionOptions.parallel_threads, block_based_table_builder.cc
/// ParallelCompressionRep): the SST written with parallel_threads &gt; 1 must be byte-identical to the serial one for every
/// index / filter / compression / format configuration, and readable. Upstream's equivalent coverage is
/// db_basic_test (DBBasicTestWithParallelIO / compression_parallel_threads params) and table_test's parallel variants.
/// </summary>
[TestClass]
public sealed class ParallelCompressionTests
{
    private const uint FixedContextChecksum = 0x5eed1234;

    private sealed record Case(string Name, CompressionType Type, TableFormatOptions Format, CompressionOptions? Compression = null,
        SliceTransform? Prefix = null, int Keys = 3000, bool WithRangeDeletions = false);

    private static IEnumerable<object[]> Cases()
    {
        TableFormatOptions Fmt(int fv, IndexType it = IndexType.BinarySearch, int blockSize = 512) => new()
        {
            FormatVersion = fv,
            BlockSize = blockSize,
            IndexType = it,
            FilterPolicy = new BloomFilterPolicy(10),
        };
        yield return [new Case("lz4-fv7", CompressionType.LZ4, Fmt(7))];
        yield return [new Case("zstd-fv7", CompressionType.Zstd, Fmt(7), WithRangeDeletions: true)];
        yield return [new Case("snappy-fv5", CompressionType.Snappy, Fmt(5))];
        yield return [new Case("zlib-fv6", CompressionType.Zlib, Fmt(6, blockSize: 2048), Keys: 1500)];
        yield return [new Case("lz4hc-fv2", CompressionType.LZ4HC, Fmt(2))];
        yield return [new Case("zstd-fv8-common-prefix", CompressionType.Zstd, Fmt(8))];
        yield return [new Case("lz4-first-key", CompressionType.LZ4, Fmt(7, IndexType.BinarySearchWithFirstKey))];
        yield return [new Case("zstd-partitioned", CompressionType.Zstd, Fmt(7, IndexType.TwoLevelIndexSearch) with
        {
            MetadataBlockSize = 256,
            PartitionFilters = true,
            DecouplePartitionedFilters = true,
        })];
        yield return [new Case("lz4-partitioned-no-filter-partitions", CompressionType.LZ4, Fmt(4, IndexType.TwoLevelIndexSearch) with { MetadataBlockSize = 300 })];
        yield return [new Case("lz4-hash", CompressionType.LZ4, Fmt(7, IndexType.HashSearch), Prefix: SliceTransform.CreateFixedPrefix(4))];
        yield return [new Case("zstd-dict", CompressionType.Zstd, Fmt(7),
            new CompressionOptions { MaxDictBytes = 4096, ZstdMaxTrainBytes = 32768, MaxDictBufferBytes = 64 << 10 })];
        yield return [new Case("lz4-dict-raw", CompressionType.LZ4, Fmt(7), new CompressionOptions { MaxDictBytes = 2048 })];
        yield return [new Case("zstd-super-block-align", CompressionType.Zstd, Fmt(7) with { SuperBlockAlignmentSize = 4096, SuperBlockAlignmentSpaceOverheadRatio = 4 })];
        yield return [new Case("lz4-crc32c-index-uncompressed", CompressionType.LZ4, Fmt(7) with { Checksum = ChecksumType.Crc32c, EnableIndexCompression = false })];
        yield return [new Case("zstd-xxhash", CompressionType.Zstd, Fmt(6) with { Checksum = ChecksumType.XxHash, IndexShortening = IndexShorteningMode.ShortenSeparatorsAndSuccessor })];
        yield return [new Case("lz4-tiny", CompressionType.LZ4, Fmt(7), Keys: 3)];
    }

    public static string CaseName(System.Reflection.MethodInfo _, object[] data) => ((Case)data[0]).Name;

    [TestMethod]
    [DynamicData(nameof(Cases), DynamicDataDisplayName = nameof(CaseName))]
    public void ParallelOutputIsByteIdenticalToSerial(object c)
    {
        var tc = (Case)c;
        int seed = Environment.TickCount;
        // Deterministic content per case (the seed only varies the key set across runs; both builds use it).
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(seed, tc.Keys);
        byte[] serial = BuildTable(tc, entries, threads: 1, out bool serialParallel);
        Assert.IsFalse(serialParallel);
        foreach (uint threads in (uint[])[2, 4, 9])
        {
            byte[] parallel = BuildTable(tc, entries, threads, out bool active);
            Assert.IsTrue(active, "parallel compression should be active");
            Assert.AreEqual(serial.Length, parallel.Length, $"seed={seed} threads={threads}");
            Assert.IsTrue(serial.AsSpan().SequenceEqual(parallel), $"seed={seed} threads={threads}: SST bytes differ at {FirstDifference(serial, parallel)}");
        }
        // And the output reads back.
        using BlockBasedTableReader r = BlockBasedTableReader.Open(new MemRandomAccessFile(serial),
            new TableReaderOptions { FilterPolicy = tc.Format.FilterPolicy, PrefixExtractor = tc.Prefix });
        using BlockBasedTableIterator it = r.NewIterator();
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next(), n++)
        {
            CollectionAssert.AreEqual(entries[n].Key, DbFormat.ExtractUserKey(it.Key).ToArray());
            CollectionAssert.AreEqual(entries[n].Value, it.Value.ToArray());
        }
        Assert.AreEqual(entries.Count, n);
    }

    [TestMethod]
    public void NotActiveWithoutCompressionOrWithCoupledPartitionedFilters()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(1, 100);
        var none = new Case("none", CompressionType.None, new TableFormatOptions());
        BuildTable(none, entries, 4, out bool active);
        Assert.IsFalse(active);
        var coupled = new Case("coupled", CompressionType.LZ4, new TableFormatOptions
        {
            IndexType = IndexType.TwoLevelIndexSearch,
            PartitionFilters = true,
            DecouplePartitionedFilters = false,
            FilterPolicy = new BloomFilterPolicy(10),
        });
        BuildTable(coupled, entries, 4, out active);
        Assert.IsFalse(active);
    }

    /// <summary>Abandon / Dispose with blocks still in the ring (no Finish) leave no worker running and reuse the ring safely.</summary>
    [TestMethod]
    public void AbandonWithBlocksInFlight()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(2, 2000);
        var tc = new Case("abandon", CompressionType.Zstd, new TableFormatOptions { BlockSize = 256 });
        for (int round = 0; round < 20; round++)
        {
            var w = new MemWritableFile();
            using var b = new BlockBasedTableBuilder(w, tc.Format, Tbo(tc, 4));
            for (int i = 0; i < entries.Count / (round + 1); i++) b.Add(Ikey(entries[i].Key, (ulong)i + 1), entries[i].Value);
            if (round % 2 == 0) b.Abandon();
        }
        // The pooled rings still produce identical output afterwards.
        byte[] serial = BuildTable(tc, entries, 1, out _);
        byte[] parallel = BuildTable(tc, entries, 4, out _);
        Assert.IsTrue(serial.AsSpan().SequenceEqual(parallel));
    }

    /// <summary>EstimatedFileSize covers the blocks in flight (used by compaction to cut output files).</summary>
    [TestMethod]
    public void EstimatedFileSizeIncludesInflightBlocks()
    {
        List<(byte[] Key, byte[] Value)> entries = MakeEntries(3, 2000);
        var tc = new Case("estimate", CompressionType.Zstd, new TableFormatOptions { BlockSize = 256 });
        var w = new MemWritableFile();
        using var b = new BlockBasedTableBuilder(w, tc.Format, Tbo(tc, 4));
        for (int i = 0; i < entries.Count; i++)
        {
            b.Add(Ikey(entries[i].Key, (ulong)i + 1), entries[i].Value);
            Assert.IsTrue(b.EstimatedFileSize >= b.FileSize);
        }
        b.Finish();
        Assert.AreEqual(b.FileSize, b.EstimatedFileSize);
        Assert.AreEqual((ulong)w.Size, b.FileSize);
    }

    private static TableBuilderOptions Tbo(Case c, uint threads)
    {
        CompressionOptions co = c.Compression ?? new CompressionOptions();
        return new TableBuilderOptions
        {
            CompressionType = c.Type,
            CompressionOptions = new CompressionOptions
            {
                MaxDictBytes = co.MaxDictBytes,
                ZstdMaxTrainBytes = co.ZstdMaxTrainBytes,
                MaxDictBufferBytes = co.MaxDictBufferBytes,
                ParallelThreads = threads,
            },
            PrefixExtractor = c.Prefix,
            TestBaseContextChecksum = FixedContextChecksum,
        };
    }

    private static byte[] BuildTable(Case c, List<(byte[] Key, byte[] Value)> entries, uint threads, out bool parallelActive)
    {
        var w = new MemWritableFile();
        using var b = new BlockBasedTableBuilder(w, c.Format, Tbo(c, threads));
        parallelActive = b.IsParallelCompressionActive;
        for (int i = 0; i < entries.Count; i++)
        {
            b.Add(Ikey(entries[i].Key, (ulong)i + 1), entries[i].Value);
            if (c.WithRangeDeletions && i % 500 == 7)
                b.Add(Ikey(entries[i].Key, (ulong)i + 100_000, EntryType.RangeDeletion), entries[Math.Min(i + 3, entries.Count - 1)].Key);
        }
        b.Finish();
        return w.ToArray();
    }

    /// <summary>
    /// Sorted unique keys sharing prefixes (some adjacent blocks share a boundary user key through equal prefixes) with a
    /// mix of compressible and incompressible values, so some blocks are rejected for compression.
    /// </summary>
    private static List<(byte[] Key, byte[] Value)> MakeEntries(int seed, int count)
    {
        var rnd = new Random(seed);
        var keys = new SortedSet<string>(StringComparer.Ordinal);
        while (keys.Count < count)
            keys.Add("k" + rnd.Next(500).ToString("D3", System.Globalization.CultureInfo.InvariantCulture) + "-" + rnd.Next(10_000_000));
        var list = new List<(byte[], byte[])>(count);
        int i = 0;
        foreach (string k in keys)
        {
            byte[] v;
            if ((i / 40) % 5 == 4)
            {
                v = new byte[60];
                rnd.NextBytes(v); // incompressible run: whole blocks get rejected
            }
            else
            {
                v = Encoding.ASCII.GetBytes("value-" + i + new string('z', i % 50));
            }
            list.Add((Encoding.ASCII.GetBytes(k), v));
            i++;
        }
        return list;
    }

    private static byte[] Ikey(byte[] userKey, ulong seq, EntryType type = EntryType.Value)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, seq, type);
        return k;
    }

    private static int FirstDifference(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++) if (a[i] != b[i]) return i;
        return n;
    }
}
