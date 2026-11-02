using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>The engine a db_bench run drives (--engine).</summary>
public enum EngineKind
{
    /// <summary>Native RocksDB through rocksdb-sharp ("rocksdb-sharp", alias "native").</summary>
    RocksDbSharp,
    /// <summary>The managed port ("managed").</summary>
    Managed,
    /// <summary>bench/native-c/rocksdb_bench, the pure C leg ("c-native").</summary>
    CNative,
}

/// <summary>The db_bench workloads (--benchmarks); names as in tools/db_bench_tool.cc.</summary>
public enum DbBenchmark
{
    FillSeq,
    FillRandom,
    Overwrite,
    FillSync,
    ReadRandom,
    ReadMissing,
    ReadSeq,
    ReadReverse,
    SeekRandom,
    MultiReadRandom,
    ReadWhileWriting,
    DeleteRandom,
    Flush,
    Compact,
    WaitForCompaction,
    Stats,
    /// <summary>Optimistic transactions of --txn_puts random puts each (separate OptimisticTransactionDB).</summary>
    TxnOptimistic,
    /// <summary>Pessimistic (WriteCommitted TransactionDB) transactions of --txn_puts random puts each.</summary>
    TxnPessimistic,
    /// <summary>The txnpessimistic workload on a WritePrepared TransactionDB.</summary>
    TxnWritePrepared,
    /// <summary>
    /// The txnpessimistic workload on a WriteUnprepared TransactionDB: named transactions whose batch is written to the DB
    /// as an unprepared batch once it holds half of the transaction's puts (write_batch_flush_threshold).
    /// </summary>
    TxnWriteUnprepared,
}

/// <summary>Block cache implementation (--cache_type).</summary>
public enum BlockCacheKind
{
    /// <summary>"lru" (RocksDB's default).</summary>
    Lru,
    /// <summary>"hyperclock".</summary>
    HyperClock,
}

/// <summary>Command-line spellings of the benchmark enums; strings are parsed here, at the CLI boundary, only.</summary>
public static class BenchFlags
{
    private static readonly (string Name, DbBenchmark Value)[] s_benchmarks =
    [
        ("fillseq", DbBenchmark.FillSeq), ("fillrandom", DbBenchmark.FillRandom), ("overwrite", DbBenchmark.Overwrite),
        ("fillsync", DbBenchmark.FillSync), ("readrandom", DbBenchmark.ReadRandom), ("readmissing", DbBenchmark.ReadMissing),
        ("readseq", DbBenchmark.ReadSeq), ("readreverse", DbBenchmark.ReadReverse), ("seekrandom", DbBenchmark.SeekRandom),
        ("multireadrandom", DbBenchmark.MultiReadRandom), ("readwhilewriting", DbBenchmark.ReadWhileWriting),
        ("deleterandom", DbBenchmark.DeleteRandom), ("flush", DbBenchmark.Flush), ("compact", DbBenchmark.Compact),
        ("waitforcompaction", DbBenchmark.WaitForCompaction), ("stats", DbBenchmark.Stats),
        ("txnoptimistic", DbBenchmark.TxnOptimistic), ("txnpessimistic", DbBenchmark.TxnPessimistic),
        ("txnwriteprepared", DbBenchmark.TxnWritePrepared), ("txnwriteunprepared", DbBenchmark.TxnWriteUnprepared),
    ];

    /// <summary>All benchmark names, in the order of <see cref="DbBenchmark"/>.</summary>
    public static IEnumerable<string> AllBenchmarkNames
    {
        get
        {
            foreach ((string n, DbBenchmark _) in s_benchmarks) yield return n;
        }
    }

    public static string Name(DbBenchmark b)
    {
        foreach ((string n, DbBenchmark v) in s_benchmarks) if (v == b) return n;
        throw new ArgumentOutOfRangeException(nameof(b), b, null);
    }

    public static DbBenchmark ParseBenchmark(string name)
    {
        foreach ((string n, DbBenchmark v) in s_benchmarks) if (n == name) return v;
        throw new ArgumentException("unknown benchmark " + name);
    }

    public static EngineKind ParseEngine(string name) => name switch
    {
        "rocksdb-sharp" or "native" => EngineKind.RocksDbSharp,
        "managed" => EngineKind.Managed,
        "c-native" => EngineKind.CNative,
        _ => throw new ArgumentException("unknown engine '" + name + "' (expected rocksdb-sharp|native|managed|c-native)"),
    };

    /// <summary>The engine's --engine spelling (also its label in RESULT/STATS lines).</summary>
    public static string Name(EngineKind e) => e switch
    {
        EngineKind.RocksDbSharp => "rocksdb-sharp",
        EngineKind.Managed => "managed",
        EngineKind.CNative => "c-native",
        _ => throw new ArgumentOutOfRangeException(nameof(e), e, null),
    };

    public static BlockCacheKind ParseCacheType(string name) => name switch
    {
        "lru" => BlockCacheKind.Lru,
        "hyperclock" => BlockCacheKind.HyperClock,
        _ => throw new ArgumentException("unknown --cache_type " + name + " (lru|hyperclock)"),
    };

    /// <summary>--compression: none (alias no), snappy, lz4, zstd.</summary>
    public static CompressionType ParseCompression(string name) => name switch
    {
        "none" or "no" => CompressionType.None,
        "snappy" => CompressionType.Snappy,
        "lz4" => CompressionType.LZ4,
        "zstd" => CompressionType.Zstd,
        _ => throw new ArgumentException("unsupported --compression " + name + " (none|snappy|lz4|zstd)"),
    };

    /// <summary>The --compression spelling (used in CSV output).</summary>
    public static string Name(CompressionType c) => c switch
    {
        CompressionType.None => "none",
        CompressionType.Snappy => "snappy",
        CompressionType.LZ4 => "lz4",
        CompressionType.Zstd => "zstd",
        _ => throw new ArgumentOutOfRangeException(nameof(c), c, null),
    };
}
