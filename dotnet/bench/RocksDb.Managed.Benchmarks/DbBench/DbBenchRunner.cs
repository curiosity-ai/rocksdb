using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// A small db_bench (tools/db_bench_tool.cc) work-alike for end-to-end comparisons between engines.
/// <code>
/// dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench \
///     --engine native --benchmarks fillseq,fillrandom,readrandom --num 1000000 --value_size 100
/// </code>
/// Supported benchmarks: fillseq, fillrandom, overwrite, readrandom, readmissing, deleterandom, flush.
/// Keys follow db_bench's GenerateKeyFromInt (8-byte big-endian number zero-padded to --key_size); values come from
/// a pre-generated random buffer (compression is off, so compressibility doesn't matter).
/// Like db_bench, fill benchmarks start from a fresh DB (--use_existing_db=false), others reuse it.
/// </summary>
public static class DbBenchRunner
{
    private sealed class Flags
    {
        public string Engine = "native";
        public string[] Benchmarks = ["fillseq", "fillrandom", "readrandom"];
        public long Num = 1_000_000;
        public long Reads = -1;
        public int KeySize = 16;
        public int ValueSize = 100;
        public string Db = Path.Combine(Path.GetTempPath(), "rocksdb-managed-dbbench");
        public bool UseExistingDb;
        public int Seed = 301;
        public EngineOptions Options = new();
    }

    public static int Run(string[] args)
    {
        Flags f;
        try
        {
            f = Parse(args);
        }
        catch (ArgumentException e)
        {
            Console.Error.WriteLine(e.Message);
            PrintUsage();
            return 1;
        }

        Console.WriteLine("Engine:     " + f.Engine);
        Console.WriteLine("Keys:       " + f.KeySize + " bytes each");
        Console.WriteLine("Values:     " + f.ValueSize + " bytes each");
        Console.WriteLine("Entries:    " + f.Num);
        Console.WriteLine("Options:    " + f.Options);
        Console.WriteLine("DB path:    " + f.Db);
        Console.WriteLine("------------------------------------------------");

        IKeyValueEngine? engine = null;
        try
        {
            foreach (string name in f.Benchmarks)
            {
                bool fresh = !f.UseExistingDb && name is "fillseq" or "fillrandom";
                if (engine is null || fresh)
                {
                    engine?.Dispose();
                    engine = EngineFactory.Open(f.Engine, f.Db, f.Options, destroyExisting: fresh);
                }
                RunOne(engine, name, f);
            }
        }
        catch (NotSupportedException e)
        {
            Console.Error.WriteLine(e.Message);
            return 2;
        }
        finally
        {
            engine?.Dispose();
        }
        return 0;
    }

    private static void RunOne(IKeyValueEngine engine, string name, Flags f)
    {
        var rnd = new Random(f.Seed);
        byte[] key = new byte[f.KeySize];
        byte[] valueSource = new byte[Math.Max(1 << 20, f.ValueSize * 2)];
        rnd.NextBytes(valueSource);
        byte[] readBuffer = new byte[Math.Max(f.ValueSize, 1) * 2];
        long ops = name is "readrandom" or "readmissing" && f.Reads >= 0 ? f.Reads : f.Num;
        long bytes = 0, found = 0;
        int valuePos = 0;

        long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        int gen0Before = GC.CollectionCount(0);
        var sw = Stopwatch.StartNew();
        switch (name)
        {
            case "fillseq":
            case "fillrandom":
            case "overwrite":
                for (long i = 0; i < ops; i++)
                {
                    long k = name == "fillseq" ? i : NextLong(rnd, f.Num);
                    MakeKey(key, k);
                    if (valuePos + f.ValueSize > valueSource.Length) valuePos = 0;
                    engine.Put(key, valueSource.AsSpan(valuePos, f.ValueSize));
                    valuePos += f.ValueSize;
                    bytes += f.KeySize + f.ValueSize;
                }
                break;
            case "readrandom":
            case "readmissing":
                for (long i = 0; i < ops; i++)
                {
                    MakeKey(key, NextLong(rnd, f.Num));
                    if (name == "readmissing") key[^1] = (byte)'.'; // db_bench appends '.' to make keys absent
                    if (engine.TryGet(key, readBuffer, out int len))
                    {
                        found++;
                        bytes += f.KeySize + len;
                    }
                }
                break;
            case "deleterandom":
                for (long i = 0; i < ops; i++)
                {
                    MakeKey(key, NextLong(rnd, f.Num));
                    engine.Delete(key);
                }
                break;
            case "flush":
                engine.Flush();
                ops = 1;
                break;
            default:
                throw new ArgumentException("unknown benchmark " + name);
        }
        sw.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
        int gen0 = GC.CollectionCount(0) - gen0Before;

        double seconds = sw.Elapsed.TotalSeconds;
        double microsPerOp = sw.Elapsed.TotalMicroseconds / Math.Max(ops, 1);
        double opsPerSec = ops / Math.Max(seconds, 1e-9);
        string line = string.Format(CultureInfo.InvariantCulture, "{0,-12} : {1,11:F3} micros/op {2,9:F0} ops/sec {3:F3} seconds {4} operations;",
            name, microsPerOp, opsPerSec, seconds, ops);
        if (bytes > 0) line += string.Format(CultureInfo.InvariantCulture, " {0,6:F1} MB/s", bytes / 1048576.0 / Math.Max(seconds, 1e-9));
        if (name is "readrandom" or "readmissing") line += " (" + found + " of " + ops + " found)";
        // Managed allocations (all threads) per operation; native engines only allocate in unmanaged memory.
        line += string.Format(CultureInfo.InvariantCulture, " alloc {0:F1} B/op gen0 {1}", allocated / (double)Math.Max(ops, 1), gen0);
        Console.WriteLine(line);
    }

    /// <summary>db_bench GenerateKeyFromInt without prefixes: big-endian 8 bytes, zero padded.</summary>
    private static void MakeKey(byte[] key, long k)
    {
        key.AsSpan().Clear();
        if (key.Length >= 8) BinaryPrimitives.WriteInt64BigEndian(key, k);
        else
        {
            Span<byte> tmp = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(tmp, k);
            tmp[(8 - key.Length)..].CopyTo(key);
        }
    }

    private static long NextLong(Random rnd, long max) => rnd.NextInt64(max);

    private static Flags Parse(string[] args)
    {
        var f = new Flags();
        EngineOptions o = f.Options;
        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            if (a == "dbbench") continue;
            if (!a.StartsWith("--", StringComparison.Ordinal)) throw new ArgumentException("unexpected argument " + a);
            string name, value;
            int eq = a.IndexOf('=');
            if (eq > 0)
            {
                name = a[2..eq];
                value = a[(eq + 1)..];
            }
            else
            {
                name = a[2..];
                value = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal) ? args[++i] : "true";
            }
            switch (name)
            {
                case "engine": f.Engine = value; break;
                case "benchmarks": f.Benchmarks = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries); break;
                case "num": f.Num = long.Parse(value, CultureInfo.InvariantCulture); break;
                case "reads": f.Reads = long.Parse(value, CultureInfo.InvariantCulture); break;
                case "key_size": f.KeySize = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "value_size": f.ValueSize = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "db": f.Db = value; break;
                case "use_existing_db": f.UseExistingDb = ParseBool(value); break;
                case "seed": f.Seed = int.Parse(value, CultureInfo.InvariantCulture); break;
                case "disable_wal": o = o with { DisableWal = ParseBool(value) }; break;
                case "sync": o = o with { Sync = ParseBool(value) }; break;
                case "cache_size": o = o with { BlockCacheSize = long.Parse(value, CultureInfo.InvariantCulture) }; break;
                case "bloom_bits": o = o with { BloomBitsPerKey = int.Parse(value, CultureInfo.InvariantCulture) }; break;
                case "write_buffer_size": o = o with { WriteBufferSize = long.Parse(value, CultureInfo.InvariantCulture) }; break;
                case "block_size": o = o with { BlockSize = int.Parse(value, CultureInfo.InvariantCulture) }; break;
                case "compression_type": o = o with { Compression = value }; break;
                default: throw new ArgumentException("unknown flag --" + name);
            }
        }
        f.Options = o;
        if (f.KeySize < 8) throw new ArgumentException("--key_size must be >= 8");
        return f;
    }

    private static bool ParseBool(string v) => v is "1" or "true" or "True";

    private static void PrintUsage()
    {
        Console.Error.WriteLine("usage: dbbench [--engine native|managed] [--benchmarks fillseq,fillrandom,overwrite,readrandom,readmissing,deleterandom,flush]");
        Console.Error.WriteLine("               [--num N] [--reads N] [--key_size 16] [--value_size 100] [--db PATH] [--use_existing_db]");
        Console.Error.WriteLine("               [--disable_wal] [--sync] [--cache_size BYTES] [--bloom_bits 10] [--write_buffer_size BYTES] [--block_size 4096]");
    }
}
