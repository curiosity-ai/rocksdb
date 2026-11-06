using System.Runtime.CompilerServices;
using System.Text;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.AllocationProfile;

/// <summary>How often a code path runs (dotnet/CLAUDE.md, ../CLAUDE.md "Hot Path Analysis").</summary>
internal enum PathKind
{
    /// <summary>Per user operation (reads, writes, iteration): must be allocation-free in steady state.</summary>
    Hot,
    /// <summary>Moderate frequency (transactions, snapshots, properties, handle lookups, per-scan setup).</summary>
    Warm,
    /// <summary>Rare (open, flush, compaction, checkpoint, backup, ingestion): reported, clarity first.</summary>
    Cold,
}

/// <summary>
/// One allocation scenario: <see cref="Setup"/> builds the state (DB, keys, ...) and returns a <see cref="Runner"/>
/// that performs exactly <c>ops</c> operations per call. The harness calls it in chunks of <see cref="ChunkOps"/>.
/// </summary>
internal sealed class Scenario
{
    public required string Name { get; init; }
    public required string Group { get; init; }
    public required PathKind Path { get; init; }
    /// <summary>What one "op" is (the unit of B/op).</summary>
    public required string Op { get; init; }
    public string? Notes { get; init; }
    /// <summary>Operations per <see cref="Runner.Run"/> call; the stopwatch is checked between chunks.</summary>
    public int ChunkOps { get; init; } = 1000;
    /// <summary>Native (rocksdb-sharp) comparison scenario.</summary>
    public bool Native { get; init; }
    public required Func<ScenarioContext, ValueTask<Runner>> Setup { get; init; }
}

/// <summary>Executes <c>ops</c> operations. Sync runners return a completed <see cref="ValueTask"/> (no allocation).</summary>
internal abstract class Runner
{
    public abstract ValueTask Run(int ops);

    public static Runner Sync(Action<int> body) => new SyncRunner(body);

    /// <summary>
    /// Async loop. Pass a lambda marked <c>[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]</c> so the
    /// harness' own state machine box is pooled when an op really suspends.
    /// </summary>
    public static Runner Async(Func<int, ValueTask> body) => new AsyncRunner(body);

    /// <summary>
    /// <paramref name="workers"/> concurrent loops on the thread pool, each running <c>ops / workers</c> operations
    /// (<c>body(worker, count)</c>). Starting the workers allocates a few hundred bytes per chunk; use a large chunk.
    /// </summary>
    public static Runner Concurrent(int workers, Func<int, int, ValueTask> body) => new ConcurrentRunner(workers, body);

    private sealed class SyncRunner(Action<int> body) : Runner
    {
        public override ValueTask Run(int ops)
        {
            body(ops);
            return default;
        }
    }

    private sealed class AsyncRunner(Func<int, ValueTask> body) : Runner
    {
        public override ValueTask Run(int ops) => body(ops);
    }

    private sealed class ConcurrentRunner(int workers, Func<int, int, ValueTask> body) : Runner
    {
        private readonly Task[] _tasks = new Task[workers];

        public override async ValueTask Run(int ops)
        {
            int each = Math.Max(1, ops / workers);
            for (int w = 0; w < workers; w++)
            {
                int worker = w;
                _tasks[w] = Task.Run(async () => await body(worker, each).ConfigureAwait(false));
            }
            await Task.WhenAll(_tasks).ConfigureAwait(false);
        }
    }
}

/// <summary>Per-scenario resources: a work directory, key/value generators and DB helpers. Disposed by the harness.</summary>
internal sealed class ScenarioContext : IAsyncDisposable
{
    public const int KeySize = 16;
    public const int ValueSize = 100;

    private readonly List<object> _owned = [];
    private int _dirs;

    public ScenarioContext(string workDirectory)
    {
        WorkDirectory = workDirectory;
        Directory.CreateDirectory(workDirectory);
    }

    public string WorkDirectory { get; }

    /// <summary>A new empty directory under the work directory.</summary>
    public string NewDirectory(string name)
    {
        string dir = System.IO.Path.Combine(WorkDirectory, name + "-" + _dirs++);
        if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>Registers <paramref name="resource"/> (IDisposable / IAsyncDisposable) for disposal in reverse order.</summary>
    public T Own<T>(T resource) where T : class
    {
        _owned.Add(resource);
        return resource;
    }

    /// <summary>16-byte keys <c>"k%015d"</c> for indexes [offset, offset + count), optionally with a suffix that sorts between keys (misses).</summary>
    public static byte[][] Keys(int count, int offset = 0, bool missing = false)
    {
        var keys = new byte[count][];
        for (int i = 0; i < count; i++) keys[i] = Key(offset + i, missing);
        return keys;
    }

    public static byte[] Key(long index, bool missing = false)
    {
        byte[] k = Encoding.ASCII.GetBytes("k" + index.ToString("D15", System.Globalization.CultureInfo.InvariantCulture));
        if (missing) k[^1] = (byte)'.';   // '.' < '0': sorts before the real key, never present
        return k;
    }

    /// <summary>The keys in a pseudo-random order (fixed seed), so point reads touch random blocks.</summary>
    public static byte[][] Shuffled(byte[][] keys)
    {
        byte[][] copy = (byte[][])keys.Clone();
        var rng = new Random(42);
        rng.Shuffle(copy);
        return copy;
    }

    /// <summary>A deterministic value of <paramref name="size"/> bytes, half compressible (like db_bench).</summary>
    public static byte[] Value(int size, int seed = 1)
    {
        var v = new byte[size];
        var rng = new Random(seed);
        rng.NextBytes(v.AsSpan(0, size / 2));
        for (int i = size / 2; i < size; i++) v[i] = (byte)'x';
        return v;
    }

    /// <summary>Default options for scenarios: 4 MiB write buffer (so warm-up rotates memtables), 64 MiB LRU cache, bloom 10.</summary>
    public static ColumnFamilyOptions DefaultCfOptions(long blockCacheBytes = 64L << 20)
    {
        return new ColumnFamilyOptions
        {
            WriteBufferSize = 4UL << 20,
            TableOptions = new BlockBasedTableOptions
            {
                BlockCache = BlockCache.CreateLru(blockCacheBytes),
                FilterPolicy = new BloomFilterPolicy(10),
            },
        };
    }

    public static DbOptions DefaultDbOptions() => new() { CreateIfMissing = true, CreateMissingColumnFamilies = true };

    public RocksDatabase OpenDb(string name, DbOptions? dbOptions = null, ColumnFamilyOptions? cfOptions = null) =>
        Own(RocksDatabase.Open(NewDirectory(name), dbOptions ?? DefaultDbOptions(), cfOptions ?? DefaultCfOptions()));

    /// <summary>Puts every key with <paramref name="value"/>; optionally flushes and compacts everything into one sorted run.</summary>
    public static void Fill(RocksDatabase db, byte[][] keys, ReadOnlySpan<byte> value, bool flush, bool compact)
    {
        var wo = new WriteOptions { DisableWal = true };
        foreach (byte[] k in keys) db.Put(wo, k, value);
        if (flush) db.Flush();
        if (compact) db.CompactRange();
    }

    public static void Fill(ColumnFamily cf, byte[][] keys, ReadOnlySpan<byte> value, bool flush)
    {
        var wo = new WriteOptions { DisableWal = true };
        foreach (byte[] k in keys) cf.Put(wo, k, value);
        if (flush) cf.Flush();
    }

    /// <summary>Reads every key once so the block cache holds the data blocks.</summary>
    public static void WarmCache(RocksDatabase db, byte[][] keys)
    {
        Span<byte> buf = stackalloc byte[1024];
        foreach (byte[] k in keys) db.TryGet(k, buf, out _);
    }

    public async ValueTask DisposeAsync()
    {
        for (int i = _owned.Count - 1; i >= 0; i--)
        {
            switch (_owned[i])
            {
                case IAsyncDisposable ad: await ad.DisposeAsync().ConfigureAwait(false); break;
                case IDisposable d: d.Dispose(); break;
            }
        }
        _owned.Clear();
        try
        {
            if (Directory.Exists(WorkDirectory)) Directory.Delete(WorkDirectory, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}

/// <summary>Cheap index sequence for the measured loops (no allocation, no Random calls).</summary>
internal struct Cursor(int length)
{
    private int _i;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Next()
    {
        int i = _i;
        _i = i + 1 == length ? 0 : i + 1;
        return i;
    }
}
