using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Benchmarks.DbBench;

namespace RocksDb.Managed.Benchmarks;

// Attribute-group reads (one entity split across two column families) next to the plain batched read they are built
// on, all from cached SST blocks:
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories AttributeGroups

/// <summary>GetAttributeGroups / MultiGetAttributeGroups with reused result groups, and a reused MultiGetBatch.</summary>
[MemoryDiagnoser]
[BenchmarkCategory("AttributeGroups")]
public class AttributeGroupBenchmarks
{
    private const int KeyCount = 50_000;
    private const int OpsPerInvoke = 1024;
    private const int KeySize = 16;

    private RocksDatabase _db = null!;
    private string _dir = "";
    private byte[][] _keys = null!;
    private PinnableAttributeGroup[] _two = null!;
    private PinnableAttributeGroup[] _one = null!;
    private ReadOnlyMemory<byte>[] _batchKeys = null!;
    private PinnableAttributeGroup[][] _batchResults = null!;
    private MultiGetBatch _batch = null!;
    private ColumnFamily _hot = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-attrgroups-" + Environment.ProcessId);
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
        var cfo = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { BlockCache = Cache.BlockCache.CreateLru(64L << 20) } };
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cfo);
        _hot = _db.CreateColumnFamily("hot", cfo);
        ColumnFamily cold = _db.CreateColumnFamily("cold", cfo);
        WideColumn[] hc = [WideColumn.Default(new byte[40]), new WideColumn("n"u8.ToArray(), new byte[8])];
        WideColumn[] cc = [new WideColumn("city"u8.ToArray(), new byte[20]), new WideColumn("email"u8.ToArray(), new byte[30])];
        AttributeGroup[] groups = [new AttributeGroup(_hot, hc), new AttributeGroup(cold, cc)];
        byte[] key = new byte[KeySize];
        var wo = new WriteOptions { DisableWal = true };
        for (int i = 0; i < KeyCount; i++)
        {
            KeyFormat.Make(key, (ulong)i);
            _db.PutAttributeGroups(wo, key, groups);
        }
        _hot.Flush();
        cold.Flush();
        var rng = new SplitMix64(42);
        _keys = new byte[OpsPerInvoke][];
        for (int i = 0; i < _keys.Length; i++)
        {
            _keys[i] = new byte[KeySize];
            KeyFormat.Make(_keys[i], rng.Uniform(KeyCount));
        }
        _two = [new PinnableAttributeGroup(_hot), new PinnableAttributeGroup(cold)];
        _one = [new PinnableAttributeGroup(_hot)];
        _batchKeys = new ReadOnlyMemory<byte>[16];
        _batchResults = new PinnableAttributeGroup[16][];
        for (int i = 0; i < 16; i++) _batchResults[i] = [new PinnableAttributeGroup(_hot), new PinnableAttributeGroup(cold)];
        _batch = new MultiGetBatch(16);
        for (int i = 0; i < _keys.Length; i++) _db.GetAttributeGroups(null, _keys[i], _two);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _batch.Dispose();
        _db.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    /// <summary>GetAttributeGroups over two column families (implicit consistent view). Per key.</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public int GetAttributeGroups2Cf()
    {
        int n = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            _db.GetAttributeGroups(null, _keys[i], _two);
            n += _two[1].Columns.Count;
        }
        return n;
    }

    /// <summary>GetAttributeGroups of one column family. Per key.</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public int GetAttributeGroups1Cf()
    {
        int n = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            _db.GetAttributeGroups(null, _keys[i], _one);
            n += _one[0].Columns.Count;
        }
        return n;
    }

    /// <summary>MultiGetAttributeGroups of 16 keys x 2 column families. Per key.</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public int MultiGetAttributeGroups16()
    {
        int n = 0;
        for (int i = 0; i < _keys.Length; i += 16)
        {
            for (int j = 0; j < 16; j++) _batchKeys[j] = _keys[(i + j) % _keys.Length];
            _db.MultiGetAttributeGroups(null, _batchKeys, _batchResults);
            n += _batchResults[0][0].Columns.Count;
        }
        return n;
    }

    /// <summary>The underlying reused-batch MultiGetEntity of 16 keys on one column family. Per key.</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public int MultiGetEntity16()
    {
        int n = 0;
        for (int i = 0; i < _keys.Length; i += 16)
        {
            _batch.Clear();
            for (int j = 0; j < 16; j++) _batch.Add(_keys[(i + j) % _keys.Length]);
            _hot.MultiGetEntity(_batch);
            n += _batch.FoundCount;
        }
        return n;
    }
}
