using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table.Filters;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// Batched MultiGet where the full filter dominates: one 2M-key SST (a 10-bit FastLocalBloom filter of ~2.4 MiB, larger
/// than a core's L2) owned by the table reader, and batches of 16 random keys that are all absent (every key is answered
/// by the filter) or all present (filter, index and cached data blocks). Results are per 16-key batch.
/// </summary>
[MemoryDiagnoser]
public class MultiGetFilterBenchmarks
{
    private const int KeyCount = 2_000_000;
    private const int BatchKeys = 16;
    private const int Batches = 64;

    private string _dir = "";
    private RocksDatabase _db = null!;
    private MultiGetBatch _batch = null!;
    private byte[][] _present = [];
    private byte[][] _absent = [];

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-mgfilter-" + Guid.NewGuid().ToString("N"));
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, WriteBufferSize = 1UL << 30 };
        cf.TableOptions.FormatVersion = 6;
        cf.TableOptions.FilterPolicy = new BloomFilterPolicy(10);
        cf.TableOptions.BlockCache = Cache.BlockCache.CreateLru(1L << 30);
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cf);
        byte[] value = new byte[100];
        new Random(5).NextBytes(value);
        using (WriteBatch wb = _db.CreateWriteBatch())
        {
            for (int i = 0; i < KeyCount; i++)
            {
                wb.Put(Key(i, present: true), value);
                if (wb.Count == 1000)
                {
                    _db.Write(new WriteOptions { DisableWal = true }, wb);
                    wb.Clear();
                }
            }
            if (wb.Count > 0) _db.Write(new WriteOptions { DisableWal = true }, wb);
        }
        _db.Flush();
        var rnd = new Random(7);
        _present = new byte[BatchKeys * Batches][];
        _absent = new byte[BatchKeys * Batches][];
        for (int i = 0; i < _present.Length; i++)
        {
            int k = rnd.Next(KeyCount);
            _present[i] = Key(k, present: true);
            _absent[i] = Key(k, present: false);
        }
        _batch = new MultiGetBatch(BatchKeys);
        // Warm the block cache with the data blocks of the present keys.
        Present();
    }

    /// <summary>16-byte keys: 8-byte big-endian number, then 8 bytes of '0' (present) or '.' (absent).</summary>
    private static byte[] Key(int i, bool present)
    {
        byte[] k = new byte[16];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(k, (ulong)i);
        k.AsSpan(8).Fill(present ? (byte)'0' : (byte)'.');
        return k;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _batch.Dispose();
        _db.Dispose();
        try { Directory.Delete(_dir, true); } catch (IOException) { }
    }

    private int Run(byte[][] keys)
    {
        int found = 0;
        for (int b = 0; b < Batches; b++)
        {
            _batch.Clear();
            for (int j = 0; j < BatchKeys; j++) _batch.Add(keys[b * BatchKeys + j]);
            _db.MultiGet(_batch);
            for (int j = 0; j < BatchKeys; j++) if (_batch.GetStatus(j) == MultiGetStatus.Found) found++;
        }
        return found;
    }

    [Benchmark(OperationsPerInvoke = Batches, Description = "MultiGet(16) absent keys")]
    public int Absent() => Run(_absent);

    [Benchmark(OperationsPerInvoke = Batches, Description = "MultiGet(16) present keys")]
    public int Present() => Run(_present);
}
