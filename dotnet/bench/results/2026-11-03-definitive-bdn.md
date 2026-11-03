# BenchmarkDotNet: DbHeadToHead and DbAsync (definitive, default job)

Run 2026-11-03 12:15-13:04 UTC on the quiet 4-vCPU KVM VM, with no other agents and nothing else running. The
1-minute load average was 1.42 at the start and 1.63 at the end. The run came right after the 5-repetition dbbench
matrix (`2026-11-03-definitive-r5.md`) and its extras, on tree `fb97d3d`. The CPU is the same as in
`2026-11-03-final-bdn.md` (tree `cfb30f9`): "Intel Xeon Processor @ 2.10GHz", family 6 model 207. Command:

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbHeadToHead DbAsync --cli /root/.dotnet/dotnet
```

The run used BenchmarkDotNet's **default job** (pilot, 6+ warm-up iterations until stable, 15-100 measured iterations,
outliers removed) with `[MemoryDiagnoser]`, on .NET 10.0.14 x64 RyuJIT (x86-64-v4). Times are per operation
(`OperationsPerInvoke`). `Engine=RocksDbSharp` is the NuGet `RocksDB` 11.12 binding (native `librocksdb.so.11.12`);
`Managed` is RocksDb.Managed. `Allocated` `-` means 0 bytes per operation. The 1-26 B values are the amortized cost of
rare pool or memtable arena growth: 26 B per 100-entry batch is 0.26 B per entry. `DbGetBenchmarks` now has
`Location = SstUncached` as well, which uses a 1 MiB block cache, so nearly every Get reads its data block from the
page cache.

## Summary

| operation | rocksdb-sharp | managed | managed / rocksdb-sharp | cfb30f9 ratio (2026-11-03-final-bdn) |
|---|---:|---:|---:|---:|
| Put (memtable, WAL) | 3.98 µs | 2.72 µs | 0.68x | 0.72x |
| WriteBatch of 100 puts | 130.7 µs | 58.9 µs | 0.45x | 0.56x |
| Get from the memtable | 1451 ns | 838 ns | 0.58x | 0.45x |
| Get from a cached SST block | 1982 ns | 1101 ns | 0.56x | 0.55x |
| Get of a missing key (Bloom filter) | 400 ns | 113 ns | 0.28x | 0.30x |
| Get, block cache miss (1 MiB cache, page cache) | 3636 ns | 2104 ns | 0.58x | - |
| Seek + 10 Next | 4.17 µs | 0.99 µs | 0.24x | 0.25x |
| Seek + 100 Next | 27.27 µs | 5.78 µs | 0.21x | 0.21x |
| MultiGet of 16 keys | 27.73 µs | 18.07 µs | 0.65x | 0.62x |

**No hot path allocates.** Every `Allocated` cell is `-`, except for 1 B per managed Put and 26 B per 100-entry batch
(the same as in the previous run) and 1 B per async MultiGet(16). That includes the block-cache-miss Get
(`SstUncached`). The memtable Get moved from 765 to 838 ns for managed and from 1716 to 1451 ns for rocksdb-sharp. Both
differences are within one standard deviation of the cells (134-137 ns). The other ratios are within a few percent of
the previous run.

`DbAsync` results, as sync / async with blocks in the block cache (page cache in parentheses):

- Put: 2.62 / 2.67 µs (2.54 / 2.69)
- TryGet: 0.96 / 1.12 µs (2.03 / 2.24)
- Seek + 10 Next: 1.36 / 1.35 µs (2.68 / 2.83)
- MultiGet(16): 15.1 / 14.4 µs (32.5 / 33.9)

Async costs -5% to +17% when every call completes synchronously; TryGet costs the most (the previous run: 0-11%).

The per-class reports below are the unmodified `*-report-github.md` files.

## DbPutBenchmarks

```

BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.5 LTS (Noble Numbat)
Intel Xeon Processor 2.10GHz, 1 CPU, 4 logical and 4 physical cores
.NET SDK 11.0.100-rc.3.26522.148
  [Host]     : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4
  Job-JMDAGQ : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4

Toolchain=.NET 10.0  

```
| Method | Engine       | Mean     | Error     | StdDev    | Allocated |
|------- |------------- |---------:|----------:|----------:|----------:|
| **Put**    | **RocksDbSharp** | **3.977 μs** | **0.1871 μs** | **0.5457 μs** |         **-** |
| **Put**    | **Managed**      | **2.723 μs** | **0.0895 μs** | **0.2626 μs** |       **1 B** |

## DbWriteBatchBenchmarks

```

BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.5 LTS (Noble Numbat)
Intel Xeon Processor 2.10GHz, 1 CPU, 4 logical and 4 physical cores
.NET SDK 11.0.100-rc.3.26522.148
  [Host]     : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4
  Job-JMDAGQ : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4

Toolchain=.NET 10.0  

```
| Method                  | Engine       | Mean      | Error    | StdDev    | Allocated |
|------------------------ |------------- |----------:|---------:|----------:|----------:|
| **&#39;WriteBatch(100) write&#39;** | **RocksDbSharp** | **130.71 μs** | **5.174 μs** | **14.845 μs** |         **-** |
| **&#39;WriteBatch(100) write&#39;** | **Managed**      |  **58.88 μs** | **1.965 μs** |  **5.701 μs** |      **26 B** |

## DbGetBenchmarks

```

BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.5 LTS (Noble Numbat)
Intel Xeon Processor 2.10GHz, 1 CPU, 4 logical and 4 physical cores
.NET SDK 11.0.100-rc.3.26522.148
  [Host]     : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4
  Job-JMDAGQ : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4

Toolchain=.NET 10.0  

```
| Method | Location    | Engine       | Mean       | Error     | StdDev    | Allocated |
|------- |------------ |------------- |-----------:|----------:|----------:|----------:|
| **Get**    | **Memtable**    | **RocksDbSharp** | **1,451.1 ns** |  **45.67 ns** | **133.95 ns** |         **-** |
| **Get**    | **Memtable**    | **Managed**      |   **838.1 ns** |  **46.80 ns** | **137.26 ns** |         **-** |
| **Get**    | **SstCached**   | **RocksDbSharp** | **1,981.7 ns** |  **64.29 ns** | **181.33 ns** |         **-** |
| **Get**    | **SstCached**   | **Managed**      | **1,101.4 ns** |  **30.85 ns** |  **88.51 ns** |         **-** |
| **Get**    | **Miss**        | **RocksDbSharp** |   **399.6 ns** |  **12.69 ns** |  **36.00 ns** |         **-** |
| **Get**    | **Miss**        | **Managed**      |   **113.0 ns** |   **2.43 ns** |   **7.08 ns** |         **-** |
| **Get**    | **SstUncached** | **RocksDbSharp** | **3,636.1 ns** | **123.49 ns** | **358.27 ns** |         **-** |
| **Get**    | **SstUncached** | **Managed**      | **2,103.9 ns** |  **64.61 ns** | **189.48 ns** |         **-** |

## DbIteratorBenchmarks

```

BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.5 LTS (Noble Numbat)
Intel Xeon Processor 2.10GHz, 1 CPU, 4 logical and 4 physical cores
.NET SDK 11.0.100-rc.3.26522.148
  [Host]     : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4
  Job-JMDAGQ : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4

Toolchain=.NET 10.0  

```
| Method            | Nexts | Engine       | Mean        | Error     | StdDev      | Allocated |
|------------------ |------ |------------- |------------:|----------:|------------:|----------:|
| **&#39;Seek + N x Next&#39;** | **10**    | **RocksDbSharp** |  **4,170.2 ns** | **156.56 ns** |   **461.62 ns** |         **-** |
| **&#39;Seek + N x Next&#39;** | **10**    | **Managed**      |    **993.6 ns** |  **30.95 ns** |    **89.30 ns** |         **-** |
| **&#39;Seek + N x Next&#39;** | **100**   | **RocksDbSharp** | **27,269.6 ns** | **758.16 ns** | **2,163.07 ns** |         **-** |
| **&#39;Seek + N x Next&#39;** | **100**   | **Managed**      |  **5,784.1 ns** | **209.48 ns** |   **604.40 ns** |         **-** |

## DbMultiGetBenchmarks

```

BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.5 LTS (Noble Numbat)
Intel Xeon Processor 2.10GHz, 1 CPU, 4 logical and 4 physical cores
.NET SDK 11.0.100-rc.3.26522.148
  [Host]     : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4
  Job-JMDAGQ : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4

Toolchain=.NET 10.0  

```
| Method       | Engine       | Mean     | Error    | StdDev   | Allocated |
|------------- |------------- |---------:|---------:|---------:|----------:|
| **MultiGet(16)** | **RocksDbSharp** | **27.73 μs** | **0.857 μs** | **2.486 μs** |         **-** |
| **MultiGet(16)** | **Managed**      | **18.07 μs** | **0.768 μs** | **2.228 μs** |         **-** |

## DbAsyncBenchmarks

```

BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.5 LTS (Noble Numbat)
Intel Xeon Processor 2.10GHz, 1 CPU, 4 logical and 4 physical cores
.NET SDK 11.0.100-rc.3.26522.148
  [Host]     : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4
  Job-JMDAGQ : .NET 10.0.14 (10.0.14, 10.0.1426.52326), X64 RyuJIT x86-64-v4

Toolchain=.NET 10.0  

```
| Method          | Blocks     | Mean        | Error     | StdDev      | Allocated |
|---------------- |----------- |------------:|----------:|------------:|----------:|
| **Put**             | **BlockCache** |  **2,622.0 ns** | **103.50 ns** |   **303.55 ns** |         **-** |
| PutAsync        | BlockCache |  2,673.1 ns |  86.10 ns |   251.17 ns |         - |
| TryGet          | BlockCache |    959.8 ns |  20.48 ns |    59.75 ns |         - |
| TryGetAsync     | BlockCache |  1,122.7 ns |  29.92 ns |    87.74 ns |         - |
| Seek10          | BlockCache |  1,356.7 ns |  44.56 ns |   129.98 ns |         - |
| Seek10Async     | BlockCache |  1,348.6 ns |  44.50 ns |   130.50 ns |         - |
| MultiGet16      | BlockCache | 15,115.7 ns | 445.42 ns | 1,292.25 ns |         - |
| MultiGet16Async | BlockCache | 14,411.2 ns | 370.94 ns | 1,076.16 ns |       1 B |
| **Put**             | **PageCache**  |  **2,541.4 ns** |  **98.71 ns** |   **289.49 ns** |       **1 B** |
| PutAsync        | PageCache  |  2,694.0 ns | 108.47 ns |   318.14 ns |         - |
| TryGet          | PageCache  |  2,033.4 ns |  44.55 ns |   131.37 ns |         - |
| TryGetAsync     | PageCache  |  2,241.5 ns |  60.15 ns |   175.46 ns |         - |
| Seek10          | PageCache  |  2,681.7 ns |  79.71 ns |   232.52 ns |         - |
| Seek10Async     | PageCache  |  2,826.2 ns |  67.14 ns |   191.56 ns |         - |
| MultiGet16      | PageCache  | 32,465.5 ns | 712.36 ns | 2,043.90 ns |         - |
| MultiGet16Async | PageCache  | 33,906.0 ns | 931.85 ns | 2,732.97 ns |       1 B |
