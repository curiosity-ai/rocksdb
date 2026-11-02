# Benchmarks

This page explains how RocksDb.Managed is compared with native RocksDB, how to run the comparison and how to read the
results, and summarizes the latest numbers. [bench/README.md](../bench/README.md) is the reference for every flag and
workload of the runner. [perf.md](perf.md) records the optimization work with before/after measurements.

## Method: three engines, one workload

The same db_bench-style workload runs on three engines. Each engine runs in its own process on a fresh DB directory.

| Engine | What runs | Source |
|---|---|---|
| **c-native** | a db_bench-style C program on the RocksDB C API, linked to the same `librocksdb.so` that the `RocksDB` NuGet package ships | `bench/native-c/rocksdb_bench.c` |
| **rocksdb-sharp** | the NuGet `RocksDB` binding (`RocksDbSharp`), with its most efficient span APIs | `bench/RocksDb.Managed.Benchmarks/Native/` |
| **managed** | RocksDb.Managed, synchronous span APIs | `bench/RocksDb.Managed.Benchmarks/DbBench/ManagedEngine.cs` |
| **managed-async** | RocksDb.Managed, `ValueTask` APIs (one task per worker) | same, with `--async` |

c-native against rocksdb-sharp shows what the binding costs (P/Invoke, marshalling, copies). rocksdb-sharp against
managed is the choice a .NET user actually makes. c-native is the baseline for all ratios.

The engines are equivalent in everything except the engine itself (see `DbBench/Workload.cs`, which must stay in sync
with `rocksdb_bench.c`):

- **Keys and values**: db_bench's `GenerateKeyFromInt` keys (16 bytes) and `RandomGenerator` values (50%
  compressible). Random key sequences come from SplitMix64 seeded per benchmark and thread, so every engine reads and
  writes the same keys in the same order.
- **Options**: `create_if_missing`, the same compression on all levels, write buffer size, block-based table
  `format_version` 6, block size, an LRU (or HyperClock) block cache, a 10-bit full Bloom filter, WAL and sync from
  the flags. Everything else is each engine's default.
- **Measurement**: µs/op is wall time divided by total operations, as in db_bench. Percentiles come from a
  per-operation latency histogram (log-linear buckets, at most 1.6% error). The managed engines also report bytes
  allocated per operation (`GC.GetTotalAllocatedBytes`, all threads) and gen0/1/2 collection counts.
- **Point reads**: c-native uses `rocksdb_get`, as rocksdb-sharp's span `Get` does. MultiGet uses
  `rocksdb_batched_multi_get_cf` with pinnable slices in both native legs, and `MultiGetBatch` in managed.

## Running the comparison

```bash
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
cd dotnet
bench/run-all.sh                                   # full matrix + compaction-heavy configuration

# smaller matrix
NUM=200000 READS=100000 NUM_LARGE=50000 THREADS_LIST="1 4" COMPRESSIONS="none lz4" VALUE_SIZES="100 1024" bench/run-all.sh
# only the compaction-heavy configuration
RUN_MATRIX=0 NUM=500000 bench/run-all.sh
# only the integrated BlobDB configuration
RUN_MATRIX=0 RUN_COMPACTION=0 RUN_BLOB=1 NUM=200000 bench/run-all.sh
# repeated runs for statistics (the configuration of the latest results, ~100 min on 4 vCPUs)
REPEATS=5 LOAD_MAX=1.5 NUM=300000 NUM_LARGE=100000 COMPACTION_NUM=500000 bench/run-all.sh
```

With `REPEATS=N` every engine x configuration x benchmark runs N times, each in a fresh process on a fresh DB with
its own warm-up, and the engines are interleaved within each repetition (A B C D, A B C D, ...), so drift of the machine
spreads over all engines. `LOAD_MAX` waits before each repetition until the 1-minute load average is at most that value.
The report then shows medians, min-max and the coefficient of variation per cell, flags noisy rows, and adds 95%
bootstrap confidence intervals to the geometric means (details in [bench/README.md](../bench/README.md)).

`run-all.sh` builds the benchmark project in Release and `bench/native-c` (`make -C bench/native-c`, against the
11.12 `c.h` in `native-c/include` and the NuGet `librocksdb.so`). Then it runs every configuration on every engine and
writes three files to `bench/results/`:

- `<date>-<host>.md`: the report (machine, summary, per-configuration tables, LSM shape)
- `<date>-<host>.csv`: the raw `RESULT` rows
- `<date>-<host>.lsm.csv`: the `STATS` rows

Every knob is an environment variable documented in the script header: `ENGINES`, `BENCHMARKS`, `THREADS_LIST`,
`COMPRESSIONS`, `VALUE_SIZES`, `EXTRA_ARGS`, `TFM`, `NOTES`, `RUN_COMPACTION`, `RUN_BLOB`, and the `COMPACTION_*` and
`BLOB_*` overrides.

The default matrix covers value sizes 100 B and 1 KiB, 1 and 4 threads, and no compression and LZ4, with the
benchmarks `fillseq, fillrandom, overwrite, flush, readrandom, readmissing, seekrandom, readseq, readreverse,
multireadrandom, readwhilewriting, compact, deleterandom, fillsync`. Fill benchmarks start from a fresh DB; the
others use the DB the previous step left behind.

Two additional configurations:

- **compaction** (on by default): 4 MiB write buffer and target file size and a 16 MiB `max_bytes_for_level_base`, so
  fills run against background flushes and compactions and reads hit a multi-level tree. The `stats` benchmark
  records each engine's LSM shape and write amplification.
- **blob** (`RUN_BLOB=1`): 1 KiB values in blob files (`--enable_blob_files 1 --min_blob_size 512`).

To run one engine directly, or to compare a custom set:

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench \
    --engine managed --benchmarks fillrandom,readrandom --num 1000000 --value_size 100
B="dotnet bench/RocksDb.Managed.Benchmarks/bin/Release/net10.0/RocksDb.Managed.Benchmarks.dll"
$B compare --engines c-native,rocksdb-sharp,managed,managed-async --db_base /tmp/bench --csv out.csv -- \
    --benchmarks fillrandom,readrandom --num 1000000 --threads 4 --compression lz4
$B report --csv out.csv --out out.md
```

## Reading a report

- **Cells** are µs/op (lower is better). `flush` and `compact` are milliseconds for one flush or one full compaction.
  Transaction benchmarks are µs per transaction.
- **Ratios** are engine / c-native. Below 1.00x means faster than C, and above means slower.
- **Summary**: the geometric mean of the ratios over a configuration's benchmarks (the count is in parentheses).
  `waitforcompaction` is excluded: its time is the leftover background work, not the speed of an operation.
- **Repeated runs** (`REPEATS` > 1): cells are `median (min-max, CV)`, ratios are median / c-native median, and the
  summary geometric means come with a 95% bootstrap confidence interval in brackets. `!` marks a row where some
  engine's CV exceeds 10%; the "Noisy rows" section lists them. A CI that excludes 1.00x is a difference beyond the
  run-to-run noise.
- **p99** is per operation (per 16-key batch for `multireadrandom`).
- **alloc B/op** is managed bytes allocated per operation. Steady-state reads and seeks are expected to be about 0.
  Fills show the memtable arena (about 144 B per 100-byte entry), and `flush`/`compact` show per-job buffers.
- **LSM shape / write-amp** (compaction configuration): files per level from `rocksdb.num-files-at-level<N>`, and
  flush + compaction bytes written divided by user bytes. The WAL is not counted. The written bytes have a resolution
  of 0.01 GB.
- **Warm code only.** Benchmarks must measure warm code (CLAUDE.md). Every engine runs a warm-up first (`--warmup`,
  default on; see "Warm-up" in `bench/README.md`): the whole workload on a throwaway DB, including flushes and
  compactions, then the measurement on a fresh DB. The `jit` columns show how many methods were JIT-compiled while
  each benchmark was timed (0 - a few after the warm-up; a few hundred in the compaction-heavy fills). Reports before
  `2026-11-02-vm-warm*.md` (and the `-vm-cold*.md` counterparts) were measured cold: their managed numbers include
  JIT and tier-0 code, e.g. the first compaction of a process is 2.5-3.5x slower than later ones. See "Benchmark
  warm-up" in [perf.md](perf.md) for cold vs warm.
- **`fillsync`** measures the device's fsync latency more than the engine. Expect large swings between runs.

**Noise caveat.** All committed results come from a 4-vCPU cloud VM (KVM, Intel Xeon @ 2.80 GHz, 15 GiB). Even on
the quiet machine (no other agents, load average 0.5-1.7 between runs) one measurement of one cell varies by about
10% (median coefficient of variation over 5 repetitions: 9.9% for c-native, 10.0-10.7% for the .NET engines), and 10%
of the cells by more than 20%. Single runs before `2026-11-02-final-warm-r5.md` were taken while other agents ran
builds and tests (load average 4-14) and are noisier. Quote medians of repeated runs (`REPEATS`, see
[bench/README.md](../bench/README.md)) and their confidence intervals, not single cells.

## Latest results

The source is `bench/results/2026-11-02-final-warm-r5.md` (raw rows: `.csv`, LSM rows: `.lsm.csv`): the reduced
matrix (100 B and 1 KiB values, 1 and 4 threads, no compression and LZ4) plus the compaction-heavy configuration, on
c-native, rocksdb-sharp, managed and managed-async, **5 repetitions** each (fresh DB, warm-up on, engines interleaved
in every repetition), Release build, .NET 10.0.14, librocksdb 11.12 (NuGet `RocksDB` 11.12.0.7385). Sizes: num=300k
(300k reads) for 100-byte values, 100k (100k reads) for 1 KiB values, 500k for the compaction-heavy configuration. The
machine was quiet (no other agents; each repetition started at a 1-minute load average <= 1.5; 97 min in total).

Geometric mean over the benchmarks of each configuration of the **median** µs/op ratio vs c-native (lower is better;
below 1.00x is faster than C), with the 95% bootstrap confidence interval:

| configuration | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| 100 B, 1 thread, no compression | 1.06x [1.01-1.13] | 0.59x [0.56-0.63] | 0.64x [0.61-0.68] |
| 100 B, 4 threads, no compression | 1.01x [0.96-1.09] | 0.78x [0.72-0.82] | 0.79x [0.74-0.83] |
| 100 B, 1 thread, LZ4 | 1.11x [1.07-1.16] | 0.69x [0.66-0.73] | 0.76x [0.72-0.79] |
| 100 B, 4 threads, LZ4 | 1.01x [0.97-1.09] | 0.83x [0.77-0.87] | 0.78x [0.73-0.81] |
| 1 KiB, 1 thread, no compression | 1.04x [0.96-1.19] | 0.52x [0.49-0.57] | 0.55x [0.53-0.61] |
| 1 KiB, 4 threads, no compression | 1.03x [0.96-1.08] | 0.63x [0.58-0.66] | 0.60x [0.56-0.63] |
| 1 KiB, 1 thread, LZ4 | 0.99x [0.94-1.07] | 0.58x [0.55-0.62] | 0.64x [0.60-0.68] |
| 1 KiB, 4 threads, LZ4 | 1.02x [0.94-1.11] | 0.70x [0.65-0.75] | 0.74x [0.68-0.79] |
| **all 8 matrix configurations (112 benchmarks)** | **1.03x [1.01-1.06]** | **0.66x [0.64-0.67]** | **0.68x [0.67-0.70]** |
| compaction-heavy (500k x 100 B, 1 thread; 4 benchmarks) | 1.07x [1.00-1.13] | 0.58x [0.51-0.63] | 0.69x [0.64-0.72] |

Per benchmark, geometric mean over the 8 matrix configurations (median ratios, 95% bootstrap CI):

| benchmark | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| fillseq | 0.98x [0.91-1.05] | 0.65x [0.59-0.70] | 0.65x [0.61-0.70] |
| fillrandom | 1.01x [0.95-1.06] | 0.88x [0.82-0.96] | 0.79x [0.75-0.83] |
| overwrite | 1.04x [0.99-1.09] | 1.02x [0.93-1.06] | 0.80x [0.76-0.85] |
| flush | 0.91x [0.84-1.04] | 0.40x [0.37-0.44] | 0.40x [0.37-0.45] |
| readrandom | 1.03x [0.97-1.10] | 0.57x [0.55-0.63] | 0.67x [0.64-0.73] |
| readmissing | 1.04x [0.93-1.17] | 0.58x [0.49-0.61] | 0.88x [0.77-0.95] |
| seekrandom | 1.05x [0.94-1.19] | 0.49x [0.43-0.52] | 0.53x [0.47-0.56] |
| readseq | 1.17x [1.09-1.31] | 0.55x [0.50-0.61] | 0.57x [0.53-0.63] |
| readreverse | 1.14x [1.05-1.26] | 0.59x [0.53-0.64] | 0.60x [0.54-0.66] |
| multireadrandom | 1.03x [0.96-1.12] | 0.71x [0.66-0.77] | 0.76x [0.70-0.81] |
| readwhilewriting | 1.03x [0.95-1.08] | 0.69x [0.64-0.72] | 0.74x [0.68-0.77] |
| compact | 0.97x [0.89-1.12] | 0.68x [0.64-0.77] | 0.70x [0.65-0.78] |
| deleterandom | 1.06x [0.98-1.12] | 0.71x [0.66-0.76] | 0.73x [0.67-0.76] |
| fillsync | 1.04x [0.92-1.28] | 1.01x [0.87-1.09] | 0.94x [0.83-1.03] |

Takeaways:

- **Overall** the managed engine is about a third faster than the C API on the same workload (0.66x, CI 0.64-0.67),
  and rocksdb-sharp costs about 3% over C (1.03x, CI 1.01-1.06). Every configuration's CI for managed and
  managed-async lies below 1.0.
- **Single-threaded writes** are faster than C (`fillseq` 0.47-0.67x, `fillrandom` 0.58-0.78x, `overwrite`
  0.63-0.80x). **With 4 threads the sync write path is slower than C** for random writes: `fillrandom` 1.04-1.31x and
  `overwrite` 1.28-1.81x (managed), while managed-async stays at 0.75-1.03x / 0.77-0.95x (async group commit).
- **Point reads, seeks and scans** are faster than C in every configuration: `readrandom` 0.43-0.69x, `seekrandom`
  0.22-0.65x, `readseq` 0.47-0.66x, `readreverse` 0.41-0.85x, `multireadrandom` 0.52-0.91x. The exception is
  `readmissing` with 100 B values and 4 threads: 1.42-1.68x (managed) and 1.93-3.00x (managed-async), against
  0.33-0.59x (managed) and 0.51-0.73x (managed-async) in the other configurations.
- **Async** adds 0-17% over sync on reads (per-benchmark geometric means; `readmissing` +50%), because calls
  complete synchronously when nothing waits, and is faster than sync for multi-threaded writes.
- **Flush and full compaction**: one full compaction is 0.40-0.52x without compression and 0.90-1.03x with LZ4 (the
  K4os LZ4 encoder is slower than native's); one flush with 100-byte values is 0.67-1.07x. The 1 KiB `flush` rows (0.09-0.34x) are too
  low to be the flush speed alone; the flush benchmark flushes whatever the preceding fills left in the memtable, so
  treat them as not comparable.
- **Compaction-heavy configuration**: managed 0.57x / 0.61x on `fillrandom` / `overwrite` against background flushes
  and compactions, 0.59x `readrandom` and 0.55x `seekrandom` on the multi-level tree. Write amplification: c-native and
  rocksdb-sharp 2.96 in all 5 repetitions, managed-async 2.96 (2.96-2.96), managed 3.05 (2.96-3.15); LSM shapes
  `3 0 0 0 0 4 15` (native, 4-5 of 5 repetitions) against `2 0 0 0 0 3 15` (2/5), `3 0 0 0 0 4 14` or `3 0 0 0 0 4 15` (2/5) and
  `0 0 0 0 0 4 15` (1/5) for managed. The free-running managed writer is faster, so its
  compactions lag slightly differently (see "Compaction-picking parity" in [perf.md](perf.md)).
- **rocksdb-sharp** is at parity with C for most operations; its cost shows on scans (`readseq` 1.17x,
  `readreverse` 1.14x; up to 1.39x with 100 B values).
- **JIT during measurement** (warm-up on): median 1 method per measured benchmark in the matrix (max 71 managed,
  132 managed-async); the compaction-heavy fills still compile 190-420 methods on average while running (background
  compaction code paths that the warm-up's smaller tree does not reach as often). rocksdb-sharp: 0-3.
- **Noise**: 97 of 117 benchmark rows have at least one engine with CV > 10% (listed in the report's "Noisy rows");
  this is the VM's floor (c-native is as noisy as the .NET engines), which is why the summaries use medians and CIs.
  `fillsync` (device fsync latency), `readmissing`, `readseq` / `readreverse` (sub-µs operations) and the single-shot
  `flush` rows are the noisiest.
- **Robustness**: in 1 of 45 managed (sync) processes the warm-up crashed while re-creating its throwaway DB
  (`MANIFEST-000001` not found right after the destroy + open; see the report's notes); its 14 rows were re-measured.
  Cause, fixed afterwards: close did not wait for the obsolete-file purge at the end of a background flush.

Older reports in `bench/results/` are single runs on the shared, loaded VM: `2026-11-02-vm*.md` before perf pass #2,
`-vm-perf2*.md` after it, `-vm-perf3*.md` after perf pass #3, `-vm-picker-compaction.md` after the compaction-picking
parity fixes, and `-vm-warm*.md` / `-vm-cold*.md` for the warm-up comparison. [perf.md](perf.md) keeps their numbers.

## Micro-benchmarks (BenchmarkDotNet)

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --list flat
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbHeadToHead
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbAsync --job short
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*TableBenchmarks*'
```

Add `--cli /root/.dotnet/dotnet` if `dotnet` is not on the child processes' `PATH`. Every class has
`[MemoryDiagnoser]`. The `Allocated` column must be `-` for hot paths.

Latest head-to-head numbers (`bench/results/2026-11-02-final-bdn.md`: `--anyCategories DbHeadToHead DbAsync`, the
default job, quiet machine, .NET 10.0.14; mean per operation, single thread):

| operation | rocksdb-sharp | managed | managed / rocksdb-sharp |
|---|---:|---:|---:|
| Put (memtable, WAL) | 6.68 µs | 3.82 µs | 0.57x |
| WriteBatch of 100 puts | 188.3 µs | 113.6 µs | 0.60x |
| Get from the memtable | 1953 ns | 947 ns | 0.49x |
| Get from a cached SST block | 3782 ns | 1334 ns | 0.35x |
| Get of a missing key (Bloom filter) | 637 ns | 176 ns | 0.28x |
| Seek + 10 Next | 6.43 µs | 1.70 µs | 0.26x |
| Seek + 100 Next | 44.97 µs | 9.36 µs | 0.21x |
| MultiGet of 16 keys | 38.29 µs | 19.89 µs | 0.52x |

No managed hot path allocates (`Allocated` `-`, or 1-5 B amortized pool growth). Sync against async (`DbAsync`, blocks
in the block cache / only in the OS page cache): Put 4.14 / 4.35 µs, TryGet 1.31 / 1.60 µs (page cache 3.26 /
3.80 µs), Seek + 10 Next 2.09 / 2.33 µs, MultiGet(16) 19.1 / 19.1 µs (page cache 44.9 / 48.5 µs): async costs 0-20%
when every call completes synchronously.

| Class | Category | Measures |
|---|---|---|
| `DbPutBenchmarks`, `DbWriteBatchBenchmarks`, `DbGetBenchmarks`, `DbIteratorBenchmarks`, `DbMultiGetBenchmarks` | `DbHeadToHead` (+ `Put`, `WriteBatch`, `Get`, `Iterator`, `MultiGet`) | managed against rocksdb-sharp, per operation: Put, a 100-entry batch, Get from the memtable / a cached SST / a miss, Seek + 10 or 100 Next, MultiGet of 16 keys |
| `DbAsyncBenchmarks` | `DbAsync` | sync against `ValueTask` APIs for Put, TryGet, Seek + 10 Next and MultiGet(16), with blocks in the block cache or only in the page cache |
| `TableBenchmarks`, `TableConcurrentGetBenchmarks` | | SST point lookups (cached, uncached, concurrent) and table building |
| `IteratorBenchmarks` | | table, merging and DB iterator stepping |
| `MemTableBenchmarks`, `SkipListBenchmarks` | | memtable insert and lookup, the lock-free skip list |
| `CacheBenchmarks` | | LRU and HyperClock lookup and insert, single- and multi-threaded |
| `FilterBenchmarks`, `HashBenchmarks`, `CompressionBenchmarks` | `FiltersHashCompression` | Bloom and Ribbon build and query, XXH3 / xxHash64 / crc32c, the codecs (some against native counterparts) |
| `WalBenchmarks`, `WriteBatchBenchmarks` | | WAL record writing and reading, WriteBatch building and parsing |
| `EnvBenchmarks`, `UtilBenchmarks` | | file I/O paths (sync and async, `preadv2` fast path), varints and coding helpers |

For BenchmarkDotNet comparisons on a shared machine, follow [perf.md](perf.md#method): alternate the baseline and
the candidate in the same time window (`--job short --iterationCount 8`), and report medians.
