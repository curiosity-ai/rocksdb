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
# repeated runs for statistics (the configuration of the latest results, 80-100 min on 4 vCPUs)
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
- **alloc B/op** is managed bytes allocated per operation (all threads, without the runner's own objects). Steady-state
  reads and seeks are expected to be about 0. Fills show the memtable arena (about 144 B per 100-byte entry) when no
  pooled arena slabs are left, and `flush`/`compact` show per-job buffers. The first read benchmark on a fresh DB
  includes filling the empty 8 MiB block cache (~85 B/op at 100k reads, ~29 B/op at 300k), and `overwrite` in the
  300k x 100 B configurations the arena of the second memtable (~80 B/op): memory growth up to the configured sizes,
  recycled afterwards (bench/README.md, "Allocations").
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

**Noise caveat.** All committed results come from a 4-vCPU cloud VM (KVM, 15 GiB): "Intel Xeon @ 2.80 GHz" up to
`2026-11-02-*`, "Intel Xeon Processor @ 2.10GHz" (a newer core, model 207) from `2026-11-03-*` on. Absolute times
and even ratios vs C differ between the two CPUs (see "Latest results"). Even on the quiet machine (no other agents,
load average 0.5-1.7 between runs) one measurement of one cell varies by about 10% (median coefficient of variation
over 5 repetitions: 7.8-10.7% depending on the engine and run), and 10% of the cells by more than 20%. Single runs before `2026-11-02-final-warm-r5.md` were taken while other agents ran
builds and tests (load average 4-14) and are noisier. Quote medians of repeated runs (`REPEATS`, see
[bench/README.md](../bench/README.md)) and their confidence intervals, not single cells.

## Latest results

**Current reference run:** `bench/results/2026-11-03-definitive-r5.md` (raw rows: `.csv`, LSM rows: `.lsm.csv`). It
covers the reduced matrix (100 B and 1 KiB values, 1 and 4 threads, no compression and LZ4) and the compaction-heavy
configuration, on c-native, rocksdb-sharp, managed and managed-async. Each cell has **5 repetitions** (fresh DB, warm-up
on, engines interleaved in every repetition). Release build of `fb97d3d`, .NET 10.0.14, librocksdb 11.12 (NuGet
`RocksDB` 11.12.0.7385). Sizes: num=300k (300k reads) for 100-byte values, 100k (100k reads) for 1 KiB values, 500k for
the compaction-heavy configuration. The machine was quiet: no other agents, and each repetition started at a 1-minute
load average <= 1.5 (0.71-3.21 before the individual runs, median 1.44). The run took 79 min.

The run used the same CPU, matrix and sizes as the previous run, `2026-11-03-final-warm-r5.md` (tree `cfb30f9`, "Intel
Xeon Processor @ 2.10GHz", family 6 model 207), so the two compare cell by cell. The code in between fixed the flush
waiter (WAL deletion), the user-iterator and compaction readahead, compaction allocations and subcompaction warm-up
([perf.md](perf.md), "Final-run regressions"). The machine itself did not change speed: the native legs' absolute
medians are 0.99x (c-native) and 0.98x (rocksdb-sharp) of the previous run's, as a geometric mean over the 116 cells.
The managed engines' medians are 0.92x (sync) and 0.93x (async) of the previous run's.

Geometric mean over the benchmarks of each configuration of the **median** µs/op ratio vs c-native (lower is better;
below 1.00x is faster than C), with the 95% bootstrap confidence interval. The previous run is in parentheses:

| configuration | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| 100 B, 1 thread, no compression | 1.09x [1.02-1.13] (1.08x) | 0.68x [0.65-0.71] (0.72x) | 0.70x [0.66-0.72] (0.73x) |
| 100 B, 4 threads, no compression | 1.01x [0.96-1.07] (1.05x) | 0.76x [0.72-0.79] (0.77x) | 0.75x [0.70-0.78] (0.75x) |
| 100 B, 1 thread, LZ4 | 1.10x [1.02-1.14] (1.07x) | 0.68x [0.64-0.72] (0.71x) | 0.70x [0.65-0.73] (0.74x) |
| 100 B, 4 threads, LZ4 | 1.06x [1.01-1.12] (1.04x) | 0.74x [0.70-0.77] (0.76x) | 0.75x [0.70-0.78] (0.74x) |
| 1 KiB, 1 thread, no compression | 1.01x [0.93-1.09] (1.05x) | 0.69x [0.63-0.75] (0.80x) | 0.69x [0.64-0.74] (0.81x) |
| 1 KiB, 4 threads, no compression | 1.00x [0.95-1.07] (1.04x) | 0.77x [0.73-0.81] (0.84x) | 0.75x [0.71-0.79] (0.85x) |
| 1 KiB, 1 thread, LZ4 | 1.00x [0.94-1.05] (1.03x) | 0.67x [0.65-0.71] (0.76x) | 0.73x [0.70-0.77] (0.76x) |
| 1 KiB, 4 threads, LZ4 | 1.09x [1.05-1.17] (1.02x) | 0.81x [0.77-0.84] (0.89x) | 0.81x [0.76-0.84] (0.84x) |
| **all 8 matrix configurations (112 benchmarks)** | **1.05x [1.02-1.06]** (1.05x) | **0.72x [0.71-0.74]** (0.78x) | **0.73x [0.72-0.74]** (0.78x) |
| compaction-heavy (500k x 100 B, 1 thread; 4 benchmarks) | 1.06x [1.00-1.12] (1.05x) | 0.58x [0.54-0.62] (0.62x) | 0.59x [0.55-0.62] (0.65x) |

Per benchmark, geometric mean over the 8 matrix configurations (median ratios, 95% bootstrap CI; previous run in
parentheses):

| benchmark | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| fillseq | 1.00x [0.96-1.07] | 0.68x [0.64-0.73] (0.66x) | 0.66x [0.63-0.69] (0.67x) |
| fillrandom | 1.03x [0.96-1.06] | 0.74x [0.71-0.79] (0.74x) | 0.77x [0.72-0.79] (0.79x) |
| overwrite | 1.03x [0.97-1.06] | 0.77x [0.73-0.80] (0.86x) | 0.78x [0.73-0.82] (0.86x) |
| flush | 0.97x [0.82-1.05] | **0.73x [0.62-0.82]** (1.43x) | **0.73x [0.61-0.78]** (1.44x) |
| readrandom | 1.04x [0.99-1.10] | 0.67x [0.63-0.69] (0.67x) | 0.73x [0.69-0.77] (0.71x) |
| readmissing | 1.11x [0.99-1.22] | 0.60x [0.54-0.66] (0.62x) | 0.70x [0.64-0.75] (0.72x) |
| seekrandom | 1.09x [1.05-1.12] | 0.61x [0.60-0.64] (0.72x) | 0.63x [0.61-0.65] (0.63x) |
| readseq | 1.18x [1.07-1.25] | 0.66x [0.61-0.71] (0.67x) | 0.66x [0.60-0.69] (0.67x) |
| readreverse | 1.14x [1.05-1.20] | 0.69x [0.64-0.74] (0.64x) | 0.68x [0.62-0.72] (0.64x) |
| multireadrandom | 1.04x [1.00-1.09] | 0.71x [0.68-0.75] (0.73x) | 0.73x [0.70-0.78] (0.73x) |
| readwhilewriting | 1.03x [0.98-1.09] | 0.78x [0.75-0.82] (0.79x) | 0.78x [0.74-0.83] (0.79x) |
| compact | 0.99x [0.94-1.07] | 0.84x [0.81-0.90] (0.88x) | 0.82x [0.76-0.88] (0.86x) |
| deleterandom | 1.03x [0.97-1.12] | 0.71x [0.67-0.76] (0.70x) | 0.75x [0.72-0.80] (0.74x) |
| fillsync | 0.97x [0.87-1.17] | 1.02x [0.91-1.10] (1.11x) | 0.90x [0.81-1.03] (0.87x) |

Takeaways:

- **Overall**, the managed engine takes 0.72x of the C API's time on the same workload (CI 0.71-0.74). The async API
  takes 0.73x and rocksdb-sharp 1.05x. Every configuration's CI for managed and managed-async lies below 1.0. Against
  the previous run, the gain is 6 points on the same CPU, mostly from `flush`, 1 KiB seeks and overwrites.
- **`flush` is no longer a slow spot.** It went from 1.02-2.06x of C to 0.63-0.93x (async 0.66-1.02x). A flush of the
  64 MiB memtable takes 62-71 ms (async 56-70) against C's 85-105 ms with 100 B values, and 32-38 ms (async 30-50)
  against 39-56 ms with 1 KiB values. The flush waiter no longer waits for the close of the deleted 64+ MiB WAL (perf.md).
- **`seekrandom` with 1 KiB values, 1 thread, no compression** dropped from 1.21x to 0.64x, after the readahead
  became an OS `readahead` hint instead of a copied prefetch buffer. Sync `readseq` no longer allocates (0.0-1.0 B/op,
  was 7-52).
- **Only 2 of 232 managed cells are above 1.1x of C**, both on rows with CVs of 16-20%:
  - managed-async `readmissing`, 100 B, 4 threads, LZ4: 1.11x (0.21 against 0.19 µs; previous run 0.69x). C was 34%
    faster than in its previous run on this cell. Likely cause: noise on a sub-µs bloom-filter miss, plus the async
    API's per-call cost, which shows most on the cheapest operation. Sync is 0.80x.
  - managed `fillsync`, 1 KiB, 4 threads, LZ4: 1.17x (previous run 1.23x). Likely cause: one fdatasync per put, so
    the row measures the device (C's CV is 20%). Async is 1.01x.
- **Writes**: single-threaded fills are 0.49-0.71x of C and 4-thread fills 0.76-0.98x. `overwrite` is 0.65-0.90x, down
  from 0.86x to 0.77x overall.
- **Reads, seeks and scans**: `readrandom` 0.56-0.75x, `readmissing` 0.37-0.85x, `seekrandom` 0.54-0.68x, `readseq`
  0.54-0.79x, `readreverse` 0.63-0.86x, `multireadrandom` 0.61-0.85x. On reads, async costs 0-17% over sync
  (per-benchmark geometric means; the most on `readmissing`, the cheapest operation).
- **Full compaction** takes 0.74-0.97x of C (the slowest is 1 KiB with LZ4 on 4 threads). One full compaction of the
  100 B DB allocates 6-7.6 MB, down from 31-39 MB. The flush rows allocate 0.05-0.55 MB per flush, with the larger
  values in the 100 B configurations (previous run 0.04-0.25 MB; not investigated).
- **Compaction-heavy configuration**: managed is 0.65x / 0.68x of C on `fillrandom` / `overwrite` against background
  flushes and compactions. On the multi-level tree it is 0.66x on `readrandom` and 0.40x on `seekrandom`. The fills
  allocate 4.8 / 10.4 B/op, down from 77 / 119 B/op. Write amplification: c-native and rocksdb-sharp are at 2.96 in
  all 5 repetitions (shape `3 0 0 0 0 4 14-15`). managed is at 3.15 (3.05-3.15) and managed-async at 3.15 in all 5,
  with shapes `0-1 0 0 0 0 3-6 15-16`.
- **rocksdb-sharp** stays at about 5% over C. Its cost shows on scans (`readseq` 1.18x, `readreverse` 1.14x) and
  `readmissing` (1.11x).
- **Allocations**: steady-state reads, seeks and scans allocate 0.0-1.0 B/op, as do the single-threaded 100 B
  fills. The exceptions are memory growth that is recycled later (see "Reading a report"). The first `readrandom`
  fills the block cache (29 / 86 B/op). `overwrite` with 100 B values allocates the second memtable's arena (16-130
  B/op). 1 KiB fills (6-22 B/op) and 4-thread async fills (12-26 B/op) are at the previous run's level. `fillsync` now shows 2-71 B/op; the previous run's 206-1590 B/op were the runner's own
  histograms spread over num/1000 ops.
- **JIT during measurement** (warm-up on): the median is 1 method per measured benchmark for both managed engines.
  The compaction-heavy `overwrite` still compiles up to 635 methods while running (managed total 4024 methods over
  585 measurements, previous run 5579). rocksdb-sharp compiles 0-5.
- **Noise**: 82 of 116 benchmark rows have at least one engine with a CV above 10%. The median CV is 8.7% for
  c-native, 7.9% for rocksdb-sharp, 8.0% for managed and 7.8% for managed-async (16-22% at the 90th percentile). No
  process crashed.

**Extra rows** (`bench/results/2026-11-03-definitive-extras-r3.md`, 3 repetitions, same session; µs/op medians, ratio
vs C, previous run's extras in parentheses):

| workload | c-native | managed | managed-async |
|---|---:|---:|---:|
| 4 threads, 300k x 100 B, `fillrandom` / `overwrite`, default write path | 5.30 / 5.01 | 4.17 / 4.26: 0.79x / 0.85x (0.86x / 0.97x) | 4.84 / 4.84: 0.91x / 0.97x (0.91x / 0.92x) |
| same, `--enable_pipelined_write true` | 4.42 / 4.34 | 3.16 / 3.71: 0.71x / 0.86x (0.91x / 0.91x) | 4.28 / 4.61: 0.97x / 1.06x (1.25x / 1.11x) |
| same, `--unordered_write true` | 2.79 / 3.03 | 2.29 / 2.60: 0.82x / 0.86x (0.70x / 0.82x) | 2.92 / 2.86: 1.05x / 0.94x (0.94x / 0.91x) |
| full `compact` of 1M x 100 B in L0, `max_subcompactions` 1 (ms) | 706 | 535: 0.76x (0.84x) | |
| same, `max_subcompactions` 4 (ms) | 315 | 269: **0.85x** (1.47x) | |
| 100k x 1 KiB, `readreverse` | 3.25 | 0.95: 0.29x (0.27x) | 0.86: 0.26x (0.34x) |
| 100k x 1 KiB, `seekrandom --reverse_iterator` (SeekForPrev + 10 Prev) | 16.45 | 11.26: 0.68x (0.69x) | 11.43: 0.69x (0.74x) |
| 100k x 1 KiB, `flush` after fillseq, fillrandom, overwrite (ms), no compression / LZ4 | 43.9 / 43.8 | 36.8 / 35.1: 0.84x / 0.80x | 29.2 / 37.7: 0.67x / 0.86x |

- **Subcompactions now scale**: 4 subcompactions take managed from 535 to 269 ms (2.0x) and C from 706 to 315 ms
  (2.2x). The previous run had 586 -> 484 ms (1.2x), when the measured compaction was still tiering up 180-410 methods.
  A full compaction allocates 0.44 MB with 1 subcompaction and 3.4 MB with 4, down from 74 MB and 294 MB.
- **Pipelined writes**: managed gains 13-24% with 4 sync writers, native 13-17%. Async writers are now at
  0.97-1.06x of C with pipelining, up from 1.11-1.25x. `unordered_write` gains 39-45% on managed and 40-47% on native.
  This run's C was faster on this row than in the previous run (2.79 against 3.31 µs), so the ratio rose while the
  managed times stayed the same.
- **1 KiB flush** (the regression of the previous run): 0.80-0.84x of C sync, 0.67-0.86x async, in line with the
  matrix.

The previous run, `2026-11-03-final-warm-r5.md` (tree `cfb30f9`, same CPU), had managed 0.78x [0.76-0.80],
managed-async 0.78x [0.76-0.79] and rocksdb-sharp 1.05x [1.02-1.07] over the 8 matrix configurations. Its slow spots
were `flush` (1.02-2.06x) and 1 KiB single-threaded `seekrandom` (1.21x). The final run before it,
`2026-11-02-final-warm-r5.md` (tree `0f866b8`), ran on a different VM CPU ("Intel Xeon @ 2.80GHz"), so its ratios
(managed 0.66x, rocksdb-sharp 1.03x) are not comparable 1:1. [perf.md](perf.md) ("Final three-way results", "Final run
on a new VM CPU", "Definitive run") compares the three.

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

Latest head-to-head numbers come from `bench/results/2026-11-03-definitive-bdn.md`: `--anyCategories DbHeadToHead
DbAsync`, the default job, on the quiet machine with the 2.10 GHz CPU and .NET 10.0.14, tree `fb97d3d`. Times are the
mean per operation on a single thread. The last column is the ratio of the previous run on the same CPU
(`2026-11-03-final-bdn.md`, tree `cfb30f9`):

| operation | rocksdb-sharp | managed | managed / rocksdb-sharp | cfb30f9 |
|---|---:|---:|---:|---:|
| Put (memtable, WAL) | 3.98 µs | 2.72 µs | 0.68x | 0.72x |
| WriteBatch of 100 puts | 130.7 µs | 58.9 µs | 0.45x | 0.56x |
| Get from the memtable | 1451 ns | 838 ns | 0.58x | 0.45x |
| Get from a cached SST block | 1982 ns | 1101 ns | 0.56x | 0.55x |
| Get of a missing key (Bloom filter) | 400 ns | 113 ns | 0.28x | 0.30x |
| Get, block cache miss (1 MiB cache, block from the page cache) | 3636 ns | 2104 ns | 0.58x | |
| Seek + 10 Next | 4.17 µs | 0.99 µs | 0.24x | 0.25x |
| Seek + 100 Next | 27.27 µs | 5.78 µs | 0.21x | 0.21x |
| MultiGet of 16 keys | 27.73 µs | 18.07 µs | 0.65x | 0.62x |

No managed hot path allocates, including Gets that miss the block cache. `Allocated` is `-` everywhere except 1 B per
Put, 26 B per 100-entry batch and 1 B per async MultiGet(16), which are amortized memtable arena and pool growth. The
memtable Get moved by about one standard deviation on both sides (managed 765 -> 838 ns, rocksdb-sharp
1716 -> 1451 ns). The other ratios are within a few percent of the previous run.

`DbAsync` compares the sync and async APIs, with blocks in the block cache (blocks only in the OS page cache in
parentheses):

| operation | sync | async |
|---|---:|---:|
| Put | 2.62 µs (2.54) | 2.67 µs (2.69) |
| TryGet | 0.96 µs (2.03) | 1.12 µs (2.24) |
| Seek + 10 Next | 1.36 µs (2.68) | 1.35 µs (2.83) |
| MultiGet(16) | 15.1 µs (32.5) | 14.4 µs (33.9) |

When every call completes synchronously, async costs between -5% and +17%. TryGet has the largest cost.

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

## Replication

`bench/RocksDb.Managed.Replication.Benchmarks` (see `bench/README.md`) measures the WAL-stream replication pipeline and
planned switchover. The 2026-11-02 sections were measured on the shared 4-vCPU cloud box (2.80 GHz CPU), .NET 10.0.14, Release, **with other agents building and
testing at the same time (load average 6-25 during these runs)**. Treat the numbers as indicative: repeated runs
of one configuration vary by 2x or more, and the latency tails mostly reflect CPU and fsync contention from other
processes. Primary and replica are on the local file system in one process, writes are not synced, keys are 16 B,
values 100 B; every measured run follows a full-size warm-up run (the JIT column is not ~0 for the gRPC rows: Kestrel /
gRPC code keeps tiering up during the run).

### Quiet machine (2026-11-03)

`bench/results/2026-11-03-replication.md`: 3 runs of `all`, one `switchover --handoffs 20 --writers 4` and 4 runs of
the multi-process `samples/SwitchoverTest`, on the quiet VM (no other agents; the 2.10 GHz CPU, see "Noise caveat"; the
load average rises from 1.1 to 3-5 during the runs because the benchmark itself keeps the 4 vCPUs busy), tree
`cfb30f9`. WAL stream, median of the 3 runs:

| transport | writers | batch | rate (ops/s) | ops | primary ops/s | replica ops/s | lag p50 ms | p99 | max | catch-up ms |
|---|---|---|---|---|---|---|---|---|---|---|
| in-memory | 1 | 1 | max | 200,000 | 307,256 | 306,043 | 3.50 | 13.15 | 15.2 | 3.1 |
| in-memory | 4 | 1 | max | 400,000 | 207,522 | 207,497 | 0.73 | 22.43 | 30.7 | 0.8 |
| in-memory | 4 | 100 | max | 1,000,000 | 504,730 | 401,603 | 237.37 | 514.05 | 520.1 | 508.8 |
| in-memory | 4 | 1 | 20,000 | 100,000 | 20,005 | 20,005 | 0.05 | 3.84 | 5.5 | 0.0 |
| gRPC loopback | 1 | 1 | max | 200,000 | 354,079 | 346,829 | 4.85 | 16.45 | 18.1 | 6.9 |
| gRPC loopback | 4 | 1 | max | 400,000 | 179,944 | 179,735 | 2.53 | 18.71 | 31.0 | 3.4 |
| gRPC loopback | 4 | 100 | max | 1,000,000 | 502,968 | 372,894 | 342.32 | 643.92 | 646.2 | 643.8 |
| gRPC loopback | 4 | 1 | 20,000 | 100,000 | 19,994 | 19,993 | 0.27 | 4.28 | 7.7 | 0.2 |

- Against the loaded 2026-11-02 numbers below, single-entry writes are 1.4-3x faster on the primary (in-memory 1
  writer 114k -> 307k ops/s, gRPC 129k -> 354k) and the replica keeps up with them (same ops/s, catch-up of a few
  milliseconds). The unsaturated 20k ops/s stream has a median lag of 0.05 ms in process and 0.27 ms over gRPC, p99
  ~4 ms (loaded: 0.38 / 0.62 ms, p99 2-5 ms).
- **With 100-entry batches at full speed the replica fell behind** in this run: the primary writes 480-560k ops/s
  (loaded: 179k in process, 312k over gRPC), the replica applied 75-80% of that (350-425k ops/s) and needed 0.5-0.9 s
  to catch up after the last write (loaded: 8-33 ms). Fixed since by parallel memtable inserts on the replica, see the
  next section.
- Switchover, in process (`all`: 10 handoffs x 3 runs, plus 20 handoffs): writes held p50 4.6-6.1 ms in memory and
  4.8-5.6 ms over gRPC, max 6.5-11.2 ms, client gap p50 5-6 ms; the first handoff of a process 13-16 ms in memory
  and 7-11 ms over gRPC. That is the quiet-machine mode of the bimodal hold described below (5-7 ms), with no 30-40 ms
  outliers.
- Multi-process `samples/SwitchoverTest` (2 nodes + coordinator, gRPC, rolling upgrade with 2 handoffs per run, 4
  runs): held 17.9-31.0 ms, median 27 ms over 8 handoffs, every run consistent (55-65k acknowledged writes on both
  nodes). Loaded: 35-133 ms, median ~85 ms. These are first promotes of their processes (cold paths), hence above the
  warm in-process hold.

### Replica apply with parallel memtable inserts (2026-11-03, load average 4-6)

**Why the replica fell behind.** The replica's apply loop was busy 95-98% of the run: it was CPU-bound, not waiting on
the stream. The primary's 4 writers commit in write groups whose memtable inserts run in parallel
(`allow_concurrent_memtable_write`), while the replica applies each merged chunk (the tailer merges up to 256 KiB, about
2,400 entries of these batches) as one write inserted by one thread. 56% of the apply profile is
`InlineSkipList.Insert` (random keys, cache-miss bound); decoding, the `SetContents` copy and the replica's WAL write
are a few percent. Applied alone (recorded chunks, fresh replica) the serial insert peaks at 456-493k entries/s, below
what the primary writes. Allocation is ~400 B per chunk (background flush), the same before and after.

**Fix.** `ReplicationConsumer.ApplyParallelism` (default: processor count, at most 4) applies a chunk with
`WriteOptions.MemtableInsertParallelism`: a batch of at least 256 entries is cut at record boundaries into up to that
many ranges of at least 128 entries, inserted concurrently by the applying thread and thread-pool threads. The replica
still writes one WAL record per chunk at the source's sequence numbers, chunks are applied one at a time in order, and
the chunk becomes visible only after every range is in the memtable (the sequence is published last), so ordering,
durability and atomicity are as before. Applied alone the chunks now go in at 701-774k entries/s with 2 threads and
0.95-1.14M with 4, about twice the primary's rate.

`bench/results/2026-11-03-replication-apply.md`, `stream --batch 100 --threads 4 --num 1000000`, 3 rounds alternating
`--apply_parallelism 1` (the previous serial insert) and `4`, medians. Another agent was building and benchmarking at
the same time (load average 4.4-6.2), hence the lower primary rates than above:

| transport | apply threads | primary ops/s | replica ops/s | replica / primary | lag p50 ms | p99 | max | catch-up ms | apply busy % |
|---|---|---|---|---|---|---|---|---|---|
| in-memory | 1 | 439,811 | 419,525 | 89-96% | 54.48 | 125.39 | 135.5 | 109.9 | 98 |
| in-memory | 4 | 397,937 | 388,788 | 98% | 7.09 | 53.90 | 55.1 | 50.0 | 93 |
| gRPC loopback | 1 | 470,600 | 395,475 | 77-89% | 207.16 | 416.93 | 421.5 | 403.7 | 96 |
| gRPC loopback | 4 | 387,346 | 383,926 | 98-99% | 8.00 | 63.78 | 66.7 | 42.5 | 89 |

- The replica now keeps up with the primary at full speed: lag p50 5-8 ms instead of 39-268 ms, max under 75 ms,
  catch-up after the last write 23-68 ms instead of 0.1-0.6 s (0.5-0.9 s on the quiet machine above). The replica rate
  is 98-99% of the primary's over the run; the remainder is the catch-up itself, which is included in the replica's
  time.
- The remaining lag is CPU contention of this single-process benchmark: primary writers, flushes, the tailer and the
  apply threads share 4 vCPUs (with another agent's load), so the apply stays busy ~90%. The apply side has about 2x
  the headroom when it has its own cores, as a replica on another machine has.
- Chunks are smaller (2,000-2,600 instead of 420-1,160 per million entries) because the tailer merges only what
  accumulated while the replica was busy; a replica that falls behind gets bigger chunks and so more parallelism.
- The other configurations are unchanged (`all` in the results file): single-entry writes and the 20k ops/s stream
  keep up with millisecond catch-up; their chunks are mostly too small to split.

### WAL stream: throughput and primary -> replica lag (2026-11-02, load average 15-25)

| transport | writers | batch | rate (ops/s) | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| in-memory | 1 | 1 | max | 200,000 | 114,201 | 114,157 | 4.54 | 16.20 | 28.90 | 33.5 | 0.7 |
| in-memory | 4 | 1 | max | 400,000 | 143,021 | 138,013 | 36.21 | 118.83 | 181.35 | 190.1 | 101.5 |
| in-memory | 4 | 100 | max | 1,000,000 | 179,135 | 178,872 | 192.45 | 481.44 | 562.34 | 572.2 | 8.2 |
| in-memory | 4 | 1 | 20,000 | 100,000 | 19,996 | 19,995 | 0.38 | 0.66 | 1.85 | 24.4 | 0.0 |
| gRPC loopback | 1 | 1 | max | 200,000 | 128,747 | 121,830 | 6.22 | 108.88 | 165.53 | 171.5 | 88.2 |
| gRPC loopback | 4 | 1 | max | 400,000 | 174,711 | 174,544 | 2.77 | 6.83 | 18.37 | 30.8 | 2.2 |
| gRPC loopback | 4 | 100 | max | 1,000,000 | 311,941 | 308,793 | 60.55 | 115.57 | 123.33 | 128.3 | 32.7 |
| gRPC loopback | 4 | 1 | 20,000 | 100,000 | 19,991 | 19,988 | 0.62 | 1.25 | 5.20 | 26.7 | 0.6 |

- The replica keeps up with the primary's write rate in every configuration (replica ops/s = primary ops/s within a
  few percent; catch-up after the last write is milliseconds).
- At an unsaturated 20k ops/s the median lag is 0.4 ms in process and 0.6 ms over gRPC, p99 2-5 ms.
- At full speed the lag is queueing: the stream buffers up to 16 chunks of up to 256 KiB, so with 100-entry batches
  the replica is a few hundred thousand entries (100-500 ms) behind while writers saturate the box.

### Switchover: writes held per handoff

`switchover --handoffs 20 --writers 4`, before (`c2126e3`) and after the hold reductions in
`src/RocksDb.Managed.Replication/README.md` ("Switchover cost"), runs interleaved. Columns: first handoff of the
process held / client gap, then p50 and max over the other 19.

| transport | before: first | before: p50 / max | after: first | after: p50 / max |
|---|---|---|---|---|
| in-memory, round 1 (load 15/13) | 18.4 | 6.5 / 9.9 | 26.1 | 6.1 / 14.2 |
| gRPC, round 1 | 21.6 | 6.0 / 10.6 | 13.8 | 19.2 / 36.6 |
| in-memory, round 2 (load 12/15) | 21.7 | 37.6 / 97.9 | 32.2 | 35.2 / 76.0 |
| gRPC, round 2 | 25.8 | 28.0 / 81.7 | 76.0 | 31.7 / 91.6 |
| in-memory, round 3 (load 17/17) | 43.2 | 34.9 / 114.1 | 229.6 | 37.6 / 364.0 |
| gRPC, round 3 | 66.9 | 28.0 / 59.0 | 25.3 | 5.3 / 12.6 |

On this machine the warm hold is bimodal - about 5-7 ms whenever the box is quiet enough, 30-40 ms when fsync and
scheduling latency are high - and the before/after difference is inside that noise. The breakdown (timestamps on both
nodes) shows where the time goes: draining the gate ~1 ms, replica catch-up and detach 1-5 ms, the rest is the promote
round trip and the fsync'd role-state writes. The changes remove one-time costs of the first promote of a process
over gRPC (15-30 ms per side, visible in the multi-process `samples/SwitchoverTest`: before 40-230 ms, median ~99 ms,
after 35-133 ms, median ~85 ms over 8 handoffs each at load 7-20) and three fsyncs from the hold. The 10-50 ms in
rocksdb-sharp's notes were measured on a quieter machine with one fsync per state write (no directory fsync).
