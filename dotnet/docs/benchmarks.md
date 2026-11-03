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

**Noise caveat.** All committed results come from a 4-vCPU cloud VM (KVM, 15 GiB): "Intel Xeon @ 2.80 GHz" up to
`2026-11-02-*`, "Intel Xeon Processor @ 2.10GHz" (a newer core, model 207) from `2026-11-03-*` on. Absolute times
and even ratios vs C differ between the two CPUs (see "Latest results"). Even on the quiet machine (no other agents,
load average 0.5-1.7 between runs) one measurement of one cell varies by about 10% (median coefficient of variation
over 5 repetitions: 8.3-10.7% depending on the engine and run), and 10% of the cells by more than 20%. Single runs before `2026-11-02-final-warm-r5.md` were taken while other agents ran
builds and tests (load average 4-14) and are noisier. Quote medians of repeated runs (`REPEATS`, see
[bench/README.md](../bench/README.md)) and their confidence intervals, not single cells.

## Latest results

The source is `bench/results/2026-11-03-final-warm-r5.md` (raw rows: `.csv`, LSM rows: `.lsm.csv`): the reduced
matrix (100 B and 1 KiB values, 1 and 4 threads, no compression and LZ4) plus the compaction-heavy configuration, on
c-native, rocksdb-sharp, managed and managed-async, **5 repetitions** each (fresh DB, warm-up on, engines interleaved
in every repetition), Release build of `cfb30f9`, .NET 10.0.14, librocksdb 11.12 (NuGet `RocksDB` 11.12.0.7385).
Sizes as in the previous final run: num=300k (300k reads) for 100-byte values, 100k (100k reads) for 1 KiB values,
500k for the compaction-heavy configuration. The machine was quiet (no other agents; each repetition started at a
1-minute load average <= 1.5; 80 min in total).

**The VM's CPU changed** between the previous final run (`2026-11-02-final-warm-r5.md`, "Intel Xeon @ 2.80GHz") and
this one ("Intel Xeon Processor @ 2.10GHz", family 6 model 207, AVX-512 + AMX). On the new CPU the native engines are
~40% faster in absolute time and the managed engines ~30%, so the ratios vs C moved against managed partly for
reasons that are not in the code. An interleaved A/B on the new CPU (managed at `0f866b8`, the tree of the previous
run, against `cfb30f9`, with c-native; 3 repetitions of 4 configurations, `2026-11-03-ab-0f866b8-vs-cfb30f9.csv`) puts
the old tree at 0.71x of C and the current one at 0.76x: of the 18% shift from 0.66x to 0.78x below, about 7% is the
CPU, about 6% is code (flush and 1 KiB seeks, see the takeaways; 4-thread writes and reads improved), and the rest is
the difference between the 4 A/B configurations and the full matrix, and noise. Compare ratios within one file, not
across the two.

Geometric mean over the benchmarks of each configuration of the **median** µs/op ratio vs c-native (lower is better;
below 1.00x is faster than C), with the 95% bootstrap confidence interval:

| configuration | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| 100 B, 1 thread, no compression | 1.08x [1.05-1.14] | 0.72x [0.70-0.76] | 0.73x [0.70-0.76] |
| 100 B, 4 threads, no compression | 1.05x [0.99-1.11] | 0.77x [0.73-0.82] | 0.75x [0.71-0.80] |
| 100 B, 1 thread, LZ4 | 1.07x [1.01-1.12] | 0.71x [0.67-0.75] | 0.74x [0.70-0.78] |
| 100 B, 4 threads, LZ4 | 1.04x [0.99-1.12] | 0.76x [0.73-0.81] | 0.74x [0.71-0.80] |
| 1 KiB, 1 thread, no compression | 1.05x [0.98-1.10] | 0.80x [0.76-0.85] | 0.81x [0.77-0.86] |
| 1 KiB, 4 threads, no compression | 1.04x [0.96-1.13] | 0.84x [0.77-0.91] | 0.85x [0.78-0.92] |
| 1 KiB, 1 thread, LZ4 | 1.03x [0.97-1.08] | 0.76x [0.72-0.81] | 0.76x [0.72-0.80] |
| 1 KiB, 4 threads, LZ4 | 1.02x [0.96-1.07] | 0.89x [0.82-0.92] | 0.84x [0.79-0.87] |
| **all 8 matrix configurations (112 benchmarks)** | **1.05x [1.02-1.07]** | **0.78x [0.76-0.80]** | **0.78x [0.76-0.79]** |
| compaction-heavy (500k x 100 B, 1 thread; 4 benchmarks) | 1.05x [0.94-1.15] | 0.62x [0.53-0.66] | 0.65x [0.58-0.73] |

Per benchmark, geometric mean over the 8 matrix configurations (median ratios, 95% bootstrap CI):

| benchmark | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| fillseq | 1.03x [0.97-1.08] | 0.66x [0.63-0.69] | 0.67x [0.64-0.73] |
| fillrandom | 1.02x [0.98-1.08] | 0.74x [0.71-0.79] | 0.79x [0.74-0.83] |
| overwrite | 1.02x [0.96-1.07] | 0.86x [0.81-0.90] | 0.86x [0.81-0.90] |
| flush | 0.97x [0.91-1.05] | 1.43x [1.34-1.58] | 1.44x [1.35-1.56] |
| readrandom | 1.02x [0.97-1.06] | 0.67x [0.63-0.71] | 0.71x [0.67-0.75] |
| readmissing | 1.12x [1.00-1.24] | 0.62x [0.56-0.69] | 0.72x [0.67-0.81] |
| seekrandom | 1.07x [1.04-1.12] | 0.72x [0.70-0.75] | 0.63x [0.61-0.65] |
| readseq | 1.15x [1.07-1.24] | 0.67x [0.61-0.73] | 0.67x [0.62-0.70] |
| readreverse | 1.16x [1.09-1.24] | 0.64x [0.61-0.70] | 0.64x [0.61-0.69] |
| multireadrandom | 1.03x [0.99-1.09] | 0.73x [0.70-0.77] | 0.73x [0.69-0.76] |
| readwhilewriting | 1.01x [0.94-1.06] | 0.79x [0.74-0.83] | 0.79x [0.72-0.83] |
| compact | 1.01x [0.89-1.18] | 0.88x [0.78-1.04] | 0.86x [0.75-1.00] |
| deleterandom | 1.02x [0.97-1.09] | 0.70x [0.68-0.78] | 0.74x [0.70-0.79] |
| fillsync | 1.06x [0.89-1.20] | 1.11x [0.92-1.22] | 0.87x [0.76-0.98] |

Takeaways:

- **Overall** the managed engine takes about four fifths of the C API's time on the same workload (0.78x, CI
  0.76-0.80; 0.74x without the `flush` rows), sync and async alike, and rocksdb-sharp costs about 5% over C (1.05x, CI
  1.02-1.07). Every configuration's CI for managed and managed-async lies below 1.0.
- **4-thread writes are no longer a slow spot**: sync `fillrandom` 0.81-0.93x and `overwrite` 0.86-1.06x (previous
  run 1.04-1.31x / 1.28-1.81x), async 0.74-0.95x / 0.86-1.06x, from the AwaitState write wait and parallel memtable
  inserts. Single-threaded writes stay well below C (`fillseq` 0.52-0.58x, `fillrandom` 0.56-0.71x, `overwrite`
  0.70-0.92x).
- **4-thread `readmissing` is fixed too**: 0.59-0.61x with 100 B values (previous run 1.42-1.68x; async 0.69-0.77x, was 1.93-3.00x), after the
  per-processor super version cache and pinned table readers. With 1 KiB values and LZ4 it is 1.08x (async 1.20x) of
  a 0.15 µs native operation.
- **Point reads, seeks and scans** are faster than C: `readrandom` 0.60-0.77x, `seekrandom` 0.52-0.75x except 1 KiB /
  1 thread / no compression (1.21x, see below), `readseq` 0.60-0.83x, `readreverse` 0.54-0.80x, `multireadrandom`
  0.58-0.85x. Async costs 0-16% over sync on reads (per-benchmark geometric means) and is faster for seeks (0.63x vs
  0.72x).
- **Slow spots (managed > 1.1x of C)**, investigated, not fixed:
  - `flush` 1.17-2.06x (1.58-2.06x with 1 KiB values; async the same). A flush now returns only after the obsolete-file
    purge, which deletes the flushed WAL (64+ MiB with 1 KiB values): the purge moved before the job's completion
    accounting in `01ad63b` (so close cannot race it), and `FlushAsync`'s wait is released by that accounting. Upstream
    signals the flush waiter when the memtable is installed, before the purge. Evidence: 1 KiB fillseq + fillrandom +
    flush, old vs new tree, 3 runs each: 73-93 vs 111-130 ms with the WAL, 64-102 vs 67-89 ms with
    `--disable_wal true`. The flush itself did not get slower.
  - `seekrandom` with 1 KiB values, 1 thread, no compression: 1.21x (old tree on the same CPU 0.70x). Seek + 10 Next
    reads 3-4 consecutive data blocks, which triggers the user-iterator auto readahead (`BlockPrefetcher` port,
    `e4d4311`) on nearly every seek: an extra 8 KiB+ async read into a prefetch buffer per seek, mostly wasted.
    Upstream issues `Prefetch` (the OS `readahead` hint) first and falls back to a prefetch buffer only where that is
    unsupported. A probe with the auto readahead disabled: 9.1-11.0 µs/op against 16.2-17.1 (old tree 9.8-11.3). The
    same buffers make sync `readseq` allocate 6-55 B/op (previously 0.2-2.9; async scans 0.2-2.0).
  - `fillsync` 1.13-1.30x in 4 of 8 configurations (sync API; async 0.67-1.03x): one fsync per put, device-bound
    and the noisiest row (as in the previous run, 0.87-1.25x).
  - `compact` with 1 KiB values, 4 threads, LZ4: 1.10x (previous run 1.02x), within its CI.
- **Flush and full compaction**: one full compaction is 0.75-0.87x without compression and 0.84-1.10x with LZ4.
  Compactions allocate more than in the previous run (A/B: 7.4 -> 31 MB per full compaction of the 100 B DB;
  compaction-heavy fills 5-10 -> 80-140 B/op, which counts the background compactions); not profiled, the compaction
  input readahead buffers (`09c0552`) are the likely source.
- **Compaction-heavy configuration**: managed 0.69x / 0.74x on `fillrandom` / `overwrite` against background flushes
  and compactions, 0.64x `readrandom` and 0.47x `seekrandom` on the multi-level tree. Write amplification: c-native and
  rocksdb-sharp 2.96 in all 5 repetitions (LSM shape `3 0 0 0 0 4 15`, 5/5), managed 3.15 (3.05-3.15), managed-async
  3.05 (2.96-3.15), shapes `0-3 0 0 0 0 3-5 15-16`.
- **rocksdb-sharp** is at parity with C for most operations; its cost shows on scans (`readseq` 1.15x,
  `readreverse` 1.16x) and `readmissing` (1.12x).
- **JIT during measurement** (warm-up on): median 1 method per measured benchmark (managed and managed-async); the
  compaction-heavy fills still compile up to 585 methods while running. rocksdb-sharp: 0-10.
- **Noise**: 94 of 117 benchmark rows have at least one engine with CV > 10% (median CV 8.4% c-native, 8.3%
  rocksdb-sharp, 10.2% managed, 9.2% managed-async; 18-23% at the 90th percentile). No process crashed.

**Extra rows** (`bench/results/2026-11-03-final-extras-r3.md`, 3 repetitions, same session; µs/op medians, ratio vs C):

| workload | c-native | managed | managed-async |
|---|---:|---:|---:|
| 4 threads, 300k x 100 B, `fillrandom` / `overwrite`, default write path | 5.02 / 4.97 | 4.30 / 4.83 (0.86x / 0.97x) | 4.59 / 4.58 (0.91x / 0.92x) |
| same, `--enable_pipelined_write true` | 3.74 / 4.37 | 3.41 / 3.99 (0.91x / 0.91x) | 4.68 / 4.83 (1.25x / 1.11x) |
| same, `--unordered_write true` | 3.31 / 3.17 | 2.33 / 2.59 (0.70x / 0.82x) | 3.10 / 2.89 (0.94x / 0.91x) |
| full `compact` of 1M x 100 B in L0, `max_subcompactions` 1 (ms) | 697 | 586 (0.84x) | |
| same, `max_subcompactions` 4 (ms) | 330 | 484 (1.47x) | |
| 100k x 1 KiB, `readreverse` | 2.96 | 0.80 (0.27x) | 0.99 (0.34x) |
| 100k x 1 KiB, `seekrandom --reverse_iterator` (SeekForPrev + 10 Prev) | 16.57 | 11.41 (0.69x) | 12.26 (0.74x) |
| compaction-heavy tree, 500k x 100 B, 4 threads, `readrandom` (1M reads) | 1.76 | 1.02 (0.58x) | |

- Pipelined writes gain 15-20% on both engines with 4 sync writers; async writers lose with pipelining (1.11-1.25x
  of C), because each stage hand-off is a thread-pool continuation (see perf.md "Pipelined writes"). `unordered_write`
  gains 35-50% on managed, 30-35% on native.
- **Subcompactions scale less than native**: 4 subcompactions take C from 697 to 330 ms (2.1x), managed from 586 to
  484 ms (1.2x), and the managed compaction allocates 294 MB instead of 74 MB. Not profiled (candidates: per-
  subcompaction buffers, the input split, or the merge of the outputs).
- **Read sampling** (read-triggered compaction bookkeeping, both engines): one thread-static RNG step per Get / seek
  and a counter increment on 1 in 1024 file reads. `readrandom` did not get slower beyond the noise in the A/B
  (new / old tree 0.75-1.09x in the matrix configurations, 1.22x in the compaction-heavy one, 3 repetitions, CVs ~10%),
  and the read-heavy multi-level run above is 0.58x of C.

The previous final run, `2026-11-02-final-warm-r5.md` (same matrix and sizes, tree `0f866b8`, the 2.80 GHz CPU), had
managed 0.66x [0.64-0.67], managed-async 0.68x [0.67-0.70] and rocksdb-sharp 1.03x [1.01-1.06] over the 8 matrix
configurations, with 4-thread sync random writes (1.04-1.81x) and 4-thread `readmissing` with 100 B values
(1.42-3.00x) as its slow spots; [perf.md](perf.md) ("Final three-way results" and "Final run on a new VM CPU")
compares the two.

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

Latest head-to-head numbers (`bench/results/2026-11-03-final-bdn.md`: `--anyCategories DbHeadToHead DbAsync`, the
default job, quiet machine, the 2.10 GHz CPU, .NET 10.0.14; mean per operation, single thread). The last column is
the ratio of the previous final run (`2026-11-02-final-bdn.md`, 2.80 GHz CPU):

| operation | rocksdb-sharp | managed | managed / rocksdb-sharp | 2026-11-02 |
|---|---:|---:|---:|---:|
| Put (memtable, WAL) | 4.12 µs | 2.98 µs | 0.72x | 0.57x |
| WriteBatch of 100 puts | 120.2 µs | 67.8 µs | 0.56x | 0.60x |
| Get from the memtable | 1716 ns | 765 ns | 0.45x | 0.49x |
| Get from a cached SST block | 2052 ns | 1133 ns | 0.55x | 0.35x |
| Get of a missing key (Bloom filter) | 416 ns | 125 ns | 0.30x | 0.28x |
| Seek + 10 Next | 3.97 µs | 0.98 µs | 0.25x | 0.26x |
| Seek + 100 Next | 28.19 µs | 5.86 µs | 0.21x | 0.21x |
| MultiGet of 16 keys | 27.35 µs | 17.05 µs | 0.62x | 0.52x |

Both engines are faster on the newer CPU, the native binding more so for cached-block Gets (3782 -> 2052 ns against
1334 -> 1133 ns) and Put (6.68 -> 4.12 µs against 3.82 -> 2.98 µs). No managed hot path allocates (`Allocated` `-`, or
1 B per Put and 26 B per 100-entry batch, the amortized memtable arena / pool growth). Sync against async (`DbAsync`,
blocks in the block cache / only in the OS page cache): Put 2.75 / 2.87 µs, TryGet 1.04 / 1.14 µs (page cache 2.07 /
2.30 µs), Seek + 10 Next 1.45 / 1.44 µs (page cache 2.64 / 2.85 µs), MultiGet(16) 15.2 / 15.3 µs (page cache 33.6 /
35.7 µs): async costs 0-11% when every call completes synchronously.

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
- **With 100-entry batches at full speed the replica now falls behind**: the primary writes 480-560k ops/s (loaded:
  179k in process, 312k over gRPC), the replica applies 75-80% of that (350-425k ops/s), and it needs 0.5-0.9 s to
  catch up after the last write (loaded: 8-33 ms). The replica applies the stream's batches one by one through the
  normal write path, so a primary whose writers group-commit 4 x 100 entries outruns it; this is throughput, not a
  stall (the lag stays bounded by the stream's buffer of 16 x 256 KiB chunks).
- Switchover, in process (`all`: 10 handoffs x 3 runs, plus 20 handoffs): writes held p50 4.6-6.1 ms in memory and
  4.8-5.6 ms over gRPC, max 6.5-11.2 ms, client gap p50 5-6 ms; the first handoff of a process 13-16 ms in memory
  and 7-11 ms over gRPC. That is the quiet-machine mode of the bimodal hold described below (5-7 ms), with no 30-40 ms
  outliers.
- Multi-process `samples/SwitchoverTest` (2 nodes + coordinator, gRPC, rolling upgrade with 2 handoffs per run, 4
  runs): held 17.9-31.0 ms, median 27 ms over 8 handoffs, every run consistent (55-65k acknowledged writes on both
  nodes). Loaded: 35-133 ms, median ~85 ms. These are first promotes of their processes (cold paths), hence above the
  warm in-process hold.

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
