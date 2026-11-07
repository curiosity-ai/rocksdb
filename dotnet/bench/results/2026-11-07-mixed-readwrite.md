# Mixed reads / writes: readrandomwriterandom, 0-100% writes, C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz, 4 logical cores (VM), 15 GiB RAM
- OS: Linux 6.18.44-fc-v310
- .NET: .NET 10.0.14 (benchmark project net10.0, Release; SDK 11.0.100-rc.3)
- Native: librocksdb.so 11.12 from the RocksDB NuGet package 11.12.0.7385 (c-native and rocksdb-sharp)
- Tree: 886811b (branch claude/rocksdb-csharp-dotnet-port-w9bqfg + the readrandomwriterandom benchmark)
- Date: 2026-11-07, 06:59 - 07:33 UTC; 1-minute load average before each cell: min 0.87, median 1.27, max 1.46 (30 cells)

Mixed read/write sweep: `readrandomwriterandom` (db_bench ReadRandomWriteRandom) at 0%, 25%, 50%, 75% and 100% writes
(`--readwritepercent` 100, 75, 50, 25, 0), on c-native, rocksdb-sharp, managed and managed-async.

## Configuration

- Each cell is one `compare` run of `--benchmarks fillrandom,flush,readrandomwriterandom,stats --num 1000000
  --value_size 100 --compression none --readwritepercent P --threads T`, with `--reads 1000000` for 1 thread and
  `--reads 2000000` for 4 threads (total `readrandomwriterandom` ops, split across the threads). All other flags
  are the defaults: 16-byte keys, values 50% compressible, 8 MiB LRU block cache, 10-bit bloom filter, 64 MiB write
  buffer, WAL on, no sync, warm-up on.
- Pre-fill: every engine process runs the same `fillrandom` of 1M random keys (same seed, so the same keys and values
  in every engine; about 63% of the key range exists) followed by a `flush`. The fill overflows the 64 MiB memtable
  once, so the DB starts with 2 L0 files and an empty memtable. The measurement then runs on that DB. Its Puts go to
  the memtable and trigger flushes in the write-heavy cells.
- Each engine runs in its own process, on its own fresh DB, after the standard warm-up (the same benchmark list on a
  separate warm-up DB, see bench/README.md "Warm-up").
- 3 repetitions. Each repetition runs every (threads, write %) cell, and every cell runs the four engines in the
  order c-native, rocksdb-sharp, managed, managed-async. Before each cell the script waited until the 1-minute load
  average was at most 1.5 (the longest wait was 50 s). The tables show medians over the 3 repetitions.
- reads/s and writes/s = ops/s x the read / write fraction. The fraction is exact: every thread alternates
  blocks of 100 ops, `readwritepercent` Gets then the rest Puts.

## Observations

- Total throughput relative to c-native: rocksdb-sharp is at 0.91-1.02x. Managed is at 1.43-1.58x with 1 thread
  and 1.25-1.65x with 4 threads. Managed-async is at 1.35-1.47x and 1.20-1.44x. Per-cell µs/op ratios with CVs are
  in the per-configuration tables below; the geometric mean of the µs/op ratio over the 5 mixes is about 0.66x for
  managed with 1 thread and 0.73x with 4.
- The managed lead is largest for pure reads (1.58x / 1.65x). With 4 threads it shrinks as the write share grows,
  down to 1.25-1.31x at 50-100% writes. With 1 thread it stays between 1.43x and 1.58x.
- Reads scale with threads and writes do not. From 1 to 4 threads, pure-read throughput grows 3.0x on C
  (311k -> 937k ops/s) and 3.1x on managed (493k -> 1.55M). Pure-write throughput grows 1.04x on C (279k -> 289k)
  and falls on managed (422k -> 369k). All writers go through the single write group / WAL.
- With 1 thread, every engine is slower on the mixes than on either pure workload. C: 311k ops/s at 0% writes,
  279k at 100%, 215-240k for 25-75% writes. The same holds for the other engines.
- Managed allocations are 4-21 B/op. That is the steady-state growth described in bench/README.md
  "Allocations" (block cache buffers on the first misses, memtable arenas when the memtable switches), not a
  per-op allocation.
- JIT during the measurement: 0-25 methods in the read-heavy cells, except managed-async at 4 threads and 25% writes
  (214). In the write-heavy cells the managed engines
  compile more while measured (max over the repetitions): up to 477 (sync) and 633 (async) methods at 4 threads,
  89-232 at 1 thread (75-100% writes). These cells flush memtables during the measurement, and with 4 writer threads some
  flush / write-stall paths are hotter than in the warm-up. The cells are still stable (CV <= 9%).
- Variance: one `readrandomwriterandom` cell has a CV above 10%: rocksdb-sharp at 4 threads, 50% writes, with
  2.33 / 2.33 / 3.09 µs/op. The median is unaffected. The other noisy rows are the single-shot `flush` after the
  fill.
- The LSM / write-amp section below pools all write percentages of one thread count, because the `.lsm.csv` rows do
  not carry the percentage. The LSM shapes are identical across engines in most repetitions.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 15 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| num=1000000, value=100B, threads=1, compression=none | 1.04x [1.00-1.08] (7) | 0.66x [0.63-0.68] (7) | 0.69x [0.67-0.72] (7) |
| num=1000000, value=100B, threads=4, compression=none | 1.06x [1.01-1.12] (7) | 0.74x [0.71-0.77] (7) | 0.76x [0.74-0.78] (7) |
| **all matrix configurations** | **1.05x [1.02-1.09] (14)** | **0.70x [0.68-0.72] (14)** | **0.73x [0.71-0.75] (14)** |

### Per benchmark (geometric mean over the 2 matrix configurations of the median ratio vs c-native)

| benchmark | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| fillrandom | 1.03x [1.00-1.07] | 0.71x [0.69-0.75] | 0.76x [0.74-0.79] |
| flush | 1.12x [0.96-1.26] | 0.70x [0.60-0.76] | 0.61x [0.56-0.65] |
| readrandomwriterandom w0% | 1.02x [0.98-1.08] | 0.62x [0.58-0.67] | 0.70x [0.65-0.73] |
| readrandomwriterandom w25% | 1.00x [0.95-1.08] | 0.69x [0.65-0.74] | 0.74x [0.69-0.81] |
| readrandomwriterandom w50% | 1.09x [1.02-1.27] | 0.72x [0.69-0.75] | 0.77x [0.73-0.83] |
| readrandomwriterandom w75% | 1.05x [1.01-1.14] | 0.75x [0.69-0.80] | 0.79x [0.76-0.85] |
| readrandomwriterandom w100% | 1.04x [0.92-1.05] | 0.72x [0.67-0.76] | 0.74x [0.69-0.78] |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| rocksdb-sharp | 90 | 94 | 0 | 3 (fillrandom, num=1000000, value=100B, threads=1, compression=none, rep 1) |
| managed | 90 | 3268 | 18 | 477 (readrandomwriterandom w75%, num=1000000, value=100B, threads=4, compression=none, rep 2) |
| managed-async | 90 | 4901 | 22 | 633 (readrandomwriterandom w50%, num=1000000, value=100B, threads=4, compression=none, rep 2) |

## Mixed reads / writes (`readrandomwriterandom`)

Throughput, higher is better (medians over the repetitions): reads/s and writes/s = ops/s x the fraction of the ops that were reads / writes; vs C = ops/s of the engine / ops/s of c-native (< 1 = slower). alloc = managed bytes allocated per op.

### num=1000000, value=100B, threads=1, compression=none

| writes | c-native reads/s | c-native writes/s | c-native ops/s | rocksdb-sharp reads/s | rocksdb-sharp writes/s | rocksdb-sharp ops/s | managed reads/s | managed writes/s | managed ops/s | managed-async reads/s | managed-async writes/s | managed-async ops/s | rocksdb-sharp vs C | managed vs C | managed-async vs C | managed alloc B/op | managed-async alloc B/op |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0% | 311.0k | 0 | 311.0k | 309.4k | 0 | 309.4k | 492.5k | 0 | 492.5k | 441.8k | 0 | 441.8k | 0.99x | 1.58x | 1.42x | 8.7 | 8.7 |
| 25% | 161.3k | 53.8k | 215.1k | 164.4k | 54.8k | 219.1k | 246.8k | 82.3k | 329.0k | 227.5k | 75.8k | 303.4k | 1.02x | 1.53x | 1.41x | 8.7 | 8.6 |
| 50% | 114.4k | 114.4k | 228.9k | 104.1k | 104.1k | 208.1k | 167.7k | 167.7k | 335.4k | 157.6k | 157.6k | 315.1k | 0.91x | 1.47x | 1.38x | 9.5 | 9.5 |
| 75% | 59.9k | 179.7k | 239.6k | 56.3k | 168.9k | 225.2k | 85.8k | 257.4k | 343.1k | 80.9k | 242.6k | 323.5k | 0.94x | 1.43x | 1.35x | 21.3 | 21.3 |
| 100% | 0 | 278.7k | 278.7k | 0 | 259.4k | 259.4k | 0 | 421.7k | 421.7k | 0 | 410.1k | 410.1k | 0.93x | 1.51x | 1.47x | 12.7 | 12.7 |

### num=1000000, value=100B, threads=4, compression=none

| writes | c-native reads/s | c-native writes/s | c-native ops/s | rocksdb-sharp reads/s | rocksdb-sharp writes/s | rocksdb-sharp ops/s | managed reads/s | managed writes/s | managed ops/s | managed-async reads/s | managed-async writes/s | managed-async ops/s | rocksdb-sharp vs C | managed vs C | managed-async vs C | managed alloc B/op | managed-async alloc B/op |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0% | 936.9k | 0 | 936.9k | 903.4k | 0 | 903.4k | 1.55M | 0 | 1.55M | 1.34M | 0 | 1.34M | 0.96x | 1.65x | 1.44x | 4.3 | 4.3 |
| 25% | 485.1k | 161.7k | 646.8k | 474.0k | 158.0k | 632.0k | 673.9k | 224.6k | 898.5k | 631.6k | 210.5k | 842.1k | 0.98x | 1.39x | 1.30x | 4.4 | 4.3 |
| 50% | 231.5k | 231.5k | 463.0k | 214.6k | 214.6k | 429.3k | 302.6k | 302.6k | 605.2k | 281.8k | 281.8k | 563.7k | 0.93x | 1.31x | 1.22x | 10.8 | 10.8 |
| 75% | 89.5k | 268.4k | 357.8k | 86.3k | 258.9k | 345.2k | 111.7k | 335.2k | 446.9k | 107.3k | 322.0k | 429.4k | 0.96x | 1.25x | 1.20x | 10.9 | 10.8 |
| 100% | 0 | 288.6k | 288.6k | 0 | 289.1k | 289.1k | 0 | 368.8k | 368.8k | 0 | 360.0k | 360.0k | 1.00x | 1.28x | 1.25x | 6.5 | 7.0 |

## num=1000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.59 (3.31-3.94, 5%) | 3.64 (3.51-4.09, 5%) | 2.30 (2.22-2.52, 4%) | 2.50 (2.31-2.65, 4%) | 1.01x | 0.64x | 0.70x | 8.21 | 8.91 | 5.58 | 5.78 | 58.7 | 58.7 | 3 | 66 | 47 |
| flush ! | 49.6 ms (44.3 ms-69.7 ms, 13% !) | 52.4 ms (44.5 ms-67.4 ms, 14% !) | 33.1 ms (26.7 ms-50.7 ms, 20% !) | 30.1 ms (26.7 ms-39.6 ms, 11% !) | 1.05x | 0.67x | 0.61x | - | - | - | - | 27696.0 | 27944.0 | 2 | 20 | 22 |
| readrandomwriterandom w0% | 3.22 (3.18-3.28, 2%) | 3.23 (3.19-3.48, 5%) | 2.03 (1.91-2.07, 4%) | 2.26 (2.04-2.34, 7%) | 1.01x | 0.63x | 0.70x | 8.97 | 8.98 | 5.02 | 5.69 | 8.7 | 8.7 | 0 | 2 | 4 |
| readrandomwriterandom w25% | 4.65 (4.23-4.65, 5%) | 4.56 (4.44-4.77, 4%) | 3.04 (2.91-3.05, 3%) | 3.30 (3.06-3.45, 6%) | 0.98x | 0.65x | 0.71x | 12.81 | 14.03 | 7.41 | 8.15 | 8.7 | 8.6 | 0 | 2 | 9 |
| readrandomwriterandom w50% | 4.37 (4.33-4.64, 4%) | 4.80 (4.52-4.87, 4%) | 2.98 (2.95-3.09, 2%) | 3.17 (3.02-3.44, 7%) | 1.10x | 0.68x | 0.73x | 13.12 | 15.52 | 8.07 | 8.32 | 9.5 | 9.5 | 0 | 19 | 25 |
| readrandomwriterandom w75% | 4.17 (3.87-4.37, 6%) | 4.44 (4.35-4.49, 2%) | 2.91 (2.57-3.09, 9%) | 3.09 (3.00-3.35, 6%) | 1.06x | 0.70x | 0.74x | 12.43 | 14.08 | 8.53 | 8.78 | 21.3 | 21.3 | 0 | 105 | 89 |
| readrandomwriterandom w100% | 3.59 (3.58-3.94, 6%) | 3.86 (3.39-3.89, 7%) | 2.37 (2.24-2.46, 5%) | 2.44 (2.36-2.60, 5%) | 1.07x | 0.66x | 0.68x | 8.76 | 9.68 | 5.53 | 5.64 | 12.7 | 12.7 | 0 | 150 | 232 |

## num=1000000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.34 (3.09-3.47, 4%) | 3.46 (3.24-3.89, 5%) | 2.63 (2.53-2.94, 4%) | 2.80 (2.63-2.98, 3%) | 1.04x | 0.79x | 0.84x | 35.41 | 37.92 | 29.00 | 29.21 | 59.8 | 63.9 | 3 | 64 | 67 |
| flush ! | 24.2 ms (21.3 ms-33.8 ms, 15% !) | 28.8 ms (22.3 ms-52.3 ms, 30% !) | 17.6 ms (14.2 ms-27.9 ms, 23% !) | 14.8 ms (13.7 ms-19.9 ms, 12% !) | 1.19x | 0.73x | 0.61x | - | - | - | - | 60472.0 | 60712.0 | 2 | 15 | 15 |
| readrandomwriterandom w0% | 1.07 (1.02-1.09, 4%) | 1.11 (1.05-1.12, 3%) | 0.65 (0.62-0.72, 8%) | 0.74 (0.72-0.77, 3%) | 1.04x | 0.61x | 0.70x | 18.61 | 20.07 | 6.66 | 7.64 | 4.3 | 4.3 | 0 | 7 | 19 |
| readrandomwriterandom w25% | 1.55 (1.54-1.65, 4%) | 1.58 (1.53-1.67, 4%) | 1.11 (1.08-1.17, 4%) | 1.19 (1.18-1.31, 6%) | 1.02x | 0.72x | 0.77x | 22.31 | 21.64 | 13.98 | 17.15 | 4.4 | 4.3 | 0 | 9 | 214 |
| readrandomwriterandom w50% ! | 2.16 (2.12-2.19, 2%) | 2.33 (2.33-3.09, 17% !) | 1.65 (1.61-1.71, 3%) | 1.77 (1.75-1.87, 3%) | 1.08x | 0.77x | 0.82x | 30.66 | 33.36 | 19.75 | 24.83 | 10.8 | 10.8 | 0 | 127 | 633 |
| readrandomwriterandom w75% | 2.79 (2.79-2.80, 0%) | 2.90 (2.83-3.14, 6%) | 2.24 (2.15-2.37, 5%) | 2.33 (2.27-2.33, 1%) | 1.04x | 0.80x | 0.83x | 35.36 | 35.27 | 25.99 | 28.74 | 10.9 | 10.8 | 0 | 477 | 399 |
| readrandomwriterandom w100% | 3.46 (3.42-3.52, 1%) | 3.46 (3.35-3.51, 2%) | 2.71 (2.69-2.91, 4%) | 2.78 (2.75-2.93, 3%) | 1.00x | 0.78x | 0.80x | 38.45 | 38.38 | 28.00 | 28.28 | 6.5 | 7.0 | 0 | 243 | 513 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| num=1000000, value=100B, threads=1, compression=none | flush | c-native | 12.5% | 49.6 ms | 44.3 ms-69.7 ms | 15 |
| num=1000000, value=100B, threads=1, compression=none | flush | rocksdb-sharp | 14.3% | 52.4 ms | 44.5 ms-67.4 ms | 15 |
| num=1000000, value=100B, threads=1, compression=none | flush | managed | 19.7% | 33.1 ms | 26.7 ms-50.7 ms | 15 |
| num=1000000, value=100B, threads=1, compression=none | flush | managed-async | 10.8% | 30.1 ms | 26.7 ms-39.6 ms | 15 |
| num=1000000, value=100B, threads=4, compression=none | flush | c-native | 14.8% | 24.2 ms | 21.3 ms-33.8 ms | 15 |
| num=1000000, value=100B, threads=4, compression=none | flush | rocksdb-sharp | 29.9% | 28.8 ms | 22.3 ms-52.3 ms | 15 |
| num=1000000, value=100B, threads=4, compression=none | flush | managed | 23.1% | 17.6 ms | 14.2 ms-27.9 ms | 15 |
| num=1000000, value=100B, threads=4, compression=none | flush | managed-async | 12.4% | 14.8 ms | 13.7 ms-19.9 ms | 15 |
| num=1000000, value=100B, threads=4, compression=none | readrandomwriterandom w50% | rocksdb-sharp | 17.1% | 2.33 | 2.33-3.09 | 3 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

Over the repetitions: write-amp median (min-max), and the most frequent LSM shape with the number of repetitions that had it.

| configuration | engine | runs | write-amp median (min-max) | written MB median | most frequent files per level (L0..L6) |
|---|---|---:|---:|---:|---|
| num=1000000, value=100B, threads=1, compression=none | c-native | 15 | 0.86 (0.67-1.16) | 143.4 | `3 0 0 0 0 0 0` (6/15) |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 15 | 0.86 (0.67-1.25) | 143.4 | `3 0 0 0 0 0 0` (6/15) |
| num=1000000, value=100B, threads=1, compression=none | managed | 15 | 0.86 (0.67-1.16) | 143.4 | `3 0 0 0 0 0 0` (6/15) |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 15 | 0.86 (0.67-1.25) | 143.4 | `3 0 0 0 0 0 0` (6/15) |
| num=1000000, value=100B, threads=4, compression=none | c-native | 15 | 1.00 (0.56-1.25) | 225.3 | `3 0 0 0 0 0 0` (6/15) |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 15 | 1.02 (0.56-1.25) | 225.3 | `3 0 0 0 0 0 0` (5/15) |
| num=1000000, value=100B, threads=4, compression=none | managed | 15 | 1.02 (0.56-1.25) | 225.3 | `3 0 0 0 0 0 0` (5/15) |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 15 | 1.00 (0.56-1.11) | 225.3 | `3 0 0 0 0 0 0` (6/15) |

Every repetition:

| configuration | engine | rep | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---:|---|---:|---:|---:|---:|
| num=1000000, value=100B, threads=1, compression=none | c-native | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 1 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 1 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 1 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 1 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145652701 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145648918 |
| num=1000000, value=100B, threads=1, compression=none | managed | 1 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145653757 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 1 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145655657 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 1 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 1 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 1 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `1 0 0 0 0 0 2` | 221.3 | 276.5 | 1.25 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 1 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 1 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 1 | `4 0 0 0 0 0 0` | 165.9 | 133.1 | 0.80 | 142855417 |
| num=1000000, value=100B, threads=4, compression=none | managed | 1 | `4 0 0 0 0 0 0` | 165.9 | 133.1 | 0.80 | 144143987 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 1 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 1 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 1 | `1 0 0 0 0 0 2` | 276.6 | 276.5 | 1.00 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 1 | `1 0 0 0 0 0 2` | 276.6 | 276.5 | 1.00 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 1 | `1 0 0 0 0 0 2` | 276.6 | 317.4 | 1.15 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 1 | `1 0 0 0 0 0 2` | 276.6 | 276.5 | 1.00 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 1 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 0 2` | 331.9 | 358.4 | 1.08 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 1 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 1 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 2 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 2 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 2 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 2 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145650453 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145649452 |
| num=1000000, value=100B, threads=1, compression=none | managed | 2 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145656952 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 2 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145653778 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 2 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 2 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 2 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 2 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 2 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 2 | `0 0 0 0 0 0 2` | 221.3 | 276.5 | 1.25 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 2 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 2 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 2 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 2 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 2 | `1 0 0 0 0 0 2` | 221.3 | 276.5 | 1.25 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 2 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 2 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 2 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 2 | `2 0 0 0 0 0 2` | 276.6 | 317.4 | 1.15 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 2 | `2 0 0 0 0 0 2` | 276.6 | 317.4 | 1.15 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 2 | `2 0 0 0 0 0 2` | 276.6 | 317.4 | 1.15 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 2 | `1 0 0 0 0 0 2` | 276.6 | 276.5 | 1.00 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 2 | `3 0 0 0 0 0 2` | 331.9 | 358.4 | 1.08 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 2 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 2 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 3 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 3 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 3 | `3 0 0 0 0 0 0` | 138.3 | 92.2 | 0.67 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 3 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145651377 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145652257 |
| num=1000000, value=100B, threads=1, compression=none | managed | 3 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145654898 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 3 | `4 0 0 0 0 0 0` | 165.9 | 143.4 | 0.86 | 145656607 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 3 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 3 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 3 | `0 0 0 0 0 0 2` | 193.6 | 225.3 | 1.16 | 0 |
| num=1000000, value=100B, threads=1, compression=none | c-native | 3 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed | 3 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=1, compression=none | managed-async | 3 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 3 | `3 0 0 0 0 0 0` | 110.6 | 92.2 | 0.83 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 3 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 3 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 3 | `3 0 0 0 0 0 0` | 165.9 | 92.2 | 0.56 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 3 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 3 | `1 0 0 0 0 0 2` | 221.3 | 276.5 | 1.25 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 3 | `1 0 0 0 0 0 2` | 221.3 | 276.5 | 1.25 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 3 | `0 0 0 0 0 0 2` | 221.3 | 225.3 | 1.02 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 3 | `1 0 0 0 0 0 2` | 276.6 | 276.5 | 1.00 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 3 | `2 0 0 0 0 0 2` | 276.6 | 317.4 | 1.15 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 3 | `2 0 0 0 0 0 2` | 276.6 | 317.4 | 1.15 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 3 | `1 0 0 0 0 0 2` | 276.6 | 276.5 | 1.00 | 0 |
| num=1000000, value=100B, threads=4, compression=none | c-native | 3 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=4, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 0 2` | 331.9 | 358.4 | 1.08 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed | 3 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |
| num=1000000, value=100B, threads=4, compression=none | managed-async | 3 | `3 0 0 0 0 0 2` | 331.9 | 368.6 | 1.11 | 0 |

Raw data: `2026-11-07-mixed-readwrite.csv` `2026-11-07-mixed-readwrite.lsm.csv` 
