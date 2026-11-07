# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-03 07:31-07:41 UTC, host vm (same session as 2026-11-03-final-warm-r5, right after it)
- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v230
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by c-native
- Repetitions: 3, all configurations in turn per repetition (engines interleaved), each repetition started at a 1-minute load average <= 1.5; 1-minute load average before the 21 engine-set runs 1.2-2.1; total runtime 9 min

Extra rows for the 2026-11-03 final run (tree cfb30f9), REPEATS=3, quiet machine, Release build, warm-up on (`--warmup true`) for every engine. Common options as in the matrix (16-byte keys, values 50% compressible, 8 MiB LRU block cache, 10-bit full bloom filter, 64 MiB write buffer unless stated, 4 KiB blocks, format_version 6, WAL on, no sync). Configurations:

- `w4-default` / `w4-pipelined` / `w4-unordered`: `fillrandom,overwrite`, 300k x 100 B, 4 threads, no compression; default write path, `--enable_pipelined_write true`, `--unordered_write true` (all engines set the option).
- `subcomp1` / `subcomp4`: `fillrandom,compact`, 1M x 100 B, 1 thread, `--disable_auto_compactions true --write_buffer_size 16777216 --target_file_size_base 4194304 --max_background_jobs 8`, `--max_subcompactions 1` / `4`; `compact` is one full CompactRange of the L0 files (ms).
- `reverse-1k`: `fillseq,fillrandom,overwrite,flush,readreverse,seekrandom`, 100k x 1 KiB, 1 thread, no compression, `--reverse_iterator true` (seekrandom = SeekForPrev + 10 x Prev).
- `readheavy`: `fillrandom,waitforcompaction,readrandom,readrandom`, 500k x 100 B, 1M reads, 4 threads, the compaction-heavy shape (`--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`), so reads go through a multi-level tree with read sampling (read-triggered compaction bookkeeping) active in both engines. readrandom runs twice per process, hence "up to 6 repetitions" for that row.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 6 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | managed / c-native | managed-async / c-native |
|---|---:|---:|
| w4-default: num=300000, value=100B, threads=4, compression=none | 0.91x [0.83-1.08] (2) | 0.92x [0.86-1.00] (2) |
| w4-pipelined: num=300000, value=100B, threads=4, compression=none | 0.91x [0.83-0.98] (2) | 1.18x [1.02-1.23] (2) |
| w4-unordered: num=300000, value=100B, threads=4, compression=none | 0.76x [0.70-0.88] (2) | 0.92x [0.85-0.99] (2) |
| subcomp1: num=1000000, value=100B, threads=1, compression=none | 0.79x [0.70-0.84] (2) | - |
| subcomp4: num=1000000, value=100B, threads=1, compression=none | 0.96x [0.89-1.09] (2) | - |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | 0.73x [0.63-0.85] (6) | 0.74x [0.63-0.90] (6) |
| readheavy: num=500000, value=100B, threads=4, compression=none | 0.69x [0.62-0.79] (2) | - |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| managed | 60 | 6741 | 11 | 835 (fillrandom, readheavy: num=500000, value=100B, threads=4, compression=none, rep 1) |
| managed-async | 36 | 1262 | 12 | 225 (overwrite, w4-pipelined: num=300000, value=100B, threads=4, compression=none, rep 2) |

## w4-default: num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 5.02 (4.91-5.18, 3%) | 4.30 (3.86-4.90, 12% !) | 4.59 (4.23-5.04, 9%) | 0.86x | 0.91x | 66.75 | 60.37 | 52.08 | 4.0 | 39.9 | 11 | 19 |
| overwrite ! | 4.97 (4.85-5.04, 2%) | 4.83 (4.47-5.99, 15% !) | 4.58 (4.38-4.81, 5%) | 0.97x | 0.92x | 59.97 | 69.12 | 52.86 | 134.6 | 149.4 | 170 | 90 |

## w4-pipelined: num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.74 (3.73-4.16, 6%) | 3.41 (3.40-3.68, 5%) | 4.68 (4.09-4.82, 8%) | 0.91x | 1.25x | 50.63 | 48.50 | 57.56 | 4.0 | 18.0 | 8 | 21 |
| overwrite | 4.37 (4.09-4.74, 7%) | 3.99 (3.90-4.27, 5%) | 4.83 (4.54-4.84, 4%) | 0.91x | 1.11x | 60.56 | 55.60 | 53.82 | 131.1 | 138.1 | 96 | 225 |

## w4-unordered: num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 3.31 (3.01-3.68, 10% !) | 2.33 (2.24-2.64, 9%) | 3.10 (2.97-3.18, 3%) | 0.70x | 0.94x | 43.46 | 28.18 | 41.74 | 18.0 | 0.5 | 12 | 17 |
| overwrite | 3.17 (3.08-3.28, 3%) | 2.59 (2.51-2.88, 7%) | 2.89 (2.83-3.00, 3%) | 0.82x | 0.91x | 44.39 | 29.48 | 38.60 | 148.6 | 110.1 | 58 | 107 |

## subcomp1: num=1000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.42 (3.37-4.01, 10%) | 2.53 (2.34-2.68, 7%) | - | 0.74x | - | 11.74 | 8.52 | - | 1.1 | - | 238 | - |
| compact | 697.3 ms (678.7 ms-724.0 ms, 3%) | 586.4 ms (584.1 ms-623.2 ms, 4%) | - | 0.84x | - | - | - | - | 73912976.0 | - | 260 | - |

## subcomp4: num=1000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 3.71 (3.44-4.05, 8%) | 2.33 (2.22-2.81, 13% !) | - | 0.63x | - | 13.60 | 7.26 | - | 0.5 | - | 369 | - |
| compact | 329.8 ms (329.8 ms-332.8 ms, 1%) | 483.9 ms (462.1 ms-497.8 ms, 4%) | - | 1.47x | - | - | - | - | 293684328.0 | - | 414 | - |

## reverse-1k: num=100000, value=1024B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 6.11 (5.65-6.67, 8%) | 4.45 (4.40-4.63, 3%) | 3.79 (3.62-5.09, 19% !) | 0.73x | 0.62x | 21.44 | 11.28 | 12.46 | 1.7 | 2.2 | 30 | 9 |
| overwrite ! | 6.20 (5.63-6.71, 9%) | 5.69 (5.10-5.87, 7%) | 5.51 (4.41-6.55, 20% !) | 0.92x | 0.89x | 20.92 | 13.72 | 18.33 | 16.2 | 15.5 | 27 | 10 |
| fillseq | 4.02 (3.73-4.13, 5%) | 2.11 (2.01-2.20, 5%) | 2.35 (2.22-2.40, 4%) | 0.52x | 0.58x | 12.83 | 7.18 | 8.48 | 12.0 | 11.3 | 3 | 6 |
| flush ! | 38.2 ms (36.1 ms-74.5 ms, 44% !) | 84.6 ms (78.8 ms-163.3 ms, 43% !) | 80.1 ms (72.5 ms-126.7 ms, 32% !) | 2.22x | 2.10x | - | - | - | 56312.0 | 56544.0 | 11 | 2 |
| readreverse ! | 2.96 (2.69-3.02, 6%) | 0.80 (0.70-0.96, 16% !) | 0.99 (0.77-1.77, 45% !) | 0.27x | 0.34x | 11.60 | 3.64 | 4.72 | 86.6 | 86.5 | 3 | 6 |
| seekrandom | 16.57 (16.35-17.33, 3%) | 11.41 (11.20-13.00, 8%) | 12.26 (12.16-12.89, 3%) | 0.69x | 0.74x | 59.70 | 48.78 | 52.51 | 0.7 | 0.6 | 11 | 12 |

## readheavy: num=500000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 4.89 (4.16-5.62, 15% !) | 4.04 (3.79-4.08, 4%) | - | 0.83x | - | 69.73 | 62.87 | - | 88.7 | - | 835 | - |
| waitforcompaction ! | 34.21 (28.19-34.85, 11% !) | 28.28 (27.20-29.23, 4%) | - | 0.83x | - | - | - | - | 40.0 | - | 0 | - |
| readrandom ! | 1.76 (1.55-1.81, 7%) | 1.02 (0.91-1.26, 11% !) | - | 0.58x | - | 33.47 | 13.05 | - | 4.7 | - | 8 | - |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| w4-default: num=300000, value=100B, threads=4, compression=none | fillrandom | managed | 11.9% | 4.30 | 3.86-4.90 | 3 |
| w4-default: num=300000, value=100B, threads=4, compression=none | overwrite | managed | 15.5% | 4.83 | 4.47-5.99 | 3 |
| w4-unordered: num=300000, value=100B, threads=4, compression=none | fillrandom | c-native | 10.0% | 3.31 | 3.01-3.68 | 3 |
| subcomp4: num=1000000, value=100B, threads=1, compression=none | fillrandom | managed | 12.7% | 2.33 | 2.22-2.81 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | fillrandom | managed-async | 19.3% | 3.79 | 3.62-5.09 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | overwrite | managed-async | 19.5% | 5.51 | 4.41-6.55 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | flush | c-native | 43.6% | 38.2 ms | 36.1 ms-74.5 ms | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | flush | managed | 43.3% | 84.6 ms | 78.8 ms-163.3 ms | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | flush | managed-async | 31.5% | 80.1 ms | 72.5 ms-126.7 ms | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | readreverse | managed | 15.7% | 0.80 | 0.70-0.96 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | readreverse | managed-async | 44.5% | 0.99 | 0.77-1.77 | 3 |
| readheavy: num=500000, value=100B, threads=4, compression=none | fillrandom | c-native | 14.9% | 4.89 | 4.16-5.62 | 3 |
| readheavy: num=500000, value=100B, threads=4, compression=none | waitforcompaction | c-native | 11.3% | 34.21 | 28.19-34.85 | 3 |
| readheavy: num=500000, value=100B, threads=4, compression=none | readrandom | managed | 11.3% | 1.02 | 0.91-1.26 | 6 |

Raw data: `2026-11-03-final-extras-r3.csv` 
