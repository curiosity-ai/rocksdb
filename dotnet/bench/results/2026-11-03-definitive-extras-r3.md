# RocksDB engine comparison (definitive extras, 3 repetitions, warm, 2026-11-03): C native vs RocksDb.Managed

## Machine

- Date: 2026-11-03 12:04-12:15 UTC, host vm (same session as 2026-11-03-definitive-r5, right after it)
- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz (family 6 model 207), 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v230
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by c-native
- Repetitions: 3, all configurations in turn per repetition (engines interleaved), each repetition started at a 1-minute load average <= 1.5: rep 1 12:04 1.22 1.68 1.71; rep 2 12:07 1.40 1.69 1.72; rep 3 12:11 1.25 1.65 1.71
- 1-minute load average before each of the 24 engine-set runs: min 1.22, median 1.61, max 2.25; total runtime 10 min 53 s

Extra rows for the 2026-11-03 definitive run (tree fb97d3d), REPEATS=3, quiet machine, Release build, warm-up on (`--warmup true`) for every engine. Same configurations as 2026-11-03-final-extras-r3 (without `readheavy`), plus `flush-1k` / `flush-1k-lz4`. Common options as in the matrix (16-byte keys, values 50% compressible, 8 MiB LRU block cache, 10-bit full bloom filter, 64 MiB write buffer unless stated, 4 KiB blocks, format_version 6, WAL on, no sync). Configurations:

- `w4-default` / `w4-pipelined` / `w4-unordered`: `fillrandom,overwrite`, 300k x 100 B, 4 threads, no compression; default write path, `--enable_pipelined_write true`, `--unordered_write true` (all engines set the option).
- `subcomp1` / `subcomp4`: `fillrandom,compact`, 1M x 100 B, 1 thread, `--disable_auto_compactions true --write_buffer_size 16777216 --target_file_size_base 4194304 --max_background_jobs 8`, `--max_subcompactions 1` / `4`; `compact` is one full CompactRange of the L0 files (ms).
- `reverse-1k`: `fillseq,fillrandom,overwrite,flush,readreverse,seekrandom`, 100k x 1 KiB, 1 thread, no compression, `--reverse_iterator true` (seekrandom = SeekForPrev + 10 x Prev).
- `flush-1k` / `flush-1k-lz4`: `fillseq,fillrandom,overwrite,flush`, 100k x 1 KiB, 1 thread, no compression / LZ4: the `flush` regression of the 2026-11-03 final run (1.58-2.06x of C with 1 KiB values; docs/perf.md "Final-run regressions").

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 3 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | managed / c-native | managed-async / c-native |
|---|---:|---:|
| w4-default: num=300000, value=100B, threads=4, compression=none | 0.82x [0.72-0.87] (2) | 0.94x [0.82-0.98] (2) |
| w4-pipelined: num=300000, value=100B, threads=4, compression=none | 0.78x [0.73-0.95] (2) | 1.01x [0.96-1.15] (2) |
| w4-unordered: num=300000, value=100B, threads=4, compression=none | 0.84x [0.75-0.86] (2) | 0.99x [0.93-1.04] (2) |
| subcomp1: num=1000000, value=100B, threads=1, compression=none | 0.70x [0.66-0.78] (2) | - |
| subcomp4: num=1000000, value=100B, threads=1, compression=none | 0.74x [0.72-0.78] (2) | - |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | 0.63x [0.58-0.67] (6) | 0.61x [0.57-0.68] (6) |
| flush-1k: num=100000, value=1024B, threads=1, compression=none | 0.72x [0.53-0.78] (4) | 0.71x [0.55-0.83] (4) |
| flush-1k-lz4: num=100000, value=1024B, threads=1, compression=lz4 | 0.67x [0.59-0.74] (4) | 0.67x [0.60-0.75] (4) |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| managed | 72 | 2625 | 11 | 282 (compact, subcomp1: num=1000000, value=100B, threads=1, compression=none, rep 2) |
| managed-async | 60 | 1456 | 9 | 135 (overwrite, w4-pipelined: num=300000, value=100B, threads=4, compression=none, rep 2) |

## w4-default: num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 5.30 (5.04-5.58, 5%) | 4.17 (4.01-4.23, 3%) | 4.84 (4.60-4.93, 4%) | 0.79x | 0.91x | 68.95 | 56.59 | 58.87 | 10.5 | 22.2 | 11 | 18 |
| overwrite ! | 5.01 (4.91-5.94, 11% !) | 4.26 (3.95-4.57, 7%) | 4.84 (4.58-4.88, 3%) | 0.85x | 0.97x | 57.97 | 52.76 | 59.20 | 138.4 | 142.5 | 179 | 95 |

## w4-pipelined: num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 4.42 (3.59-4.44, 12% !) | 3.16 (2.98-3.75, 12% !) | 4.28 (3.93-4.43, 6%) | 0.71x | 0.97x | 62.54 | 38.11 | 50.20 | 0.0 | 0.0 | 186 | 21 |
| overwrite | 4.34 (4.28-4.38, 1%) | 3.71 (3.42-3.99, 8%) | 4.61 (4.55-4.62, 1%) | 0.86x | 1.06x | 60.62 | 48.10 | 53.64 | 138.7 | 131.5 | 119 | 135 |

## w4-unordered: num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 2.79 (2.76-2.94, 3%) | 2.29 (2.05-2.36, 7%) | 2.92 (2.87-3.06, 3%) | 0.82x | 1.05x | 33.10 | 33.28 | 36.41 | 0.0 | 0.0 | 11 | 18 |
| overwrite | 3.03 (3.01-3.10, 1%) | 2.60 (2.33-2.61, 6%) | 2.86 (2.62-2.97, 6%) | 0.86x | 0.94x | 48.99 | 32.92 | 37.91 | 124.5 | 103.5 | 119 | 74 |

## subcomp1: num=1000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.37 (3.20-3.62, 6%) | 2.18 (2.11-2.25, 3%) | - | 0.65x | - | 11.52 | 7.60 | - | 2.9 | - | 203 | - |
| compact | 705.9 ms (642.5 ms-743.9 ms, 7%) | 534.5 ms (514.1 ms-592.9 ms, 7%) | - | 0.76x | - | - | - | - | 441272.0 | - | 282 | - |

## subcomp4: num=1000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.43 (3.42-3.53, 2%) | 2.19 (2.16-2.20, 1%) | - | 0.64x | - | 12.21 | 7.50 | - | 2.8 | - | 89 | - |
| compact | 314.9 ms (308.4 ms-321.7 ms, 2%) | 268.7 ms (267.4 ms-294.7 ms, 6%) | - | 0.85x | - | - | - | - | 3431408.0 | - | 27 | - |

## reverse-1k: num=100000, value=1024B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 5.39 (5.32-6.24, 9%) | 4.29 (3.96-4.55, 7%) | 4.35 (3.64-5.21, 18% !) | 0.80x | 0.81x | 19.80 | 12.44 | 12.03 | 0.4 | 1.7 | 49 | 10 |
| overwrite | 5.77 (5.54-5.82, 3%) | 4.70 (4.46-5.15, 7%) | 4.70 (4.54-4.86, 3%) | 0.81x | 0.81x | 19.17 | 12.60 | 15.82 | 14.8 | 17.4 | 17 | 25 |
| fillseq ! | 3.97 (3.92-4.60, 9%) | 2.27 (2.23-2.42, 5%) | 2.34 (2.26-3.41, 24% !) | 0.57x | 0.59x | 13.67 | 7.18 | 7.17 | 11.4 | 13.2 | 13 | 6 |
| flush ! | 37.5 ms (37.2 ms-39.0 ms, 3%) | 31.5 ms (27.9 ms-34.3 ms, 10% !) | 27.3 ms (25.8 ms-31.6 ms, 11% !) | 0.84x | 0.73x | - | - | - | 55192.0 | 55192.0 | 13 | 11 |
| readreverse ! | 3.25 (2.72-3.44, 12% !) | 0.95 (0.79-1.08, 15% !) | 0.86 (0.78-0.86, 6%) | 0.29x | 0.26x | 13.43 | 3.98 | 3.81 | 85.8 | 85.8 | 27 | 22 |
| seekrandom | 16.45 (16.38-17.56, 4%) | 11.26 (11.22-11.53, 1%) | 11.43 (11.25-12.24, 5%) | 0.68x | 0.69x | 58.78 | 49.27 | 50.07 | 0.1 | 0.0 | 11 | 12 |

## flush-1k: num=100000, value=1024B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 6.00 (5.88-6.07, 2%) | 4.41 (4.06-4.62, 6%) | 4.67 (4.50-4.98, 5%) | 0.73x | 0.78x | 20.22 | 13.20 | 11.93 | 0.4 | 0.4 | 11 | 2 |
| overwrite ! | 5.81 (5.73-6.05, 3%) | 4.66 (3.72-4.88, 14% !) | 4.54 (4.54-4.75, 3%) | 0.80x | 0.78x | 19.06 | 13.35 | 12.68 | 17.6 | 14.3 | 15 | 3 |
| fillseq ! | 3.94 (3.91-4.17, 4%) | 2.13 (1.94-2.53, 14% !) | 2.43 (2.36-3.91, 30% !) | 0.54x | 0.62x | 13.93 | 7.56 | 9.14 | 13.2 | 10.7 | 4 | 19 |
| flush ! | 43.9 ms (38.3 ms-109.8 ms, 62% !) | 36.8 ms (32.5 ms-37.8 ms, 8%) | 29.2 ms (28.8 ms-31.2 ms, 4%) | 0.84x | 0.67x | - | - | - | 54968.0 | 55192.0 | 10 | 10 |

## flush-1k-lz4: num=100000, value=1024B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | managed / C | managed-async / C | c-native p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 6.27 (5.68-6.47, 7%) | 3.75 (3.59-4.67, 15% !) | 3.67 (3.50-3.84, 5%) | 0.60x | 0.58x | 23.42 | 14.34 | 13.18 | 0.2 | 0.2 | 3 | 2 |
| overwrite ! | 6.45 (5.60-7.40, 14% !) | 4.46 (3.92-4.76, 10%) | 4.36 (3.97-4.52, 7%) | 0.69x | 0.68x | 25.57 | 17.01 | 13.97 | 2.5 | 5.5 | 73 | 120 |
| fillseq | 3.74 (3.55-3.75, 3%) | 2.24 (2.03-2.36, 8%) | 2.16 (2.07-2.33, 6%) | 0.60x | 0.58x | 11.41 | 7.01 | 6.54 | 7.0 | 5.4 | 8 | 4 |
| flush ! | 43.8 ms (40.7 ms-58.6 ms, 20% !) | 35.1 ms (32.4 ms-37.0 ms, 7%) | 37.7 ms (34.2 ms-50.1 ms, 21% !) | 0.80x | 0.86x | - | - | - | 94656.0 | 94304.0 | 95 | 3 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| w4-default: num=300000, value=100B, threads=4, compression=none | overwrite | c-native | 10.7% | 5.01 | 4.91-5.94 | 3 |
| w4-pipelined: num=300000, value=100B, threads=4, compression=none | fillrandom | c-native | 11.7% | 4.42 | 3.59-4.44 | 3 |
| w4-pipelined: num=300000, value=100B, threads=4, compression=none | fillrandom | managed | 12.2% | 3.16 | 2.98-3.75 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | fillrandom | managed-async | 17.8% | 4.35 | 3.64-5.21 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | fillseq | managed-async | 24.1% | 2.34 | 2.26-3.41 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | flush | managed | 10.2% | 31.5 ms | 27.9 ms-34.3 ms | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | flush | managed-async | 10.6% | 27.3 ms | 25.8 ms-31.6 ms | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | readreverse | c-native | 11.8% | 3.25 | 2.72-3.44 | 3 |
| reverse-1k: num=100000, value=1024B, threads=1, compression=none | readreverse | managed | 15.4% | 0.95 | 0.79-1.08 | 3 |
| flush-1k: num=100000, value=1024B, threads=1, compression=none | overwrite | managed | 14.0% | 4.66 | 3.72-4.88 | 3 |
| flush-1k: num=100000, value=1024B, threads=1, compression=none | fillseq | managed | 13.6% | 2.13 | 1.94-2.53 | 3 |
| flush-1k: num=100000, value=1024B, threads=1, compression=none | fillseq | managed-async | 30.2% | 2.43 | 2.36-3.91 | 3 |
| flush-1k: num=100000, value=1024B, threads=1, compression=none | flush | c-native | 62.1% | 43.9 ms | 38.3 ms-109.8 ms | 3 |
| flush-1k-lz4: num=100000, value=1024B, threads=1, compression=lz4 | fillrandom | managed | 14.6% | 3.75 | 3.59-4.67 | 3 |
| flush-1k-lz4: num=100000, value=1024B, threads=1, compression=lz4 | overwrite | c-native | 13.9% | 6.45 | 5.60-7.40 | 3 |
| flush-1k-lz4: num=100000, value=1024B, threads=1, compression=lz4 | flush | c-native | 20.1% | 43.8 ms | 40.7 ms-58.6 ms | 3 |
| flush-1k-lz4: num=100000, value=1024B, threads=1, compression=lz4 | flush | managed-async | 20.6% | 37.7 ms | 34.2 ms-50.1 ms | 3 |

Raw data: `2026-11-03-definitive-extras-r3.csv` 
