# RocksDB engine comparison (definitive, 5 repetitions, warm, 2026-11-03): C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-03 10:44 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v230
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 0.71 1.95 1.97
- Repetitions: 5; UTC time and load average (1/5/15 min) at the start of each: rep 1 10:44 0.71 1.95 1.97; rep 2 11:00 1.28 1.68 1.77; rep 3 11:16 1.44 1.75 1.80; rep 4 11:32 1.39 1.66 1.72; rep 5 11:47 1.41 1.70 1.71
- 1-minute load average before each of the 45 engine-set runs: min 0.71, median 1.44, max 3.21
- Load average at end: 1.44 1.74 1.73; total runtime 79 min 5 s

Matrix: value sizes [100 1024] (num=300000, reads=num; large values num=100000, reads=num), threads [1 4],
compression [none lz4], benchmarks `fillseq,fillrandom,overwrite,flush,readrandom,readmissing,seekrandom,readseq,readreverse,multireadrandom,readwhilewriting,compact,deleterandom,fillsync`. Common options: 16-byte keys, values 50% compressible,
8 MiB LRU block cache, 10-bit full bloom filter, 64 MiB write buffer, 4 KiB blocks, format_version 6, WAL on, no sync
(fillsync: num/1000 synced puts). Fill benchmarks start from a fresh DB; the others run on the DB left by the previous step.
Ops are split across threads; readwhilewriting = N reader threads + 1 writer. Each engine runs in its own process.

Compaction-heavy configuration (label `compaction`): num=500000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Warm-up on for every engine (`--warmup true`): before the measured run, each process runs every benchmark of the list
on a separate temporary DB (same options, max(50k, num/10) <= 200k keys unless --warmup_ops; repeated benchmarks for >= 32
repetitions, >= warmup_ops ops and >= 1 s each) plus 3 reopen + flush/compaction cycles, deletes it, and (.NET) waits for
the JIT to quiesce; every measured benchmark starts after a settle pause (JIT quiet for 200 ms; the C leg sleeps 200 ms).
The measured run starts on a fresh DB and block cache. The jit columns show the methods compiled during each measurement.

Definitive statistically repeated run on commit fb97d3d (since 2026-11-03-final-warm-r5 on cfb30f9: flush, compaction readahead, compaction allocation and subcompaction fixes; see docs/perf.md): REPEATS=5, LOAD_MAX=1.5, quiet machine (no other agents, nothing compiled during the measurements). Release build, warm-up on. Same sizes, matrix and CPU (Intel Xeon @ 2.10GHz, family 6 model 207) as 2026-11-03-final-warm-r5, so the two runs compare cell by cell.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 5 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| num=300000, value=100B, threads=1, compression=none | 1.09x [1.02-1.13] (14) | 0.68x [0.65-0.71] (14) | 0.70x [0.66-0.72] (14) |
| num=300000, value=100B, threads=4, compression=none | 1.01x [0.96-1.07] (14) | 0.76x [0.72-0.79] (14) | 0.75x [0.70-0.78] (14) |
| num=300000, value=100B, threads=1, compression=lz4 | 1.10x [1.02-1.14] (14) | 0.68x [0.64-0.72] (14) | 0.70x [0.65-0.73] (14) |
| num=300000, value=100B, threads=4, compression=lz4 | 1.06x [1.01-1.12] (14) | 0.74x [0.70-0.77] (14) | 0.75x [0.70-0.78] (14) |
| num=100000, value=1024B, threads=1, compression=none | 1.01x [0.93-1.09] (14) | 0.69x [0.63-0.75] (14) | 0.69x [0.64-0.74] (14) |
| num=100000, value=1024B, threads=4, compression=none | 1.00x [0.95-1.07] (14) | 0.77x [0.73-0.81] (14) | 0.75x [0.71-0.79] (14) |
| num=100000, value=1024B, threads=1, compression=lz4 | 1.00x [0.94-1.05] (14) | 0.67x [0.65-0.71] (14) | 0.73x [0.70-0.77] (14) |
| num=100000, value=1024B, threads=4, compression=lz4 | 1.09x [1.05-1.17] (14) | 0.81x [0.77-0.84] (14) | 0.81x [0.76-0.84] (14) |
| compaction: num=500000, value=100B, threads=1, compression=none | 1.06x [1.00-1.12] (4) | 0.58x [0.54-0.62] (4) | 0.59x [0.55-0.62] (4) |
| **all matrix configurations** | **1.05x [1.02-1.06] (112)** | **0.72x [0.71-0.74] (112)** | **0.73x [0.72-0.74] (112)** |

### Per benchmark (geometric mean over the 8 matrix configurations of the median ratio vs c-native)

| benchmark | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| fillseq | 1.00x [0.96-1.07] | 0.68x [0.64-0.73] | 0.66x [0.63-0.69] |
| fillrandom | 1.03x [0.96-1.06] | 0.74x [0.71-0.79] | 0.77x [0.72-0.79] |
| overwrite | 1.03x [0.97-1.06] | 0.77x [0.73-0.80] | 0.78x [0.73-0.82] |
| flush | 0.97x [0.82-1.05] | 0.73x [0.62-0.82] | 0.73x [0.61-0.78] |
| readrandom | 1.04x [0.99-1.10] | 0.67x [0.63-0.69] | 0.73x [0.69-0.77] |
| readmissing | 1.11x [0.99-1.22] | 0.60x [0.54-0.66] | 0.70x [0.64-0.75] |
| seekrandom | 1.09x [1.05-1.12] | 0.61x [0.60-0.64] | 0.63x [0.61-0.65] |
| readseq | 1.18x [1.07-1.25] | 0.66x [0.61-0.71] | 0.66x [0.60-0.69] |
| readreverse | 1.14x [1.05-1.20] | 0.69x [0.64-0.74] | 0.68x [0.62-0.72] |
| multireadrandom | 1.04x [1.00-1.09] | 0.71x [0.68-0.75] | 0.73x [0.70-0.78] |
| readwhilewriting | 1.03x [0.98-1.09] | 0.78x [0.75-0.82] | 0.78x [0.74-0.83] |
| compact | 0.99x [0.94-1.07] | 0.84x [0.81-0.90] | 0.82x [0.76-0.88] |
| deleterandom | 1.03x [0.97-1.12] | 0.71x [0.67-0.76] | 0.75x [0.72-0.80] |
| fillsync | 0.97x [0.87-1.17] | 1.02x [0.91-1.10] | 0.90x [0.81-1.03] |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| rocksdb-sharp | 585 | 149 | 0 | 5 (readwhilewriting, num=100000, value=1024B, threads=1, compression=lz4, rep 1) |
| managed | 585 | 4024 | 1 | 635 (overwrite, compaction: num=500000, value=100B, threads=1, compression=none, rep 3) |
| managed-async | 585 | 5100 | 1 | 467 (overwrite, compaction: num=500000, value=100B, threads=1, compression=none, rep 4) |

## num=300000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 1.86 (1.65-2.47, 16% !) | 1.94 (1.76-2.00, 6%) | 0.99 (0.94-1.05, 4%) | 1.12 (1.07-1.23, 6%) | 1.04x | 0.54x | 0.60x | 6.11 | 6.30 | 3.54 | 4.18 | 0.0 | 0.0 | 3 | 3 | 5 |
| fillrandom ! | 4.34 (3.85-4.76, 8%) | 4.56 (4.28-4.66, 3%) | 3.08 (2.98-3.29, 4%) | 3.24 (2.84-3.75, 11% !) | 1.05x | 0.71x | 0.75x | 14.10 | 16.08 | 9.24 | 10.19 | 0.0 | 0.0 | 0 | 0 | 1 |
| overwrite | 4.85 (4.39-5.26, 7%) | 4.93 (4.56-5.30, 6%) | 3.47 (3.06-3.82, 8%) | 3.48 (3.34-3.59, 3%) | 1.02x | 0.72x | 0.72x | 16.32 | 17.43 | 11.12 | 10.49 | 79.1 | 16.1 | 0 | 14 | 9 |
| flush ! | 97.9 ms (94.8 ms-123.7 ms, 12% !) | 97.8 ms (93.5 ms-105.2 ms, 4%) | 61.9 ms (60.2 ms-80.7 ms, 13% !) | 69.8 ms (59.0 ms-84.0 ms, 13% !) | 1.00x | 0.63x | 0.71x | - | - | - | - | 341952.0 | 343672.0 | 0 | 19 | 37 |
| readrandom | 4.05 (3.98-4.80, 8%) | 4.52 (4.14-4.87, 6%) | 2.92 (2.68-3.04, 6%) | 3.17 (2.96-3.45, 6%) | 1.12x | 0.72x | 0.78x | 14.98 | 18.45 | 8.81 | 9.85 | 28.7 | 28.7 | 0 | 37 | 70 |
| readmissing ! | 0.70 (0.63-0.75, 8%) | 0.72 (0.67-0.82, 8%) | 0.33 (0.30-0.47, 19% !) | 0.36 (0.36-0.51, 17% !) | 1.03x | 0.47x | 0.52x | 5.65 | 6.16 | 3.92 | 4.10 | 0.0 | 0.0 | 0 | 0 | 0 |
| seekrandom | 11.47 (10.93-12.22, 4%) | 13.54 (12.68-14.04, 4%) | 7.28 (6.94-7.83, 5%) | 7.66 (7.43-8.08, 3%) | 1.18x | 0.63x | 0.67x | 46.55 | 49.78 | 27.52 | 30.96 | 0.0 | 0.0 | 0 | 2 | 1 |
| readseq ! | 0.41 (0.39-0.53, 15% !) | 0.56 (0.49-0.66, 10% !) | 0.25 (0.22-0.36, 22% !) | 0.24 (0.23-0.26, 6%) | 1.36x | 0.62x | 0.58x | 3.42 | 4.32 | 2.78 | 2.65 | 0.0 | 0.0 | 0 | 2 | 0 |
| readreverse ! | 0.50 (0.49-0.62, 11% !) | 0.63 (0.55-0.70, 9%) | 0.32 (0.26-0.43, 19% !) | 0.32 (0.29-0.33, 6%) | 1.27x | 0.65x | 0.65x | 3.77 | 4.60 | 3.28 | 3.10 | 0.0 | 0.0 | 0 | 0 | 0 |
| multireadrandom | 3.79 (3.72-4.08, 4%) | 3.93 (3.75-4.14, 4%) | 2.90 (2.57-2.99, 6%) | 2.91 (2.78-2.95, 3%) | 1.04x | 0.77x | 0.77x | 138 | 141 | 113 | 110 | 0.1 | 0.1 | 0 | 0 | 0 |
| readwhilewriting | 6.21 (5.63-6.50, 5%) | 6.55 (5.95-7.09, 8%) | 4.86 (4.24-5.53, 9%) | 4.61 (4.55-5.26, 6%) | 1.06x | 0.78x | 0.74x | 23.44 | 26.46 | 15.64 | 14.55 | 0.0 | 0.0 | 0 | 0 | 0 |
| compact ! | 546.5 ms (483.1 ms-641.3 ms, 11% !) | 538.4 ms (498.4 ms-655.4 ms, 11% !) | 415.1 ms (397.4 ms-465.2 ms, 7%) | 400.0 ms (391.5 ms-461.4 ms, 8%) | 0.99x | 0.76x | 0.73x | - | - | - | - | 7416424.0 | 7631600.0 | 0 | 70 | 11 |
| deleterandom ! | 3.28 (3.14-3.59, 6%) | 3.44 (3.05-4.39, 15% !) | 1.94 (1.85-2.60, 15% !) | 2.03 (2.02-2.34, 7%) | 1.05x | 0.59x | 0.62x | 10.56 | 11.58 | 6.17 | 6.33 | 0.0 | 0.0 | 0 | 0 | 0 |
| fillsync ! | 185 (160-213, 11% !) | 208 (131-248, 29% !) | 190 (165-213, 11% !) | 202 (127-231, 21% !) | 1.13x | 1.03x | 1.09x | 512 | 631 | 467 | 578 | 3.8 | 2.1 | 0 | 0 | 0 |

## num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 3.56 (3.15-3.88, 7%) | 3.46 (3.22-3.71, 6%) | 2.97 (2.63-3.08, 6%) | 3.09 (2.75-3.13, 6%) | 0.97x | 0.84x | 0.87x | 44.19 | 42.44 | 42.88 | 37.30 | 0.0 | 13.7 | 3 | 3 | 5 |
| fillrandom | 4.56 (4.22-5.37, 10%) | 4.70 (4.50-5.16, 6%) | 4.45 (3.96-4.70, 6%) | 4.42 (4.07-4.53, 5%) | 1.03x | 0.98x | 0.97x | 57.49 | 59.84 | 64.94 | 51.76 | 0.0 | 12.2 | 0 | 0 | 4 |
| overwrite ! | 5.27 (5.23-5.64, 3%) | 5.16 (4.66-5.53, 7%) | 4.45 (4.24-4.63, 3%) | 4.49 (4.04-5.47, 12% !) | 0.98x | 0.84x | 0.85x | 69.73 | 66.40 | 60.58 | 55.37 | 114.2 | 121.7 | 0 | 32 | 12 |
| flush ! | 84.7 ms (77.6 ms-153.8 ms, 32% !) | 83.2 ms (76.0 ms-87.7 ms, 6%) | 66.0 ms (55.2 ms-69.8 ms, 11% !) | 55.5 ms (47.1 ms-64.7 ms, 12% !) | 0.98x | 0.78x | 0.66x | - | - | - | - | 187832.0 | 186824.0 | 0 | 6 | 38 |
| readrandom | 1.80 (1.65-1.93, 7%) | 1.76 (1.73-2.09, 8%) | 1.21 (1.13-1.37, 8%) | 1.36 (1.30-1.44, 5%) | 0.98x | 0.67x | 0.76x | 32.68 | 33.20 | 15.71 | 15.80 | 28.9 | 28.8 | 0 | 35 | 71 |
| readmissing ! | 0.25 (0.21-0.33, 18% !) | 0.25 (0.19-0.30, 22% !) | 0.21 (0.17-0.25, 17% !) | 0.20 (0.16-0.22, 12% !) | 0.99x | 0.85x | 0.80x | 6.67 | 6.07 | 5.33 | 5.20 | 0.1 | 0.0 | 0 | 0 | 1 |
| seekrandom | 5.06 (4.56-5.40, 7%) | 5.22 (4.95-5.61, 5%) | 2.74 (2.58-3.22, 9%) | 2.74 (2.60-3.02, 6%) | 1.03x | 0.54x | 0.54x | 67.51 | 62.96 | 41.57 | 40.49 | 0.2 | 0.1 | 0 | 3 | 2 |
| readseq ! | 0.13 (0.12-0.16, 11% !) | 0.14 (0.14-0.16, 6%) | 0.08 (0.07-0.09, 8%) | 0.08 (0.07-0.08, 6%) | 1.10x | 0.62x | 0.62x | 4.32 | 4.86 | 4.07 | 3.62 | 0.2 | 0.1 | 0 | 0 | 1 |
| readreverse ! | 0.15 (0.13-0.16, 7%) | 0.16 (0.15-0.20, 12% !) | 0.10 (0.09-0.10, 5%) | 0.09 (0.09-0.10, 4%) | 1.09x | 0.64x | 0.60x | 4.09 | 4.36 | 3.27 | 3.49 | 0.2 | 0.1 | 0 | 0 | 0 |
| multireadrandom | 1.72 (1.66-1.84, 4%) | 1.68 (1.59-1.84, 7%) | 1.05 (0.95-1.17, 7%) | 1.04 (0.99-1.12, 5%) | 0.98x | 0.61x | 0.60x | 249 | 216 | 137 | 145 | 0.4 | 0.3 | 0 | 1 | 1 |
| readwhilewriting ! | 2.51 (2.39-2.79, 7%) | 2.64 (2.32-3.14, 11% !) | 1.91 (1.79-2.09, 6%) | 2.06 (1.60-2.28, 13% !) | 1.05x | 0.76x | 0.82x | 37.60 | 40.60 | 16.78 | 19.31 | 0.1 | 0.0 | 0 | 1 | 1 |
| compact ! | 383.1 ms (335.0 ms-439.2 ms, 11% !) | 347.7 ms (327.4 ms-429.9 ms, 12% !) | 281.7 ms (254.9 ms-290.8 ms, 5%) | 287.2 ms (242.8 ms-365.6 ms, 16% !) | 0.91x | 0.74x | 0.75x | - | - | - | - | 5991080.0 | 7121472.0 | 0 | 73 | 17 |
| deleterandom | 3.65 (3.34-3.95, 7%) | 3.79 (3.73-3.85, 1%) | 3.27 (3.21-3.72, 6%) | 3.61 (3.33-3.73, 4%) | 1.04x | 0.89x | 0.99x | 47.71 | 50.55 | 39.00 | 40.61 | 0.0 | 12.9 | 0 | 1 | 1 |
| fillsync ! | 119 (81.96-125, 18% !) | 115 (103-174, 22% !) | 126 (111-144, 11% !) | 101 (86.20-109, 9%) | 0.97x | 1.06x | 0.85x | 1589 | 1622 | 1425 | 1371 | 12.8 | 51.4 | 0 | 1 | 1 |

## num=300000, value=100B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 1.91 (1.68-2.17, 9%) | 2.01 (1.86-2.34, 10% !) | 1.19 (0.94-1.45, 18% !) | 1.13 (0.99-1.20, 7%) | 1.05x | 0.62x | 0.59x | 6.94 | 6.53 | 3.95 | 3.96 | 0.0 | 0.0 | 3 | 3 | 7 |
| fillrandom ! | 4.37 (4.24-5.59, 12% !) | 5.11 (4.23-5.44, 11% !) | 2.92 (2.74-3.80, 14% !) | 3.34 (2.93-3.37, 6%) | 1.17x | 0.67x | 0.76x | 14.15 | 17.01 | 9.23 | 10.80 | 0.0 | 0.0 | 0 | 0 | 1 |
| overwrite ! | 4.99 (4.21-5.43, 10% !) | 5.05 (4.77-5.39, 6%) | 3.51 (3.34-3.54, 2%) | 3.76 (3.53-3.80, 3%) | 1.01x | 0.70x | 0.75x | 20.11 | 17.26 | 11.13 | 11.90 | 90.7 | 34.7 | 0 | 30 | 50 |
| flush ! | 105.0 ms (97.9 ms-118.7 ms, 9%) | 101.5 ms (99.2 ms-110.1 ms, 5%) | 71.1 ms (67.8 ms-94.3 ms, 14% !) | 69.7 ms (67.1 ms-89.8 ms, 13% !) | 0.97x | 0.68x | 0.66x | - | - | - | - | 294664.0 | 549272.0 | 0 | 6 | 38 |
| readrandom ! | 4.53 (4.36-4.72, 3%) | 4.95 (4.20-5.37, 9%) | 3.29 (2.95-4.08, 12% !) | 3.42 (3.15-3.73, 6%) | 1.09x | 0.73x | 0.75x | 15.62 | 18.31 | 10.81 | 11.18 | 28.7 | 28.7 | 0 | 37 | 70 |
| readmissing ! | 0.72 (0.64-0.81, 10%) | 0.77 (0.69-0.83, 8%) | 0.33 (0.31-0.43, 13% !) | 0.37 (0.36-0.57, 21% !) | 1.07x | 0.46x | 0.52x | 5.98 | 6.36 | 4.08 | 4.30 | 0.0 | 0.0 | 0 | 1 | 0 |
| seekrandom ! | 12.27 (11.34-12.84, 5%) | 14.86 (12.37-17.43, 12% !) | 7.97 (7.64-9.35, 9%) | 8.48 (8.01-8.77, 4%) | 1.21x | 0.65x | 0.69x | 43.59 | 48.40 | 32.11 | 32.84 | 0.0 | 0.0 | 0 | 2 | 2 |
| readseq ! | 0.46 (0.43-0.58, 16% !) | 0.58 (0.55-0.70, 10% !) | 0.25 (0.24-0.37, 20% !) | 0.25 (0.24-0.28, 8%) | 1.27x | 0.54x | 0.54x | 4.29 | 5.74 | 2.81 | 3.05 | 0.0 | 0.0 | 0 | 2 | 1 |
| readreverse ! | 0.54 (0.51-0.81, 23% !) | 0.63 (0.58-0.70, 7%) | 0.36 (0.32-0.46, 15% !) | 0.31 (0.31-0.39, 11% !) | 1.16x | 0.66x | 0.58x | 4.36 | 5.03 | 3.29 | 3.18 | 0.1 | 0.0 | 0 | 0 | 0 |
| multireadrandom ! | 3.96 (3.34-4.03, 7%) | 4.22 (3.76-4.79, 10% !) | 2.93 (2.79-3.75, 12% !) | 3.23 (2.83-3.68, 11% !) | 1.07x | 0.74x | 0.82x | 141 | 156 | 121 | 142 | 0.1 | 0.1 | 0 | 0 | 0 |
| readwhilewriting ! | 6.46 (6.14-7.34, 7%) | 7.19 (6.63-8.03, 8%) | 5.24 (4.44-5.93, 11% !) | 5.36 (4.55-5.76, 9%) | 1.11x | 0.81x | 0.83x | 24.72 | 29.85 | 16.37 | 18.63 | 0.0 | 0.0 | 0 | 0 | 10 |
| compact ! | 534.7 ms (482.6 ms-631.1 ms, 11% !) | 550.6 ms (523.1 ms-588.9 ms, 5%) | 450.6 ms (411.6 ms-602.9 ms, 16% !) | 437.0 ms (253.0 ms-518.8 ms, 24% !) | 1.03x | 0.84x | 0.82x | - | - | - | - | 6310824.0 | 6540856.0 | 0 | 15 | 39 |
| deleterandom ! | 3.41 (2.76-3.61, 10%) | 3.70 (3.09-3.92, 9%) | 1.94 (1.80-2.08, 6%) | 2.15 (1.84-2.71, 15% !) | 1.08x | 0.57x | 0.63x | 11.34 | 12.65 | 6.19 | 5.97 | 0.0 | 0.0 | 0 | 0 | 0 |
| fillsync ! | 189 (176-402, 41% !) | 222 (177-259, 15% !) | 185 (169-225, 11% !) | 197 (184-232, 10%) | 1.18x | 0.98x | 1.04x | 557 | 631 | 729 | 469 | 3.8 | 2.1 | 0 | 0 | 0 |

## num=300000, value=100B, threads=4, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 3.43 (3.36-3.96, 7%) | 3.40 (3.27-3.70, 5%) | 2.82 (2.66-3.21, 7%) | 2.89 (2.85-3.09, 4%) | 0.99x | 0.82x | 0.84x | 43.10 | 43.65 | 46.01 | 36.90 | 0.0 | 14.0 | 3 | 5 | 8 |
| fillrandom ! | 4.67 (4.20-5.47, 11% !) | 4.90 (4.76-5.55, 7%) | 3.98 (3.72-4.52, 7%) | 4.41 (3.98-4.59, 6%) | 1.05x | 0.85x | 0.94x | 66.46 | 64.47 | 48.93 | 52.91 | 0.0 | 12.1 | 0 | 1 | 3 |
| overwrite | 5.28 (5.18-6.09, 7%) | 5.66 (4.87-6.16, 8%) | 4.46 (4.42-4.91, 5%) | 4.63 (4.33-5.17, 7%) | 1.07x | 0.85x | 0.88x | 71.55 | 66.87 | 59.61 | 56.78 | 118.8 | 129.6 | 0 | 77 | 84 |
| flush ! | 94.6 ms (80.7 ms-119.0 ms, 16% !) | 92.9 ms (88.8 ms-108.5 ms, 8%) | 61.9 ms (61.7 ms-72.4 ms, 7%) | 63.2 ms (59.5 ms-68.2 ms, 6%) | 0.98x | 0.65x | 0.67x | - | - | - | - | 228016.0 | 302408.0 | 0 | 3 | 40 |
| readrandom ! | 2.00 (1.86-2.38, 9%) | 2.18 (1.97-2.32, 7%) | 1.25 (1.15-1.41, 8%) | 1.42 (1.31-2.03, 19% !) | 1.09x | 0.62x | 0.71x | 36.31 | 37.13 | 15.05 | 17.98 | 28.9 | 28.8 | 0 | 35 | 70 |
| readmissing ! | 0.19 (0.17-0.26, 20% !) | 0.30 (0.28-0.43, 19% !) | 0.15 (0.15-0.15, 2%) | 0.21 (0.17-0.23, 16% !) | 1.60x | 0.80x | 1.11x | 6.50 | 8.89 | 5.12 | 5.61 | 0.1 | 0.0 | 0 | 1 | 1 |
| seekrandom | 5.44 (5.13-5.64, 4%) | 5.98 (5.65-6.58, 6%) | 2.98 (2.70-3.34, 8%) | 3.09 (2.95-3.30, 4%) | 1.10x | 0.55x | 0.57x | 66.02 | 74.23 | 40.49 | 42.71 | 0.2 | 0.1 | 0 | 3 | 2 |
| readseq ! | 0.13 (0.12-0.16, 11% !) | 0.16 (0.15-0.17, 6%) | 0.08 (0.08-0.09, 5%) | 0.08 (0.08-0.09, 4%) | 1.24x | 0.61x | 0.62x | 4.97 | 5.06 | 4.09 | 4.44 | 0.2 | 0.1 | 0 | 1 | 1 |
| readreverse ! | 0.15 (0.14-0.17, 8%) | 0.18 (0.17-0.19, 5%) | 0.10 (0.09-0.11, 10% !) | 0.09 (0.09-0.11, 7%) | 1.21x | 0.67x | 0.65x | 4.82 | 4.97 | 3.28 | 3.83 | 0.3 | 0.1 | 0 | 0 | 0 |
| multireadrandom | 1.99 (1.82-2.09, 6%) | 2.04 (1.94-2.10, 3%) | 1.22 (1.15-1.33, 7%) | 1.18 (1.14-1.33, 7%) | 1.02x | 0.61x | 0.59x | 267 | 262 | 162 | 151 | 0.5 | 0.3 | 0 | 1 | 3 |
| readwhilewriting ! | 2.73 (2.44-3.19, 11% !) | 2.89 (2.75-3.58, 11% !) | 2.11 (1.69-2.25, 11% !) | 2.05 (1.31-2.17, 18% !) | 1.06x | 0.77x | 0.75x | 42.17 | 46.86 | 18.50 | 19.74 | 0.1 | 0.0 | 0 | 1 | 1 |
| compact ! | 387.4 ms (354.5 ms-443.2 ms, 10%) | 384.1 ms (377.3 ms-609.3 ms, 23% !) | 319.5 ms (305.2 ms-382.5 ms, 10%) | 285.5 ms (212.6 ms-337.7 ms, 16% !) | 0.99x | 0.82x | 0.74x | - | - | - | - | 6704640.0 | 7223904.0 | 0 | 32 | 42 |
| deleterandom ! | 3.99 (3.85-4.23, 4%) | 3.54 (3.37-5.39, 22% !) | 3.30 (3.17-3.50, 4%) | 3.59 (3.52-4.14, 7%) | 0.89x | 0.83x | 0.90x | 57.38 | 43.20 | 43.36 | 43.94 | 0.0 | 12.2 | 0 | 0 | 2 |
| fillsync ! | 134 (119-147, 7%) | 105 (89.69-163, 25% !) | 147 (122-174, 15% !) | 94.00 (88.85-130, 17% !) | 0.79x | 1.10x | 0.70x | 1704 | 1556 | 2152 | 1737 | 12.8 | 55.3 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 3.93 (3.48-4.46, 11% !) | 4.05 (3.69-4.39, 7%) | 2.55 (2.01-3.56, 25% !) | 2.28 (2.08-2.33, 5%) | 1.03x | 0.65x | 0.58x | 12.39 | 12.89 | 7.43 | 6.69 | 14.2 | 13.0 | 3 | 5 | 7 |
| fillrandom ! | 5.98 (5.75-6.63, 5%) | 6.10 (5.17-6.33, 8%) | 3.90 (3.80-4.99, 13% !) | 4.04 (3.38-4.69, 12% !) | 1.02x | 0.65x | 0.68x | 19.24 | 22.14 | 12.80 | 12.49 | 1.1 | 0.4 | 0 | 5 | 5 |
| overwrite ! | 6.41 (6.05-6.80, 4%) | 6.57 (6.31-7.21, 5%) | 4.51 (3.87-5.20, 11% !) | 4.77 (4.02-7.71, 28% !) | 1.03x | 0.70x | 0.74x | 22.60 | 26.45 | 12.96 | 13.82 | 15.6 | 17.6 | 0 | 5 | 7 |
| flush ! | 41.7 ms (35.8 ms-118.1 ms, 57% !) | 36.7 ms (34.3 ms-59.1 ms, 25% !) | 31.5 ms (29.1 ms-76.2 ms, 49% !) | 29.6 ms (27.1 ms-35.5 ms, 11% !) | 0.88x | 0.76x | 0.71x | - | - | - | - | 53872.0 | 55424.0 | 0 | 4 | 4 |
| readrandom ! | 4.73 (4.55-5.51, 8%) | 5.16 (4.50-7.45, 21% !) | 3.56 (3.05-3.70, 8%) | 3.42 (3.28-3.97, 8%) | 1.09x | 0.75x | 0.72x | 21.55 | 21.50 | 12.19 | 10.17 | 85.3 | 85.2 | 0 | 2 | 2 |
| readmissing ! | 0.70 (0.52-0.79, 16% !) | 0.57 (0.54-0.61, 5%) | 0.26 (0.24-0.36, 17% !) | 0.31 (0.30-0.42, 15% !) | 0.82x | 0.37x | 0.44x | 5.21 | 3.62 | 1.49 | 1.56 | 0.1 | 0.0 | 0 | 0 | 0 |
| seekrandom | 15.20 (15.08-15.25, 0%) | 15.50 (14.84-17.51, 7%) | 9.66 (9.28-11.29, 8%) | 10.15 (9.56-10.97, 5%) | 1.02x | 0.64x | 0.67x | 57.66 | 60.25 | 45.43 | 45.82 | 0.1 | 0.0 | 0 | 11 | 14 |
| readseq ! | 0.95 (0.89-1.17, 11% !) | 1.16 (1.01-1.37, 11% !) | 0.67 (0.63-0.76, 8%) | 0.65 (0.62-0.72, 6%) | 1.22x | 0.70x | 0.69x | 4.52 | 5.09 | 3.51 | 3.03 | 0.1 | 0.0 | 0 | 0 | 0 |
| readreverse ! | 1.27 (1.13-1.58, 14% !) | 1.30 (1.19-1.39, 6%) | 0.81 (0.70-1.16, 21% !) | 0.80 (0.72-0.82, 5%) | 1.02x | 0.63x | 0.63x | 5.49 | 5.72 | 3.56 | 3.40 | 0.1 | 0.0 | 0 | 1 | 2 |
| multireadrandom ! | 4.27 (3.67-4.47, 9%) | 4.16 (3.51-4.67, 11% !) | 3.07 (2.82-3.74, 12% !) | 3.21 (2.93-4.70, 20% !) | 0.98x | 0.72x | 0.75x | 139 | 168 | 123 | 127 | 0.4 | 0.4 | 0 | 0 | 0 |
| readwhilewriting ! | 7.26 (6.82-8.22, 8%) | 7.39 (6.60-8.24, 8%) | 5.99 (5.84-6.18, 2%) | 6.09 (5.63-7.41, 11% !) | 1.02x | 0.83x | 0.84x | 30.94 | 29.84 | 21.16 | 23.99 | 2.4 | 1.1 | 5 | 14 | 9 |
| compact ! | 554.2 ms (523.5 ms-600.3 ms, 6%) | 567.7 ms (461.9 ms-595.9 ms, 11% !) | 465.5 ms (388.5 ms-487.6 ms, 9%) | 511.3 ms (484.9 ms-548.5 ms, 4%) | 1.02x | 0.84x | 0.92x | - | - | - | - | 244168.0 | 1321784.0 | 0 | 42 | 41 |
| deleterandom ! | 2.81 (2.20-3.09, 14% !) | 2.98 (2.58-3.46, 12% !) | 1.73 (1.35-2.05, 18% !) | 1.54 (1.47-1.64, 5%) | 1.06x | 0.62x | 0.55x | 9.05 | 11.88 | 5.87 | 4.47 | 0.0 | 0.0 | 0 | 0 | 0 |
| fillsync ! | 226 (194-293, 16% !) | 233 (217-509, 42% !) | 216 (196-287, 16% !) | 217 (197-531, 50% !) | 1.03x | 0.96x | 0.96x | 1008 | 565 | 786 | 549 | 11.3 | 6.2 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 6.81 (5.80-7.10, 8%) | 6.47 (6.20-7.04, 6%) | 5.65 (5.27-6.44, 9%) | 4.73 (4.44-5.45, 8%) | 0.95x | 0.83x | 0.69x | 82.22 | 82.31 | 159 | 65.67 | 20.0 | 23.8 | 3 | 7 | 9 |
| fillrandom | 8.14 (7.20-9.22, 9%) | 7.61 (7.52-8.20, 4%) | 6.18 (5.94-6.63, 4%) | 5.56 (5.15-6.05, 6%) | 0.93x | 0.76x | 0.68x | 108 | 87.41 | 148 | 79.45 | 6.2 | 14.0 | 0 | 4 | 7 |
| overwrite ! | 7.90 (7.72-10.55, 14% !) | 7.99 (7.55-8.22, 3%) | 6.42 (5.97-7.38, 8%) | 6.24 (5.31-6.49, 8%) | 1.01x | 0.81x | 0.79x | 94.65 | 98.52 | 167 | 83.11 | 11.6 | 25.2 | 0 | 38 | 7 |
| flush ! | 39.1 ms (37.1 ms-46.8 ms, 10% !) | 41.1 ms (34.3 ms-56.8 ms, 20% !) | 36.2 ms (29.0 ms-42.6 ms, 13% !) | 30.4 ms (28.4 ms-32.0 ms, 5%) | 1.05x | 0.93x | 0.78x | - | - | - | - | 62808.0 | 55072.0 | 0 | 5 | 6 |
| readrandom ! | 2.40 (2.10-2.63, 9%) | 2.34 (2.07-2.70, 10% !) | 1.34 (1.25-1.47, 6%) | 1.74 (1.31-1.75, 13% !) | 0.98x | 0.56x | 0.72x | 39.11 | 40.74 | 16.71 | 21.63 | 85.9 | 85.7 | 0 | 2 | 2 |
| readmissing ! | 0.21 (0.19-0.23, 9%) | 0.22 (0.14-0.34, 36% !) | 0.14 (0.07-0.17, 29% !) | 0.17 (0.15-0.20, 10% !) | 1.07x | 0.65x | 0.82x | 3.48 | 5.05 | 2.28 | 2.82 | 0.2 | 0.1 | 0 | 0 | 1 |
| seekrandom | 7.36 (7.19-7.68, 3%) | 7.70 (7.62-8.02, 2%) | 4.67 (4.38-5.18, 7%) | 4.23 (3.99-4.42, 4%) | 1.05x | 0.63x | 0.58x | 84.34 | 88.59 | 55.27 | 53.69 | 0.3 | 0.2 | 0 | 12 | 15 |
| readseq ! | 0.44 (0.41-0.55, 12% !) | 0.45 (0.42-0.46, 4%) | 0.35 (0.34-0.38, 4%) | 0.36 (0.32-0.37, 5%) | 1.01x | 0.79x | 0.81x | 22.36 | 21.64 | 11.24 | 11.32 | 0.9 | 0.6 | 0 | 0 | 1 |
| readreverse ! | 0.45 (0.42-0.51, 9%) | 0.48 (0.44-0.59, 11% !) | 0.35 (0.35-0.36, 2%) | 0.35 (0.33-0.35, 3%) | 1.07x | 0.78x | 0.77x | 18.72 | 19.98 | 8.72 | 8.12 | 1.2 | 0.8 | 0 | 1 | 2 |
| multireadrandom ! | 1.78 (1.72-2.00, 7%) | 1.91 (1.83-2.03, 4%) | 1.29 (1.27-1.31, 1%) | 1.28 (1.17-1.55, 11% !) | 1.07x | 0.72x | 0.72x | 245 | 240 | 173 | 171 | 1.7 | 1.2 | 0 | 0 | 0 |
| readwhilewriting | 3.03 (2.85-3.17, 4%) | 3.10 (2.78-3.30, 7%) | 2.30 (1.99-2.41, 7%) | 2.32 (2.04-2.42, 8%) | 1.02x | 0.76x | 0.77x | 43.97 | 44.13 | 20.18 | 20.51 | 0.2 | 0.1 | 0 | 0 | 0 |
| compact ! | 521.6 ms (494.1 ms-596.6 ms, 7%) | 554.7 ms (499.8 ms-633.7 ms, 10% !) | 456.9 ms (439.4 ms-461.6 ms, 2%) | 442.0 ms (388.8 ms-472.2 ms, 8%) | 1.06x | 0.88x | 0.85x | - | - | - | - | 326728.0 | 1122664.0 | 0 | 13 | 46 |
| deleterandom ! | 3.92 (3.57-4.14, 6%) | 3.73 (3.37-4.46, 11% !) | 3.08 (2.75-3.46, 8%) | 3.35 (3.00-4.07, 13% !) | 0.95x | 0.79x | 0.85x | 58.51 | 47.99 | 38.18 | 37.44 | 0.0 | 14.1 | 0 | 2 | 3 |
| fillsync ! | 208 (126-290, 28% !) | 174 (155-270, 27% !) | 209 (174-231, 11% !) | 152 (122-331, 46% !) | 0.84x | 1.01x | 0.73x | 2206 | 2294 | 2238 | 1819 | 38.5 | 70.1 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 4.44 (3.69-4.87, 13% !) | 4.21 (3.92-5.09, 11% !) | 2.19 (1.90-2.48, 10% !) | 2.46 (2.15-2.62, 8%) | 0.95x | 0.49x | 0.55x | 14.10 | 14.76 | 6.74 | 7.89 | 0.8 | 5.7 | 4 | 4 | 5 |
| fillrandom | 6.42 (5.75-6.98, 7%) | 6.29 (5.46-6.90, 9%) | 3.72 (3.39-4.02, 7%) | 4.28 (3.78-4.33, 7%) | 0.98x | 0.58x | 0.67x | 23.30 | 20.57 | 12.75 | 13.58 | 2.0 | 1.5 | 0 | 0 | 0 |
| overwrite ! | 6.30 (5.43-6.76, 10%) | 6.09 (5.59-7.34, 11% !) | 4.11 (3.56-4.30, 7%) | 4.53 (3.97-5.17, 12% !) | 0.97x | 0.65x | 0.72x | 21.25 | 21.28 | 13.68 | 15.01 | 2.5 | 1.8 | 0 | 4 | 3 |
| flush ! | 56.2 ms (42.7 ms-62.9 ms, 16% !) | 50.7 ms (42.9 ms-55.3 ms, 11% !) | 36.9 ms (36.1 ms-46.1 ms, 11% !) | 39.4 ms (35.9 ms-46.4 ms, 10%) | 0.90x | 0.66x | 0.70x | - | - | - | - | 77248.0 | 53088.0 | 0 | 7 | 2 |
| readrandom | 4.81 (4.31-5.49, 9%) | 4.79 (4.63-5.24, 6%) | 3.36 (3.16-3.86, 8%) | 4.16 (3.84-4.65, 8%) | 0.99x | 0.70x | 0.86x | 18.97 | 18.70 | 11.51 | 14.80 | 95.0 | 95.0 | 0 | 6 | 18 |
| readmissing ! | 0.49 (0.47-0.55, 7%) | 0.54 (0.51-0.70, 14% !) | 0.26 (0.25-0.31, 11% !) | 0.31 (0.29-0.38, 11% !) | 1.09x | 0.52x | 0.63x | 2.53 | 3.10 | 1.17 | 1.44 | 0.1 | 0.0 | 0 | 0 | 0 |
| seekrandom | 15.44 (14.33-17.45, 8%) | 15.70 (15.07-15.97, 3%) | 10.48 (10.07-10.77, 3%) | 11.31 (10.77-12.31, 5%) | 1.02x | 0.68x | 0.73x | 57.84 | 55.54 | 44.17 | 48.40 | 0.1 | 0.0 | 1 | 4 | 6 |
| readseq ! | 1.10 (0.92-1.44, 17% !) | 1.36 (1.05-1.54, 17% !) | 0.72 (0.69-0.75, 3%) | 0.76 (0.68-0.94, 14% !) | 1.24x | 0.65x | 0.70x | 6.54 | 6.34 | 3.51 | 3.83 | 0.1 | 0.0 | 0 | 0 | 0 |
| readreverse ! | 1.25 (1.16-1.43, 9%) | 1.51 (1.19-1.66, 13% !) | 0.83 (0.78-0.85, 4%) | 0.85 (0.83-0.90, 4%) | 1.21x | 0.66x | 0.68x | 6.14 | 7.44 | 3.78 | 4.00 | 0.1 | 0.0 | 0 | 0 | 1 |
| multireadrandom ! | 3.92 (3.73-4.34, 6%) | 4.42 (3.72-5.25, 12% !) | 3.32 (3.01-3.47, 5%) | 3.62 (3.60-4.95, 15% !) | 1.13x | 0.85x | 0.92x | 140 | 162 | 130 | 135 | 0.5 | 0.4 | 0 | 0 | 0 |
| readwhilewriting ! | 8.63 (7.02-8.73, 11% !) | 7.77 (6.83-8.18, 7%) | 6.85 (6.54-7.68, 7%) | 6.43 (6.18-7.73, 10%) | 0.90x | 0.79x | 0.75x | 36.60 | 32.85 | 30.55 | 23.27 | 3.5 | 1.1 | 5 | 18 | 19 |
| compact ! | 621.9 ms (543.3 ms-649.2 ms, 7%) | 528.9 ms (473.9 ms-611.0 ms, 10%) | 565.4 ms (503.7 ms-665.3 ms, 11% !) | 572.8 ms (464.2 ms-719.4 ms, 17% !) | 0.85x | 0.91x | 0.92x | - | - | - | - | 1769208.0 | 3366280.0 | 0 | 5 | 18 |
| deleterandom ! | 2.76 (2.35-3.36, 13% !) | 2.92 (2.56-4.28, 22% !) | 1.51 (1.35-1.58, 6%) | 1.64 (1.39-1.96, 12% !) | 1.06x | 0.55x | 0.60x | 10.00 | 10.24 | 3.99 | 5.22 | 0.0 | 0.0 | 0 | 1 | 0 |
| fillsync ! | 291 (230-367, 19% !) | 224 (184-258, 12% !) | 253 (166-258, 17% !) | 254 (218-305, 15% !) | 0.77x | 0.87x | 0.87x | 1147 | 631 | 1196 | 1196 | 11.3 | 6.2 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=4, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 6.70 (6.12-7.19, 6%) | 6.65 (6.41-7.19, 5%) | 5.06 (4.71-5.54, 7%) | 4.16 (4.00-4.24, 2%) | 0.99x | 0.76x | 0.62x | 79.92 | 90.85 | 129 | 58.75 | 22.4 | 26.3 | 3 | 8 | 6 |
| fillrandom | 7.94 (6.87-8.73, 10%) | 7.90 (7.26-8.39, 7%) | 6.55 (6.35-7.10, 5%) | 5.88 (5.38-6.66, 9%) | 1.00x | 0.83x | 0.74x | 93.82 | 91.89 | 185 | 85.80 | 2.1 | 12.4 | 0 | 4 | 4 |
| overwrite | 7.43 (6.89-8.33, 7%) | 8.69 (7.69-8.86, 6%) | 6.67 (6.24-7.46, 8%) | 5.92 (5.50-6.22, 4%) | 1.17x | 0.90x | 0.80x | 91.07 | 108 | 180 | 85.07 | 7.8 | 12.8 | 0 | 3 | 37 |
| flush ! | 48.5 ms (41.4 ms-54.7 ms, 12% !) | 49.3 ms (42.3 ms-55.0 ms, 11% !) | 38.1 ms (36.3 ms-49.3 ms, 15% !) | 49.5 ms (35.7 ms-64.0 ms, 23% !) | 1.02x | 0.79x | 1.02x | - | - | - | - | 55912.0 | 55048.0 | 0 | 4 | 8 |
| readrandom ! | 2.73 (2.26-2.76, 8%) | 2.80 (2.71-3.03, 4%) | 1.66 (1.37-1.69, 9%) | 1.57 (1.50-1.92, 12% !) | 1.03x | 0.61x | 0.57x | 41.43 | 44.48 | 18.09 | 19.60 | 95.5 | 95.5 | 0 | 5 | 4 |
| readmissing ! | 0.17 (0.14-0.20, 14% !) | 0.24 (0.22-0.31, 15% !) | 0.14 (0.08-0.18, 27% !) | 0.17 (0.15-0.19, 10% !) | 1.38x | 0.84x | 0.99x | 3.52 | 5.18 | 2.53 | 3.03 | 0.4 | 0.1 | 0 | 0 | 2 |
| seekrandom | 7.85 (7.17-8.32, 6%) | 8.77 (8.38-9.28, 4%) | 4.71 (4.33-5.04, 5%) | 4.85 (4.61-5.14, 4%) | 1.12x | 0.60x | 0.62x | 84.77 | 93.33 | 57.09 | 55.81 | 0.6 | 0.2 | 0 | 6 | 7 |
| readseq ! | 0.47 (0.44-0.50, 6%) | 0.49 (0.48-0.61, 13% !) | 0.36 (0.35-0.38, 3%) | 0.36 (0.35-0.38, 3%) | 1.03x | 0.77x | 0.76x | 24.23 | 24.01 | 11.47 | 11.10 | 1.0 | 0.7 | 0 | 0 | 1 |
| readreverse ! | 0.43 (0.42-0.46, 4%) | 0.48 (0.46-0.53, 5%) | 0.37 (0.35-0.38, 3%) | 0.38 (0.34-0.64, 29% !) | 1.14x | 0.86x | 0.90x | 18.62 | 20.47 | 8.87 | 7.60 | 1.4 | 0.6 | 0 | 0 | 0 |
| multireadrandom | 2.05 (1.96-2.31, 7%) | 2.09 (1.94-2.35, 7%) | 1.41 (1.36-1.58, 6%) | 1.51 (1.36-1.54, 6%) | 1.02x | 0.69x | 0.73x | 306 | 258 | 186 | 204 | 1.9 | 1.1 | 0 | 0 | 0 |
| readwhilewriting | 3.07 (2.98-3.37, 5%) | 3.26 (2.91-3.41, 7%) | 2.27 (2.20-2.75, 10%) | 2.38 (2.11-2.49, 7%) | 1.06x | 0.74x | 0.78x | 42.30 | 44.86 | 23.38 | 24.00 | 0.5 | 0.1 | 0 | 0 | 1 |
| compact | 522.5 ms (501.4 ms-571.4 ms, 5%) | 571.2 ms (532.7 ms-591.9 ms, 4%) | 504.6 ms (496.2 ms-567.7 ms, 6%) | 470.0 ms (442.8 ms-557.7 ms, 9%) | 1.09x | 0.97x | 0.90x | - | - | - | - | 2747736.0 | 3367472.0 | 0 | 10 | 38 |
| deleterandom ! | 3.12 (2.98-3.70, 10%) | 3.66 (3.14-4.03, 9%) | 2.94 (2.78-3.64, 11% !) | 3.28 (2.81-3.35, 7%) | 1.17x | 0.94x | 1.05x | 38.32 | 46.93 | 33.87 | 40.41 | 0.0 | 12.8 | 0 | 0 | 1 |
| fillsync ! | 156 (135-225, 20% !) | 181 (154-424, 49% !) | 184 (173-198, 5%) | 158 (123-174, 14% !) | 1.16x | 1.17x | 1.01x | 1917 | 2261 | 1901 | 2114 | 38.5 | 70.9 | 0 | 0 | 1 |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 2.53 (2.35-2.73, 6%) | 2.75 (2.56-2.90, 5%) | 1.64 (1.45-1.77, 7%) | 1.78 (1.68-1.88, 5%) | 1.09x | 0.65x | 0.70x | 7.92 | 10.43 | 5.42 | 6.07 | 4.8 | 4.7 | 3 | 88 | 167 |
| overwrite | 2.68 (2.36-2.90, 8%) | 2.76 (2.61-2.98, 5%) | 1.81 (1.62-1.88, 7%) | 1.67 (1.62-1.77, 4%) | 1.03x | 0.68x | 0.62x | 8.61 | 9.50 | 6.78 | 5.80 | 10.4 | 10.4 | 1 | 635 | 467 |
| readrandom | 7.04 (6.33-7.40, 7%) | 7.35 (6.56-7.94, 7%) | 4.65 (4.36-5.02, 6%) | 5.16 (4.65-5.34, 6%) | 1.04x | 0.66x | 0.73x | 23.24 | 32.34 | 16.16 | 17.08 | 18.9 | 18.6 | 0 | 9 | 76 |
| seekrandom | 26.39 (25.76-27.67, 3%) | 28.29 (24.67-29.22, 7%) | 10.44 (9.71-11.65, 9%) | 10.07 (8.87-10.63, 8%) | 1.07x | 0.40x | 0.38x | 79.42 | 82.12 | 43.15 | 42.40 | 0.0 | 0.0 | 0 | 6 | 13 |
| waitforcompaction ! | 27.43 (24.59-37.20, 17% !) | 38.09 (32.33-41.13, 9%) | 27.93 (23.51-28.85, 8%) | 25.44 (24.23-32.21, 12% !) | 1.39x | 1.02x | 0.93x | - | - | - | - | 40.0 | 40.0 | 0 | 0 | 0 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| num=300000, value=100B, threads=1, compression=none | fillseq | c-native | 16.4% | 1.86 | 1.65-2.47 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillrandom | managed-async | 10.6% | 3.24 | 2.84-3.75 | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | c-native | 12.5% | 97.9 ms | 94.8 ms-123.7 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | managed | 13.4% | 61.9 ms | 60.2 ms-80.7 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | managed-async | 13.3% | 69.8 ms | 59.0 ms-84.0 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | readmissing | managed | 19.1% | 0.33 | 0.30-0.47 | 5 |
| num=300000, value=100B, threads=1, compression=none | readmissing | managed-async | 16.7% | 0.36 | 0.36-0.51 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | c-native | 15.4% | 0.41 | 0.39-0.53 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | rocksdb-sharp | 10.5% | 0.56 | 0.49-0.66 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | managed | 21.6% | 0.25 | 0.22-0.36 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | c-native | 10.6% | 0.50 | 0.49-0.62 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | managed | 19.1% | 0.32 | 0.26-0.43 | 5 |
| num=300000, value=100B, threads=1, compression=none | compact | c-native | 10.9% | 546.5 ms | 483.1 ms-641.3 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | compact | rocksdb-sharp | 10.9% | 538.4 ms | 498.4 ms-655.4 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | deleterandom | rocksdb-sharp | 15.0% | 3.44 | 3.05-4.39 | 5 |
| num=300000, value=100B, threads=1, compression=none | deleterandom | managed | 14.6% | 1.94 | 1.85-2.60 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | c-native | 11.0% | 185 | 160-213 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | rocksdb-sharp | 28.7% | 208 | 131-248 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | managed | 10.5% | 190 | 165-213 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | managed-async | 21.5% | 202 | 127-231 | 5 |
| num=300000, value=100B, threads=4, compression=none | overwrite | managed-async | 11.7% | 4.49 | 4.04-5.47 | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | c-native | 31.8% | 84.7 ms | 77.6 ms-153.8 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | managed | 10.9% | 66.0 ms | 55.2 ms-69.8 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | managed-async | 11.6% | 55.5 ms | 47.1 ms-64.7 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | c-native | 17.9% | 0.25 | 0.21-0.33 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | rocksdb-sharp | 21.5% | 0.25 | 0.19-0.30 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | managed | 16.6% | 0.21 | 0.17-0.25 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | managed-async | 12.4% | 0.20 | 0.16-0.22 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | c-native | 10.8% | 0.13 | 0.12-0.16 | 5 |
| num=300000, value=100B, threads=4, compression=none | readreverse | rocksdb-sharp | 12.4% | 0.16 | 0.15-0.20 | 5 |
| num=300000, value=100B, threads=4, compression=none | readwhilewriting | rocksdb-sharp | 11.3% | 2.64 | 2.32-3.14 | 5 |
| num=300000, value=100B, threads=4, compression=none | readwhilewriting | managed-async | 13.3% | 2.06 | 1.60-2.28 | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | c-native | 10.9% | 383.1 ms | 335.0 ms-439.2 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | rocksdb-sharp | 12.3% | 347.7 ms | 327.4 ms-429.9 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | managed-async | 15.8% | 287.2 ms | 242.8 ms-365.6 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | c-native | 18.1% | 119 | 81.96-125 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | rocksdb-sharp | 22.4% | 115 | 103-174 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | managed | 11.2% | 126 | 111-144 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillseq | rocksdb-sharp | 10.0% | 2.01 | 1.86-2.34 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillseq | managed | 17.6% | 1.19 | 0.94-1.45 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillrandom | c-native | 12.2% | 4.37 | 4.24-5.59 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillrandom | rocksdb-sharp | 11.1% | 5.11 | 4.23-5.44 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillrandom | managed | 13.9% | 2.92 | 2.74-3.80 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | overwrite | c-native | 10.1% | 4.99 | 4.21-5.43 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | managed | 14.4% | 71.1 ms | 67.8 ms-94.3 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | managed-async | 12.5% | 69.7 ms | 67.1 ms-89.8 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readrandom | managed | 12.4% | 3.29 | 2.95-4.08 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | managed | 13.5% | 0.33 | 0.31-0.43 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | managed-async | 21.0% | 0.37 | 0.36-0.57 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | seekrandom | rocksdb-sharp | 12.2% | 14.86 | 12.37-17.43 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | c-native | 15.8% | 0.46 | 0.43-0.58 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | rocksdb-sharp | 10.0% | 0.58 | 0.55-0.70 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | managed | 20.5% | 0.25 | 0.24-0.37 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | c-native | 22.7% | 0.54 | 0.51-0.81 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | managed | 15.2% | 0.36 | 0.32-0.46 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | managed-async | 10.7% | 0.31 | 0.31-0.39 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | multireadrandom | rocksdb-sharp | 10.1% | 4.22 | 3.76-4.79 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | multireadrandom | managed | 12.4% | 2.93 | 2.79-3.75 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | multireadrandom | managed-async | 11.1% | 3.23 | 2.83-3.68 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readwhilewriting | managed | 11.0% | 5.24 | 4.44-5.93 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | compact | c-native | 10.6% | 534.7 ms | 482.6 ms-631.1 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | compact | managed | 15.5% | 450.6 ms | 411.6 ms-602.9 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | compact | managed-async | 23.8% | 437.0 ms | 253.0 ms-518.8 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | deleterandom | managed-async | 14.7% | 2.15 | 1.84-2.71 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | c-native | 40.7% | 189 | 176-402 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | rocksdb-sharp | 15.1% | 222 | 177-259 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | managed | 10.8% | 185 | 169-225 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillrandom | c-native | 11.2% | 4.67 | 4.20-5.47 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | flush | c-native | 16.2% | 94.6 ms | 80.7 ms-119.0 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readrandom | managed-async | 19.1% | 1.42 | 1.31-2.03 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | c-native | 20.2% | 0.19 | 0.17-0.26 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | rocksdb-sharp | 18.6% | 0.30 | 0.28-0.43 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | managed-async | 15.9% | 0.21 | 0.17-0.23 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readseq | c-native | 11.4% | 0.13 | 0.12-0.16 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readreverse | managed | 10.5% | 0.10 | 0.09-0.11 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | c-native | 10.5% | 2.73 | 2.44-3.19 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | rocksdb-sharp | 11.0% | 2.89 | 2.75-3.58 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | managed | 10.9% | 2.11 | 1.69-2.25 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | managed-async | 18.4% | 2.05 | 1.31-2.17 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | compact | rocksdb-sharp | 23.1% | 384.1 ms | 377.3 ms-609.3 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | compact | managed-async | 16.1% | 285.5 ms | 212.6 ms-337.7 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | deleterandom | rocksdb-sharp | 22.2% | 3.54 | 3.37-5.39 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | rocksdb-sharp | 25.4% | 105 | 89.69-163 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | managed | 15.0% | 147 | 122-174 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | managed-async | 16.9% | 94.00 | 88.85-130 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillseq | c-native | 10.8% | 3.93 | 3.48-4.46 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillseq | managed | 25.5% | 2.55 | 2.01-3.56 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillrandom | managed | 12.8% | 3.90 | 3.80-4.99 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillrandom | managed-async | 12.1% | 4.04 | 3.38-4.69 | 5 |
| num=100000, value=1024B, threads=1, compression=none | overwrite | managed | 11.3% | 4.51 | 3.87-5.20 | 5 |
| num=100000, value=1024B, threads=1, compression=none | overwrite | managed-async | 28.3% | 4.77 | 4.02-7.71 | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | c-native | 57.0% | 41.7 ms | 35.8 ms-118.1 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | rocksdb-sharp | 24.7% | 36.7 ms | 34.3 ms-59.1 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | managed | 49.2% | 31.5 ms | 29.1 ms-76.2 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | managed-async | 11.3% | 29.6 ms | 27.1 ms-35.5 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | readrandom | rocksdb-sharp | 21.0% | 5.16 | 4.50-7.45 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | c-native | 15.7% | 0.70 | 0.52-0.79 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | managed | 17.4% | 0.26 | 0.24-0.36 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | managed-async | 14.6% | 0.31 | 0.30-0.42 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | c-native | 11.3% | 0.95 | 0.89-1.17 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | rocksdb-sharp | 11.4% | 1.16 | 1.01-1.37 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readreverse | c-native | 14.0% | 1.27 | 1.13-1.58 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readreverse | managed | 21.1% | 0.81 | 0.70-1.16 | 5 |
| num=100000, value=1024B, threads=1, compression=none | multireadrandom | rocksdb-sharp | 11.2% | 4.16 | 3.51-4.67 | 5 |
| num=100000, value=1024B, threads=1, compression=none | multireadrandom | managed | 12.1% | 3.07 | 2.82-3.74 | 5 |
| num=100000, value=1024B, threads=1, compression=none | multireadrandom | managed-async | 20.4% | 3.21 | 2.93-4.70 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readwhilewriting | managed-async | 11.4% | 6.09 | 5.63-7.41 | 5 |
| num=100000, value=1024B, threads=1, compression=none | compact | rocksdb-sharp | 11.2% | 567.7 ms | 461.9 ms-595.9 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | c-native | 14.1% | 2.81 | 2.20-3.09 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | rocksdb-sharp | 12.1% | 2.98 | 2.58-3.46 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | managed | 18.4% | 1.73 | 1.35-2.05 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | c-native | 16.4% | 226 | 194-293 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | rocksdb-sharp | 41.5% | 233 | 217-509 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | managed | 15.9% | 216 | 196-287 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | managed-async | 49.9% | 217 | 197-531 | 5 |
| num=100000, value=1024B, threads=4, compression=none | overwrite | c-native | 14.2% | 7.90 | 7.72-10.55 | 5 |
| num=100000, value=1024B, threads=4, compression=none | flush | c-native | 10.0% | 39.1 ms | 37.1 ms-46.8 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | flush | rocksdb-sharp | 20.2% | 41.1 ms | 34.3 ms-56.8 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | flush | managed | 13.5% | 36.2 ms | 29.0 ms-42.6 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | readrandom | rocksdb-sharp | 10.2% | 2.34 | 2.07-2.70 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readrandom | managed-async | 13.1% | 1.74 | 1.31-1.75 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | rocksdb-sharp | 35.9% | 0.22 | 0.14-0.34 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | managed | 28.5% | 0.14 | 0.07-0.17 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | managed-async | 10.2% | 0.17 | 0.15-0.20 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readseq | c-native | 12.0% | 0.44 | 0.41-0.55 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readreverse | rocksdb-sharp | 11.0% | 0.48 | 0.44-0.59 | 5 |
| num=100000, value=1024B, threads=4, compression=none | multireadrandom | managed-async | 11.3% | 1.28 | 1.17-1.55 | 5 |
| num=100000, value=1024B, threads=4, compression=none | compact | rocksdb-sharp | 10.1% | 554.7 ms | 499.8 ms-633.7 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | rocksdb-sharp | 10.9% | 3.73 | 3.37-4.46 | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | managed-async | 13.1% | 3.35 | 3.00-4.07 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | c-native | 28.4% | 208 | 126-290 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | rocksdb-sharp | 27.5% | 174 | 155-270 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | managed | 10.9% | 209 | 174-231 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | managed-async | 46.0% | 152 | 122-331 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillseq | c-native | 12.9% | 4.44 | 3.69-4.87 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillseq | rocksdb-sharp | 10.9% | 4.21 | 3.92-5.09 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillseq | managed | 10.2% | 2.19 | 1.90-2.48 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | overwrite | rocksdb-sharp | 11.2% | 6.09 | 5.59-7.34 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | overwrite | managed-async | 11.6% | 4.53 | 3.97-5.17 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | c-native | 16.4% | 56.2 ms | 42.7 ms-62.9 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | rocksdb-sharp | 10.7% | 50.7 ms | 42.9 ms-55.3 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | managed | 10.9% | 36.9 ms | 36.1 ms-46.1 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | rocksdb-sharp | 13.8% | 0.54 | 0.51-0.70 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | managed | 11.5% | 0.26 | 0.25-0.31 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | managed-async | 10.9% | 0.31 | 0.29-0.38 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | c-native | 17.3% | 1.10 | 0.92-1.44 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | rocksdb-sharp | 17.0% | 1.36 | 1.05-1.54 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | managed-async | 13.7% | 0.76 | 0.68-0.94 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | rocksdb-sharp | 13.2% | 1.51 | 1.19-1.66 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | rocksdb-sharp | 12.3% | 4.42 | 3.72-5.25 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | managed-async | 14.8% | 3.62 | 3.60-4.95 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readwhilewriting | c-native | 11.1% | 8.63 | 7.02-8.73 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | compact | managed | 11.3% | 565.4 ms | 503.7 ms-665.3 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | compact | managed-async | 16.8% | 572.8 ms | 464.2 ms-719.4 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | deleterandom | c-native | 13.1% | 2.76 | 2.35-3.36 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | deleterandom | rocksdb-sharp | 22.1% | 2.92 | 2.56-4.28 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | deleterandom | managed-async | 12.3% | 1.64 | 1.39-1.96 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | c-native | 19.2% | 291 | 230-367 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | rocksdb-sharp | 11.8% | 224 | 184-258 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | managed | 16.6% | 253 | 166-258 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | managed-async | 14.6% | 254 | 218-305 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | c-native | 11.8% | 48.5 ms | 41.4 ms-54.7 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | rocksdb-sharp | 11.3% | 49.3 ms | 42.3 ms-55.0 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | managed | 15.3% | 38.1 ms | 36.3 ms-49.3 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | managed-async | 22.6% | 49.5 ms | 35.7 ms-64.0 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readrandom | managed-async | 12.0% | 1.57 | 1.50-1.92 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | c-native | 13.7% | 0.17 | 0.14-0.20 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | rocksdb-sharp | 14.8% | 0.24 | 0.22-0.31 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | managed | 27.1% | 0.14 | 0.08-0.18 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | managed-async | 10.3% | 0.17 | 0.15-0.19 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readseq | rocksdb-sharp | 13.1% | 0.49 | 0.48-0.61 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readreverse | managed-async | 28.5% | 0.38 | 0.34-0.64 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | deleterandom | managed | 11.4% | 2.94 | 2.78-3.64 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | c-native | 20.3% | 156 | 135-225 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | rocksdb-sharp | 48.8% | 181 | 154-424 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | managed-async | 14.3% | 158 | 123-174 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | waitforcompaction | c-native | 16.5% | 27.43 | 24.59-37.20 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | waitforcompaction | managed-async | 12.4% | 25.44 | 24.23-32.21 | 5 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

Over the repetitions: write-amp median (min-max), and the most frequent LSM shape with the number of repetitions that had it.

| configuration | engine | runs | write-amp median (min-max) | written MB median | most frequent files per level (L0..L6) |
|---|---|---:|---:|---:|---|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 14` (4/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 15` (5/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 5 | 3.15 (3.05-3.15) | 348.2 | `1 0 0 0 0 4 15` (1/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 5 | 3.15 (3.15-3.15) | 348.2 | `0 0 0 0 0 4 15` (2/5) |

Every repetition:

| configuration | engine | rep | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---:|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 1 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 1 | `1 0 0 0 0 4 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 1 | `0 0 0 0 0 5 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 2 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 2 | `1 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 2 | `0 0 0 0 0 6 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 3 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 3 | `0 0 0 0 0 4 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 3 | `0 0 0 0 0 4 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 4 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 4 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 4 | `0 0 0 0 0 6 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 4 | `0 0 0 0 0 3 16` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 5 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 5 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 5 | `0 0 0 0 0 5 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 5 | `0 0 0 0 0 4 15` | 110.6 | 348.2 | 3.15 | 0 |

Raw data: `2026-11-03-definitive-r5.csv` `2026-11-03-definitive-r5.lsm.csv` 
