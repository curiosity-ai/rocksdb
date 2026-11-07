# RocksDB engine comparison (final, 5 repetitions, warm): C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 19:50 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 0.51 1.10 1.46
- Repetitions: 5; UTC time and load average (1/5/15 min) at the start of each: rep 1 19:50 0.51 1.10 1.46; rep 2 20:09 1.20 1.63 1.66; rep 3 20:28 1.21 1.69 1.74; rep 4 20:47 1.41 1.86 1.88; rep 5 21:07 1.28 1.73 1.84
- 1-minute load average before each of the 45 engine-set runs: min 0.51, median 1.65, max 3.36
- Load average at end: 1.23 1.60 1.74; total runtime 96 min 53 s

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

Final statistically repeated run: REPEATS=5, LOAD_MAX=1.5 (each repetition waits until the 1-minute load average is <= 1.5), quiet machine (no other agents, nothing compiled during the measurements). Release build, warm-up on.

Supplement: in repetition 1 the managed (sync) process of the configuration value=1024B, threads=1, compression=lz4 crashed during its warm-up (`RocksDatabase.Open` of the freshly re-created warm-up DB failed with "IO error: While open a file: .../managed-<pid>-warmup/MANIFEST-000001: Could not find file", from the warm-up's destroy + reopen before a fill benchmark), so those 14 rows were missing. They were re-measured after the BenchmarkDotNet runs (22:10 UTC, load 0.69, same build, same flags, `compare --engines managed --rep 1`), i.e. outside the A B C D interleaving of the other repetitions. The other 44 engine-set runs completed normally.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 5 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| num=300000, value=100B, threads=1, compression=none | 1.06x [1.01-1.13] (14) | 0.59x [0.56-0.63] (14) | 0.64x [0.61-0.68] (14) |
| num=300000, value=100B, threads=4, compression=none | 1.01x [0.96-1.09] (14) | 0.78x [0.72-0.82] (14) | 0.79x [0.74-0.83] (14) |
| num=300000, value=100B, threads=1, compression=lz4 | 1.11x [1.07-1.16] (14) | 0.69x [0.66-0.73] (14) | 0.76x [0.72-0.79] (14) |
| num=300000, value=100B, threads=4, compression=lz4 | 1.01x [0.97-1.09] (14) | 0.83x [0.77-0.87] (14) | 0.78x [0.73-0.81] (14) |
| num=100000, value=1024B, threads=1, compression=none | 1.04x [0.96-1.19] (14) | 0.52x [0.49-0.57] (14) | 0.55x [0.53-0.61] (14) |
| num=100000, value=1024B, threads=4, compression=none | 1.03x [0.96-1.08] (14) | 0.63x [0.58-0.66] (14) | 0.60x [0.56-0.63] (14) |
| num=100000, value=1024B, threads=1, compression=lz4 | 0.99x [0.94-1.07] (14) | 0.58x [0.55-0.62] (14) | 0.64x [0.60-0.68] (14) |
| num=100000, value=1024B, threads=4, compression=lz4 | 1.02x [0.94-1.11] (14) | 0.70x [0.65-0.75] (14) | 0.74x [0.68-0.79] (14) |
| compaction: num=500000, value=100B, threads=1, compression=none | 1.07x [1.00-1.13] (4) | 0.58x [0.51-0.63] (4) | 0.69x [0.64-0.72] (4) |
| **all matrix configurations** | **1.03x [1.01-1.06] (112)** | **0.66x [0.64-0.67] (112)** | **0.68x [0.67-0.70] (112)** |

### Per benchmark (geometric mean over the 8 matrix configurations of the median ratio vs c-native)

| benchmark | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
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

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| rocksdb-sharp | 585 | 64 | 0 | 3 (flush, num=300000, value=100B, threads=4, compression=lz4, rep 4) |
| managed | 585 | 6454 | 1 | 769 (overwrite, compaction: num=500000, value=100B, threads=1, compression=none, rep 2) |
| managed-async | 585 | 6411 | 1 | 505 (overwrite, compaction: num=500000, value=100B, threads=1, compression=none, rep 1) |

## num=300000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 3.50 (3.14-4.66, 18% !) | 3.63 (3.55-3.89, 4%) | 1.66 (1.54-1.98, 11% !) | 1.84 (1.72-1.91, 5%) | 1.04x | 0.47x | 0.53x | 11.18 | 11.00 | 5.76 | 6.13 | 0.2 | 0.2 | 1 | 1 | 2 |
| fillrandom ! | 7.12 (6.20-9.11, 15% !) | 6.91 (6.89-7.66, 5%) | 4.15 (3.98-4.32, 3%) | 4.45 (4.31-4.66, 3%) | 0.97x | 0.58x | 0.62x | 23.83 | 30.38 | 12.06 | 13.65 | 0.2 | 0.2 | 0 | 0 | 0 |
| overwrite ! | 7.68 (6.89-10.20, 16% !) | 7.91 (7.06-8.62, 8%) | 4.82 (4.38-5.33, 7%) | 4.88 (4.68-5.55, 7%) | 1.03x | 0.63x | 0.64x | 35.53 | 35.07 | 15.60 | 14.46 | 113.8 | 92.4 | 0 | 11 | 9 |
| flush ! | 138.1 ms (112.4 ms-143.3 ms, 9%) | 127.5 ms (123.1 ms-158.4 ms, 11% !) | 92.3 ms (84.6 ms-98.2 ms, 6%) | 96.4 ms (89.1 ms-105.9 ms, 8%) | 0.92x | 0.67x | 0.70x | - | - | - | - | 248640.0 | 183632.0 | 0 | 4 | 4 |
| readrandom ! | 7.70 (6.65-8.38, 10%) | 8.05 (7.61-8.26, 3%) | 4.59 (4.17-5.45, 12% !) | 5.41 (5.22-5.60, 3%) | 1.05x | 0.60x | 0.70x | 31.93 | 36.67 | 12.39 | 16.31 | 31.5 | 31.4 | 0 | 38 | 73 |
| readmissing ! | 1.25 (1.03-2.28, 37% !) | 1.28 (1.15-1.48, 10% !) | 0.49 (0.42-0.59, 13% !) | 0.86 (0.81-0.92, 5%) | 1.02x | 0.39x | 0.69x | 9.17 | 10.12 | 5.73 | 6.64 | 0.2 | 0.2 | 0 | 0 | 0 |
| seekrandom | 23.00 (19.18-23.18, 9%) | 24.36 (22.39-25.57, 5%) | 11.96 (11.28-12.85, 6%) | 12.90 (12.22-14.25, 6%) | 1.06x | 0.52x | 0.56x | 76.66 | 79.82 | 49.35 | 50.88 | 0.2 | 0.2 | 0 | 2 | 8 |
| readseq ! | 0.66 (0.61-0.90, 18% !) | 0.90 (0.75-1.13, 16% !) | 0.34 (0.32-0.49, 19% !) | 0.33 (0.32-0.47, 18% !) | 1.37x | 0.51x | 0.51x | 6.78 | 8.43 | 4.27 | 4.24 | 0.2 | 0.2 | 0 | 2 | 0 |
| readreverse ! | 0.79 (0.69-0.81, 6%) | 1.09 (0.89-1.27, 14% !) | 0.54 (0.42-0.58, 14% !) | 0.46 (0.38-0.59, 18% !) | 1.39x | 0.68x | 0.58x | 7.26 | 9.91 | 5.76 | 4.88 | 0.3 | 0.2 | 0 | 0 | 0 |
| multireadrandom ! | 5.78 (5.33-7.39, 13% !) | 6.19 (5.63-6.72, 7%) | 4.35 (4.08-5.50, 12% !) | 4.86 (4.36-5.13, 8%) | 1.07x | 0.75x | 0.84x | 209 | 215 | 148 | 168 | 0.3 | 0.3 | 0 | 0 | 0 |
| readwhilewriting ! | 9.44 (8.49-10.00, 7%) | 9.68 (8.05-11.07, 12% !) | 6.21 (5.50-6.73, 7%) | 6.72 (6.47-7.02, 3%) | 1.03x | 0.66x | 0.71x | 44.03 | 47.38 | 22.35 | 24.99 | 0.3 | 0.3 | 0 | 0 | 0 |
| compact ! | 1162.9 ms (1110.7 ms-1325.1 ms, 7%) | 1078.9 ms (988.8 ms-1407.5 ms, 16% !) | 560.5 ms (515.2 ms-672.3 ms, 13% !) | 594.4 ms (567.3 ms-658.9 ms, 6%) | 0.93x | 0.48x | 0.51x | - | - | - | - | 7366840.0 | 7690848.0 | 0 | 17 | 26 |
| deleterandom | 5.78 (5.64-6.05, 3%) | 5.84 (5.48-6.38, 7%) | 3.23 (2.88-3.37, 6%) | 3.43 (3.13-3.69, 6%) | 1.01x | 0.56x | 0.59x | 19.49 | 20.42 | 9.95 | 10.47 | 0.2 | 0.2 | 0 | 0 | 0 |
| fillsync ! | 309 (240-400, 19% !) | 349 (268-376, 12% !) | 328 (245-353, 16% !) | 312 (259-346, 11% !) | 1.13x | 1.06x | 1.01x | 852 | 803 | 827 | 778 | 205.9 | 204.3 | 0 | 0 | 0 |

## num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 5.20 (3.87-5.98, 16% !) | 5.30 (4.24-6.08, 14% !) | 3.53 (3.30-4.08, 9%) | 3.61 (3.30-4.52, 13% !) | 1.02x | 0.68x | 0.69x | 77.65 | 76.07 | 61.54 | 54.56 | 39.0 | 4.0 | 1 | 5 | 10 |
| fillrandom ! | 6.63 (6.53-7.34, 5%) | 6.37 (5.55-7.06, 10% !) | 8.68 (7.35-9.66, 13% !) | 6.85 (6.16-6.91, 5%) | 0.96x | 1.31x | 1.03x | 90.46 | 83.25 | 174 | 87.81 | 0.5 | 0.5 | 0 | 1 | 8 |
| overwrite | 7.50 (7.42-8.93, 8%) | 7.83 (7.02-8.50, 7%) | 11.09 (10.09-12.76, 10%) | 7.07 (6.45-7.38, 5%) | 1.04x | 1.48x | 0.94x | 96.60 | 100 | 207 | 103 | 117.4 | 117.4 | 0 | 42 | 15 |
| flush ! | 112.2 ms (98.5 ms-145.4 ms, 15% !) | 107.9 ms (99.7 ms-119.8 ms, 8%) | 109.3 ms (78.3 ms-133.3 ms, 19% !) | 97.5 ms (88.5 ms-120.6 ms, 14% !) | 0.96x | 0.97x | 0.87x | - | - | - | - | 208896.0 | 250168.0 | 0 | 50 | 5 |
| readrandom ! | 2.72 (2.31-2.89, 9%) | 2.90 (2.70-3.27, 8%) | 1.74 (1.69-2.42, 17% !) | 2.20 (1.95-2.52, 10% !) | 1.06x | 0.64x | 0.81x | 48.02 | 51.60 | 19.22 | 35.29 | 31.9 | 31.8 | 0 | 39 | 75 |
| readmissing ! | 0.36 (0.35-0.43, 9%) | 0.34 (0.28-0.51, 25% !) | 0.61 (0.40-0.65, 23% !) | 1.09 (0.88-1.20, 12% !) | 0.93x | 1.68x | 3.00x | 10.30 | 10.17 | 9.87 | 15.63 | 0.6 | 0.5 | 0 | 3 | 0 |
| seekrandom ! | 7.35 (6.82-8.07, 8%) | 7.54 (6.38-9.11, 13% !) | 3.80 (3.29-4.43, 12% !) | 3.83 (3.05-4.36, 13% !) | 1.03x | 0.52x | 0.52x | 93.06 | 96.85 | 53.27 | 52.97 | 0.7 | 0.6 | 0 | 5 | 2 |
| readseq ! | 0.21 (0.18-0.23, 11% !) | 0.29 (0.25-0.31, 8%) | 0.12 (0.08-0.13, 24% !) | 0.11 (0.08-0.11, 12% !) | 1.39x | 0.58x | 0.50x | 6.91 | 7.33 | 3.69 | 3.93 | 0.7 | 0.6 | 0 | 1 | 1 |
| readreverse ! | 0.27 (0.25-0.34, 15% !) | 0.28 (0.24-0.41, 21% !) | 0.11 (0.10-0.17, 21% !) | 0.13 (0.09-0.16, 21% !) | 1.03x | 0.41x | 0.48x | 7.47 | 7.39 | 4.35 | 4.15 | 0.7 | 0.6 | 0 | 0 | 0 |
| multireadrandom ! | 2.63 (2.17-2.73, 10%) | 2.21 (1.79-2.52, 12% !) | 1.38 (1.13-1.60, 14% !) | 1.56 (1.18-1.78, 19% !) | 0.84x | 0.52x | 0.59x | 326 | 301 | 177 | 187 | 0.9 | 0.8 | 0 | 1 | 1 |
| readwhilewriting ! | 3.67 (3.41-4.51, 11% !) | 3.54 (3.07-4.46, 14% !) | 2.68 (2.56-2.94, 6%) | 2.74 (2.63-3.29, 10%) | 0.97x | 0.73x | 0.75x | 55.34 | 54.51 | 36.18 | 38.00 | 0.7 | 0.6 | 0 | 1 | 1 |
| compact ! | 790.4 ms (656.1 ms-845.2 ms, 10% !) | 698.7 ms (668.6 ms-829.4 ms, 9%) | 409.2 ms (391.4 ms-442.4 ms, 5%) | 437.1 ms (400.4 ms-456.8 ms, 6%) | 0.88x | 0.52x | 0.55x | - | - | - | - | 7078848.0 | 7477552.0 | 0 | 31 | 9 |
| deleterandom ! | 5.59 (4.73-6.53, 12% !) | 5.80 (4.22-6.53, 16% !) | 5.08 (3.73-6.15, 17% !) | 5.18 (4.80-5.64, 7%) | 1.04x | 0.91x | 0.93x | 77.31 | 76.94 | 106 | 69.02 | 0.5 | 0.5 | 0 | 0 | 2 |
| fillsync ! | 175 (150-248, 21% !) | 191 (172-216, 10%) | 173 (128-202, 17% !) | 157 (134-172, 10%) | 1.09x | 0.99x | 0.89x | 2392 | 2425 | 2228 | 2179 | 518.4 | 519.3 | 0 | 0 | 0 |

## num=300000, value=100B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 3.46 (3.35-3.78, 5%) | 3.67 (3.35-3.90, 6%) | 1.64 (1.57-1.78, 6%) | 1.79 (1.69-1.99, 6%) | 1.06x | 0.47x | 0.52x | 10.74 | 11.78 | 5.49 | 5.84 | 0.2 | 0.2 | 1 | 1 | 2 |
| fillrandom | 6.61 (6.37-7.18, 5%) | 7.42 (6.50-7.83, 7%) | 4.10 (4.06-4.23, 2%) | 4.30 (4.11-4.53, 4%) | 1.12x | 0.62x | 0.65x | 24.16 | 29.35 | 12.00 | 13.10 | 0.2 | 0.2 | 0 | 0 | 1 |
| overwrite | 7.12 (6.48-8.41, 10%) | 7.35 (7.24-8.48, 7%) | 5.04 (4.86-5.23, 3%) | 5.14 (4.85-5.49, 5%) | 1.03x | 0.71x | 0.72x | 28.77 | 36.80 | 15.94 | 15.73 | 125.0 | 100.4 | 0 | 16 | 7 |
| flush ! | 152.2 ms (128.5 ms-213.9 ms, 22% !) | 132.6 ms (126.5 ms-173.3 ms, 15% !) | 127.3 ms (114.8 ms-164.3 ms, 15% !) | 139.2 ms (119.8 ms-163.5 ms, 12% !) | 0.87x | 0.84x | 0.91x | - | - | - | - | 307688.0 | 348960.0 | 0 | 5 | 2 |
| readrandom | 7.54 (6.98-7.82, 6%) | 8.12 (7.97-8.48, 3%) | 4.95 (4.74-5.97, 10%) | 6.19 (5.78-6.21, 4%) | 1.08x | 0.66x | 0.82x | 33.13 | 37.00 | 13.49 | 19.38 | 31.9 | 31.9 | 0 | 39 | 74 |
| readmissing ! | 1.14 (0.97-1.31, 13% !) | 1.33 (1.22-1.49, 8%) | 0.54 (0.47-0.63, 11% !) | 0.83 (0.72-1.09, 16% !) | 1.17x | 0.47x | 0.73x | 8.73 | 11.21 | 6.42 | 7.16 | 0.2 | 0.2 | 0 | 0 | 0 |
| seekrandom | 20.09 (18.81-20.78, 4%) | 23.95 (22.26-25.08, 4%) | 13.09 (12.55-13.77, 4%) | 14.61 (13.15-15.48, 7%) | 1.19x | 0.65x | 0.73x | 67.04 | 76.91 | 51.38 | 57.61 | 0.2 | 0.2 | 0 | 1 | 7 |
| readseq ! | 0.64 (0.59-0.76, 11% !) | 0.82 (0.81-0.99, 9%) | 0.36 (0.33-0.47, 15% !) | 0.38 (0.34-0.49, 15% !) | 1.28x | 0.56x | 0.59x | 6.44 | 8.29 | 4.76 | 4.99 | 0.2 | 0.2 | 0 | 2 | 0 |
| readreverse ! | 0.73 (0.70-0.78, 4%) | 0.90 (0.89-0.99, 5%) | 0.55 (0.42-0.64, 19% !) | 0.44 (0.43-0.66, 20% !) | 1.24x | 0.75x | 0.61x | 6.34 | 7.45 | 6.68 | 4.97 | 0.3 | 0.2 | 0 | 0 | 0 |
| multireadrandom | 5.74 (5.57-6.23, 4%) | 6.68 (6.16-6.80, 4%) | 5.22 (4.67-5.75, 9%) | 5.20 (4.49-5.63, 10%) | 1.17x | 0.91x | 0.91x | 204 | 237 | 187 | 177 | 0.3 | 0.3 | 0 | 0 | 0 |
| readwhilewriting | 8.79 (8.37-9.42, 5%) | 9.74 (8.57-10.11, 7%) | 6.82 (5.94-7.00, 7%) | 7.56 (6.58-7.98, 8%) | 1.11x | 0.78x | 0.86x | 43.67 | 44.97 | 27.03 | 32.14 | 0.3 | 0.3 | 0 | 0 | 0 |
| compact | 726.3 ms (682.5 ms-846.3 ms, 8%) | 759.6 ms (675.1 ms-818.8 ms, 7%) | 719.6 ms (637.8 ms-738.6 ms, 6%) | 726.7 ms (705.3 ms-753.3 ms, 2%) | 1.05x | 0.99x | 1.00x | - | - | - | - | 6303512.0 | 6666520.0 | 0 | 32 | 8 |
| deleterandom | 5.52 (5.42-6.10, 5%) | 5.69 (5.48-6.58, 8%) | 3.26 (3.20-3.67, 7%) | 3.64 (3.56-3.94, 4%) | 1.03x | 0.59x | 0.66x | 17.15 | 17.19 | 9.84 | 11.59 | 0.2 | 0.2 | 0 | 1 | 0 |
| fillsync ! | 271 (227-324, 14% !) | 313 (282-421, 17% !) | 267 (260-344, 13% !) | 300 (261-345, 10% !) | 1.16x | 0.99x | 1.11x | 705 | 786 | 885 | 803 | 205.9 | 204.3 | 0 | 0 | 0 |

## num=300000, value=100B, threads=4, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 5.22 (4.35-6.04, 12% !) | 5.08 (4.15-6.37, 16% !) | 3.57 (2.66-4.61, 26% !) | 3.90 (3.59-4.09, 5%) | 0.97x | 0.68x | 0.75x | 79.07 | 80.29 | 59.63 | 58.50 | 46.0 | 18.0 | 1 | 5 | 8 |
| fillrandom ! | 7.58 (5.90-7.89, 11% !) | 7.05 (6.11-7.69, 9%) | 8.74 (8.10-10.44, 10% !) | 6.83 (6.34-6.87, 3%) | 0.93x | 1.15x | 0.90x | 94.37 | 93.96 | 197 | 90.25 | 0.5 | 0.5 | 0 | 0 | 2 |
| overwrite ! | 7.29 (7.12-8.75, 9%) | 7.85 (7.60-8.02, 2%) | 13.18 (9.21-13.77, 18% !) | 6.94 (6.50-8.00, 8%) | 1.08x | 1.81x | 0.95x | 98.65 | 101 | 230 | 99.64 | 125.6 | 139.2 | 0 | 45 | 57 |
| flush ! | 125.2 ms (113.0 ms-141.7 ms, 10%) | 145.8 ms (125.1 ms-159.6 ms, 11% !) | 133.4 ms (111.6 ms-173.4 ms, 19% !) | 120.2 ms (113.6 ms-123.2 ms, 3%) | 1.16x | 1.07x | 0.96x | - | - | - | - | 380832.0 | 373576.0 | 3 | 71 | 5 |
| readrandom ! | 3.11 (2.56-3.95, 17% !) | 3.33 (2.29-3.61, 16% !) | 2.14 (1.80-2.33, 10%) | 2.24 (2.06-2.33, 5%) | 1.07x | 0.69x | 0.72x | 58.63 | 56.39 | 35.20 | 33.51 | 32.4 | 32.3 | 0 | 38 | 71 |
| readmissing ! | 0.40 (0.34-0.46, 11% !) | 0.38 (0.34-0.53, 22% !) | 0.57 (0.47-0.73, 20% !) | 0.78 (0.65-1.00, 18% !) | 0.94x | 1.42x | 1.93x | 9.91 | 11.37 | 10.19 | 11.72 | 0.6 | 0.5 | 0 | 1 | 0 |
| seekrandom | 8.34 (6.79-8.83, 10%) | 8.65 (8.30-9.48, 6%) | 4.45 (3.98-4.94, 9%) | 4.40 (4.23-4.70, 4%) | 1.04x | 0.53x | 0.53x | 106 | 110 | 59.75 | 60.17 | 0.7 | 0.6 | 0 | 6 | 7 |
| readseq ! | 0.21 (0.15-0.24, 17% !) | 0.21 (0.18-0.28, 17% !) | 0.10 (0.09-0.10, 6%) | 0.11 (0.09-0.12, 11% !) | 1.01x | 0.47x | 0.50x | 7.30 | 7.97 | 4.36 | 4.59 | 0.7 | 0.6 | 0 | 1 | 1 |
| readreverse ! | 0.26 (0.19-0.38, 27% !) | 0.25 (0.23-0.33, 17% !) | 0.12 (0.10-0.12, 9%) | 0.11 (0.10-0.14, 15% !) | 0.93x | 0.44x | 0.43x | 7.21 | 7.97 | 5.16 | 4.71 | 0.7 | 0.6 | 0 | 0 | 0 |
| multireadrandom ! | 2.73 (2.67-3.03, 6%) | 2.49 (1.99-3.19, 18% !) | 1.70 (1.41-1.84, 10%) | 1.63 (1.39-1.72, 8%) | 0.91x | 0.62x | 0.60x | 382 | 346 | 214 | 192 | 1.0 | 0.8 | 0 | 7 | 1 |
| readwhilewriting ! | 4.06 (3.80-4.39, 7%) | 3.72 (3.49-4.55, 11% !) | 2.67 (2.54-2.82, 4%) | 2.84 (2.25-3.04, 11% !) | 0.92x | 0.66x | 0.70x | 61.46 | 53.35 | 38.00 | 41.61 | 0.7 | 0.6 | 0 | 0 | 1 |
| compact ! | 523.9 ms (501.1 ms-602.6 ms, 9%) | 578.2 ms (517.3 ms-759.2 ms, 16% !) | 537.9 ms (501.4 ms-583.7 ms, 6%) | 543.2 ms (326.9 ms-572.9 ms, 20% !) | 1.10x | 1.03x | 1.04x | - | - | - | - | 5912576.0 | 6203912.0 | 0 | 14 | 27 |
| deleterandom ! | 5.66 (4.73-7.00, 15% !) | 5.98 (5.61-6.92, 9%) | 5.60 (5.44-6.10, 5%) | 4.94 (4.14-5.47, 10%) | 1.06x | 0.99x | 0.87x | 80.83 | 83.78 | 128 | 65.08 | 0.5 | 0.5 | 0 | 0 | 2 |
| fillsync ! | 178 (150-216, 13% !) | 202 (177-219, 10% !) | 187 (149-205, 12% !) | 140 (116-149, 10% !) | 1.13x | 1.05x | 0.79x | 1982 | 1950 | 2163 | 1475 | 518.4 | 519.3 | 0 | 0 | 1 |

## num=100000, value=1024B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 6.47 (5.83-6.51, 5%) | 5.87 (5.41-7.49, 13% !) | 4.31 (3.84-4.97, 11% !) | 4.07 (3.73-4.60, 10% !) | 0.91x | 0.67x | 0.63x | 24.15 | 17.32 | 10.42 | 11.58 | 35.6 | 11.6 | 1 | 3 | 3 |
| fillrandom | 8.88 (8.62-10.24, 8%) | 9.61 (8.68-10.36, 7%) | 6.97 (6.08-7.42, 7%) | 7.12 (6.88-7.45, 3%) | 1.08x | 0.78x | 0.80x | 40.39 | 42.50 | 18.60 | 20.74 | 4.2 | 3.5 | 0 | 4 | 18 |
| overwrite ! | 8.99 (8.18-9.70, 6%) | 9.52 (9.15-9.70, 2%) | 7.16 (6.17-7.44, 9%) | 7.50 (6.46-8.57, 11% !) | 1.06x | 0.80x | 0.83x | 39.54 | 42.95 | 18.25 | 20.14 | 18.0 | 11.5 | 0 | 3 | 13 |
| flush ! | 463.1 ms (342.4 ms-545.8 ms, 17% !) | 433.3 ms (358.3 ms-475.2 ms, 11% !) | 43.5 ms (36.9 ms-48.9 ms, 10%) | 39.6 ms (38.2 ms-46.8 ms, 8%) | 0.94x | 0.09x | 0.09x | - | - | - | - | 117392.0 | 47888.0 | 0 | 5 | 2 |
| readrandom | 9.93 (9.26-11.20, 7%) | 10.76 (9.27-11.30, 8%) | 4.94 (4.49-5.60, 8%) | 5.96 (5.69-6.51, 6%) | 1.08x | 0.50x | 0.60x | 45.53 | 49.50 | 13.11 | 19.43 | 89.2 | 89.1 | 0 | 8 | 6 |
| readmissing ! | 0.98 (0.79-1.12, 12% !) | 1.23 (0.87-1.60, 21% !) | 0.39 (0.31-0.56, 24% !) | 0.50 (0.48-0.70, 17% !) | 1.25x | 0.40x | 0.51x | 6.13 | 8.63 | 2.45 | 3.00 | 0.7 | 0.6 | 0 | 0 | 0 |
| seekrandom | 29.83 (24.58-30.75, 9%) | 30.83 (27.69-33.02, 7%) | 15.67 (14.67-16.62, 5%) | 16.85 (15.26-18.42, 8%) | 1.03x | 0.53x | 0.56x | 90.56 | 89.03 | 57.88 | 58.73 | 0.7 | 0.6 | 1 | 8 | 11 |
| readseq ! | 1.96 (1.72-2.90, 22% !) | 2.15 (1.66-3.49, 30% !) | 1.10 (1.00-1.54, 19% !) | 1.08 (1.03-1.31, 11% !) | 1.10x | 0.56x | 0.55x | 10.55 | 11.42 | 5.16 | 5.08 | 0.7 | 0.6 | 0 | 0 | 0 |
| readreverse ! | 2.05 (1.90-2.42, 10%) | 2.39 (2.21-2.81, 10%) | 1.24 (1.18-1.94, 23% !) | 1.31 (1.18-1.67, 14% !) | 1.17x | 0.61x | 0.64x | 9.37 | 10.75 | 5.79 | 5.68 | 0.7 | 0.6 | 0 | 1 | 1 |
| multireadrandom ! | 6.34 (5.21-8.83, 21% !) | 7.46 (5.87-8.35, 13% !) | 4.90 (4.43-5.02, 5%) | 5.24 (5.07-5.42, 2%) | 1.18x | 0.77x | 0.83x | 203 | 248 | 172 | 191 | 1.0 | 1.0 | 0 | 0 | 0 |
| readwhilewriting | 11.58 (10.07-12.36, 7%) | 11.31 (10.75-12.12, 4%) | 7.30 (6.52-7.80, 7%) | 8.29 (7.52-9.70, 10%) | 0.98x | 0.63x | 0.72x | 50.25 | 50.34 | 26.11 | 42.23 | 6.1 | 2.2 | 0 | 5 | 113 |
| compact ! | 1764.6 ms (889.2 ms-2422.8 ms, 42% !) | 1809.3 ms (929.3 ms-1926.3 ms, 31% !) | 705.4 ms (657.9 ms-774.7 ms, 7%) | 728.5 ms (698.7 ms-772.3 ms, 4%) | 1.03x | 0.40x | 0.41x | - | - | - | - | 851128.0 | 1370688.0 | 0 | 13 | 9 |
| deleterandom ! | 5.13 (4.55-6.16, 13% !) | 5.08 (4.35-6.35, 14% !) | 2.57 (2.11-2.63, 11% !) | 2.91 (2.44-3.43, 14% !) | 0.99x | 0.50x | 0.57x | 17.57 | 18.04 | 7.76 | 9.12 | 0.6 | 0.6 | 0 | 0 | 0 |
| fillsync ! | 398 (315-875, 48% !) | 326 (287-1990, 113% !) | 316 (283-361, 10% !) | 334 (260-426, 20% !) | 0.82x | 0.79x | 0.84x | 1065 | 1475 | 598 | 1294 | 617.8 | 612.8 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 8.25 (7.39-10.05, 12% !) | 7.11 (6.74-8.81, 11% !) | 7.58 (6.54-7.98, 9%) | 6.76 (6.16-7.93, 10%) | 0.86x | 0.92x | 0.82x | 103 | 98.34 | 186 | 104 | 39.1 | 18.1 | 1 | 5 | 132 |
| fillrandom ! | 9.52 (7.37-10.62, 14% !) | 9.84 (8.92-10.56, 7%) | 11.72 (9.67-17.72, 28% !) | 9.37 (9.12-10.10, 4%) | 1.03x | 1.23x | 0.98x | 114 | 122 | 253 | 147 | 8.2 | 3.0 | 0 | 18 | 25 |
| overwrite ! | 10.53 (9.89-10.86, 4%) | 11.40 (9.51-11.64, 8%) | 15.38 (12.48-17.00, 11% !) | 9.04 (8.33-11.07, 11% !) | 1.08x | 1.46x | 0.86x | 126 | 147 | 286 | 135 | 6.5 | 21.7 | 0 | 12 | 11 |
| flush ! | 377.4 ms (296.5 ms-437.3 ms, 14% !) | 354.5 ms (335.7 ms-528.5 ms, 21% !) | 43.5 ms (36.3 ms-46.8 ms, 10% !) | 44.4 ms (39.5 ms-47.7 ms, 7%) | 0.94x | 0.12x | 0.12x | - | - | - | - | 37688.0 | 58464.0 | 0 | 3 | 3 |
| readrandom ! | 4.46 (4.11-4.88, 7%) | 4.40 (4.23-5.23, 9%) | 1.94 (1.89-2.95, 21% !) | 1.96 (1.72-2.85, 21% !) | 0.99x | 0.43x | 0.44x | 61.26 | 62.75 | 29.44 | 21.12 | 90.6 | 90.3 | 1 | 7 | 18 |
| readmissing ! | 0.80 (0.77-1.01, 13% !) | 0.81 (0.57-0.86, 15% !) | 0.47 (0.25-0.50, 24% !) | 0.49 (0.37-0.72, 26% !) | 1.01x | 0.59x | 0.61x | 18.12 | 16.78 | 7.39 | 7.43 | 1.7 | 1.6 | 0 | 2 | 2 |
| seekrandom ! | 25.91 (23.56-29.26, 8%) | 26.77 (25.68-29.02, 5%) | 5.72 (5.01-6.63, 10% !) | 6.62 (5.78-7.19, 9%) | 1.03x | 0.22x | 0.26x | 222 | 235 | 64.46 | 73.68 | 1.8 | 1.7 | 0 | 36 | 53 |
| readseq ! | 0.60 (0.48-0.65, 12% !) | 0.67 (0.52-0.76, 16% !) | 0.39 (0.30-0.41, 13% !) | 0.42 (0.39-0.49, 8%) | 1.13x | 0.66x | 0.71x | 31.57 | 31.82 | 9.33 | 12.42 | 2.4 | 2.5 | 0 | 0 | 4 |
| readreverse ! | 0.49 (0.47-0.67, 15% !) | 0.63 (0.57-0.84, 17% !) | 0.42 (0.34-0.45, 12% !) | 0.40 (0.32-0.44, 11% !) | 1.28x | 0.85x | 0.81x | 23.17 | 25.53 | 10.37 | 8.63 | 2.5 | 2.2 | 0 | 4 | 4 |
| multireadrandom ! | 2.58 (2.32-3.04, 10%) | 2.57 (1.90-2.90, 16% !) | 1.66 (1.11-2.02, 22% !) | 1.85 (1.68-2.29, 13% !) | 1.00x | 0.64x | 0.71x | 334 | 326 | 195 | 219 | 3.2 | 2.6 | 0 | 0 | 1 |
| readwhilewriting ! | 4.11 (3.81-4.58, 7%) | 4.14 (3.67-4.69, 11% !) | 3.06 (2.25-3.23, 14% !) | 2.99 (2.88-3.34, 6%) | 1.01x | 0.74x | 0.73x | 57.90 | 56.16 | 39.46 | 42.92 | 2.1 | 1.9 | 0 | 0 | 1 |
| compact ! | 1249.4 ms (1196.8 ms-1305.9 ms, 4%) | 1294.3 ms (1022.4 ms-1359.0 ms, 11% !) | 587.1 ms (525.3 ms-709.7 ms, 12% !) | 628.2 ms (564.4 ms-675.3 ms, 7%) | 1.04x | 0.47x | 0.50x | - | - | - | - | 255216.0 | 1047288.0 | 0 | 8 | 26 |
| deleterandom ! | 5.04 (4.02-5.65, 13% !) | 4.91 (3.45-5.68, 18% !) | 4.31 (2.94-5.39, 21% !) | 4.07 (3.07-5.01, 19% !) | 0.98x | 0.85x | 0.81x | 77.02 | 72.98 | 65.31 | 54.39 | 1.6 | 1.6 | 0 | 0 | 2 |
| fillsync ! | 190 (153-214, 16% !) | 208 (178-227, 8%) | 238 (209-282, 12% !) | 205 (158-249, 18% !) | 1.09x | 1.25x | 1.08x | 1868 | 2064 | 2195 | 2250 | 1555.3 | 1557.8 | 0 | 0 | 1 |

## num=100000, value=1024B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 6.10 (5.55-6.48, 7%) | 6.99 (6.24-7.36, 6%) | 3.85 (3.65-4.24, 6%) | 3.97 (3.85-4.54, 7%) | 1.15x | 0.63x | 0.65x | 22.56 | 26.62 | 9.77 | 9.86 | 0.7 | 0.7 | 1 | 5 | 16 |
| fillrandom ! | 8.90 (8.29-9.28, 5%) | 9.06 (8.61-10.46, 8%) | 5.72 (4.94-6.76, 13% !) | 5.86 (5.11-6.58, 10% !) | 1.02x | 0.64x | 0.66x | 40.39 | 41.78 | 14.32 | 16.78 | 1.4 | 0.8 | 0 | 1 | 7 |
| overwrite ! | 8.96 (8.20-10.03, 8%) | 8.30 (8.21-9.11, 4%) | 5.99 (5.40-6.69, 8%) | 6.50 (5.79-8.18, 14% !) | 0.93x | 0.67x | 0.73x | 39.50 | 38.86 | 16.58 | 17.04 | 44.3 | 1.0 | 0 | 6 | 3 |
| flush ! | 287.2 ms (191.8 ms-305.4 ms, 17% !) | 222.8 ms (206.5 ms-274.7 ms, 13% !) | 81.0 ms (75.7 ms-114.0 ms, 18% !) | 74.7 ms (66.2 ms-111.6 ms, 22% !) | 0.78x | 0.28x | 0.26x | - | - | - | - | 672192.0 | 36624.0 | 0 | 10 | 27 |
| readrandom ! | 10.24 (9.23-11.74, 10%) | 9.38 (8.65-11.28, 10% !) | 5.69 (5.05-6.54, 9%) | 6.79 (6.38-7.28, 6%) | 0.92x | 0.56x | 0.66x | 48.57 | 45.45 | 16.02 | 24.00 | 113.8 | 114.0 | 0 | 8 | 3 |
| readmissing ! | 0.86 (0.80-1.56, 31% !) | 0.93 (0.80-1.11, 16% !) | 0.32 (0.31-0.33, 2%) | 0.48 (0.47-0.78, 25% !) | 1.08x | 0.37x | 0.56x | 5.52 | 5.12 | 1.22 | 2.60 | 0.7 | 0.6 | 0 | 0 | 0 |
| seekrandom | 28.02 (24.84-29.44, 7%) | 28.13 (27.24-29.59, 3%) | 15.84 (13.75-16.99, 8%) | 17.03 (14.98-18.90, 8%) | 1.00x | 0.57x | 0.61x | 93.09 | 85.02 | 57.10 | 60.25 | 0.7 | 0.6 | 1 | 3 | 6 |
| readseq ! | 1.77 (1.74-1.95, 5%) | 1.87 (1.84-2.44, 12% !) | 1.00 (0.88-1.13, 9%) | 1.05 (0.95-1.27, 12% !) | 1.06x | 0.57x | 0.59x | 9.44 | 10.90 | 4.93 | 4.69 | 0.7 | 0.6 | 0 | 0 | 0 |
| readreverse ! | 2.43 (1.81-3.42, 24% !) | 2.59 (2.26-3.10, 13% !) | 1.22 (1.12-1.53, 12% !) | 1.31 (1.14-1.61, 14% !) | 1.07x | 0.50x | 0.54x | 10.82 | 11.67 | 5.34 | 6.20 | 0.7 | 0.6 | 0 | 0 | 0 |
| multireadrandom ! | 6.33 (5.98-7.01, 6%) | 6.33 (5.94-9.28, 20% !) | 4.74 (4.35-6.85, 21% !) | 5.25 (4.48-6.83, 17% !) | 1.00x | 0.75x | 0.83x | 211 | 211 | 156 | 173 | 1.1 | 1.0 | 0 | 0 | 0 |
| readwhilewriting ! | 10.67 (10.60-12.64, 9%) | 12.80 (11.24-13.64, 8%) | 7.14 (6.99-7.24, 2%) | 7.53 (7.24-10.21, 15% !) | 1.20x | 0.67x | 0.71x | 49.17 | 55.99 | 23.72 | 30.14 | 2.7 | 1.4 | 0 | 2 | 64 |
| compact ! | 1057.1 ms (810.2 ms-1349.0 ms, 19% !) | 898.4 ms (815.4 ms-1003.9 ms, 8%) | 952.9 ms (876.3 ms-1069.9 ms, 8%) | 1004.8 ms (977.7 ms-1059.9 ms, 3%) | 0.85x | 0.90x | 0.95x | - | - | - | - | 2112160.0 | 2848104.0 | 0 | 9 | 9 |
| deleterandom ! | 4.59 (4.36-5.59, 11% !) | 5.46 (4.43-5.79, 11% !) | 2.47 (2.29-3.18, 14% !) | 2.88 (2.42-2.98, 8%) | 1.19x | 0.54x | 0.63x | 13.66 | 16.93 | 7.21 | 8.90 | 0.6 | 0.6 | 1 | 0 | 0 |
| fillsync ! | 409 (258-484, 23% !) | 318 (274-470, 22% !) | 356 (323-424, 13% !) | 375 (264-534, 25% !) | 0.78x | 0.87x | 0.92x | 918 | 885 | 1376 | 1327 | 617.8 | 612.8 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=4, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 9.05 (8.19-10.40, 10%) | 7.93 (6.86-9.53, 12% !) | 7.16 (5.95-8.46, 15% !) | 6.39 (5.80-7.27, 10% !) | 0.88x | 0.79x | 0.71x | 163 | 115 | 191 | 108 | 10.0 | 4.8 | 1 | 14 | 10 |
| fillrandom ! | 11.26 (10.75-12.22, 5%) | 11.05 (9.21-11.30, 8%) | 11.66 (9.08-14.62, 17% !) | 8.40 (7.82-9.89, 10% !) | 0.98x | 1.04x | 0.75x | 181 | 158 | 252 | 145 | 4.3 | 7.5 | 0 | 10 | 7 |
| overwrite ! | 10.97 (8.91-11.31, 9%) | 11.84 (10.60-13.26, 8%) | 14.01 (9.72-16.52, 22% !) | 8.45 (8.22-10.23, 9%) | 1.08x | 1.28x | 0.77x | 148 | 192 | 257 | 156 | 4.4 | 5.2 | 0 | 24 | 24 |
| flush ! | 204.1 ms (133.7 ms-344.4 ms, 37% !) | 155.8 ms (107.1 ms-217.0 ms, 29% !) | 70.2 ms (65.5 ms-75.6 ms, 6%) | 104.9 ms (70.0 ms-132.9 ms, 25% !) | 0.76x | 0.34x | 0.51x | - | - | - | - | 38408.0 | 36664.0 | 0 | 11 | 15 |
| readrandom ! | 4.52 (3.20-5.16, 17% !) | 4.56 (3.85-5.57, 14% !) | 2.47 (2.33-2.82, 9%) | 3.31 (2.87-3.87, 12% !) | 1.01x | 0.55x | 0.73x | 66.00 | 67.53 | 32.20 | 44.15 | 105.1 | 115.5 | 1 | 2 | 2 |
| readmissing ! | 0.93 (0.70-1.11, 17% !) | 0.93 (0.75-1.27, 22% !) | 0.31 (0.28-0.34, 7%) | 0.65 (0.52-0.83, 20% !) | 1.00x | 0.33x | 0.70x | 17.65 | 20.33 | 4.25 | 9.17 | 6.5 | 6.3 | 0 | 1 | 1 |
| seekrandom ! | 11.56 (10.04-29.66, 54% !) | 12.19 (8.68-26.84, 50% !) | 5.81 (4.59-7.18, 17% !) | 6.97 (5.95-8.15, 12% !) | 1.05x | 0.50x | 0.60x | 134 | 132 | 65.68 | 79.71 | 2.0 | 1.7 | 0 | 23 | 35 |
| readseq ! | 0.62 (0.49-0.80, 18% !) | 0.67 (0.51-0.84, 18% !) | 0.30 (0.29-0.51, 28% !) | 0.40 (0.30-0.47, 16% !) | 1.08x | 0.48x | 0.64x | 32.51 | 34.15 | 8.16 | 12.77 | 2.9 | 2.4 | 0 | 2 | 1 |
| readreverse ! | 0.62 (0.49-0.74, 14% !) | 0.66 (0.62-0.85, 13% !) | 0.40 (0.35-0.52, 16% !) | 0.49 (0.34-0.61, 22% !) | 1.07x | 0.65x | 0.79x | 28.39 | 27.74 | 8.94 | 11.18 | 2.7 | 2.5 | 0 | 3 | 4 |
| multireadrandom ! | 2.63 (2.06-3.51, 22% !) | 2.88 (2.74-3.63, 13% !) | 2.04 (1.88-2.47, 11% !) | 2.12 (1.85-2.34, 10% !) | 1.10x | 0.78x | 0.81x | 578 | 506 | 288 | 292 | 3.5 | 2.7 | 0 | 2 | 2 |
| readwhilewriting ! | 4.55 (3.93-6.28, 19% !) | 4.84 (3.97-5.32, 11% !) | 3.11 (2.70-3.32, 8%) | 3.41 (2.68-3.69, 12% !) | 1.06x | 0.68x | 0.75x | 61.90 | 69.21 | 43.37 | 45.87 | 2.3 | 1.9 | 0 | 0 | 0 |
| compact ! | 931.8 ms (789.2 ms-1013.8 ms, 10%) | 887.7 ms (834.6 ms-1115.3 ms, 12% !) | 954.5 ms (913.9 ms-1025.2 ms, 5%) | 878.4 ms (802.7 ms-901.4 ms, 5%) | 0.95x | 1.02x | 0.94x | - | - | - | - | 2671384.0 | 1842896.0 | 0 | 38 | 14 |
| deleterandom ! | 4.86 (4.73-5.34, 5%) | 6.02 (5.39-7.17, 11% !) | 4.68 (4.55-5.64, 9%) | 4.21 (3.94-4.94, 9%) | 1.24x | 0.96x | 0.87x | 71.02 | 87.55 | 105 | 54.82 | 1.6 | 1.6 | 0 | 0 | 3 |
| fillsync ! | 203 (175-247, 15% !) | 240 (233-253, 4%) | 234 (226-290, 11% !) | 194 (152-221, 15% !) | 1.18x | 1.15x | 0.96x | 1933 | 2414 | 2073 | 1802 | 1555.3 | 1557.8 | 0 | 0 | 0 |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom ! | 5.14 (4.91-5.42, 5%) | 5.05 (4.86-5.95, 9%) | 2.95 (2.85-3.39, 8%) | 3.34 (2.99-3.96, 11% !) | 0.98x | 0.57x | 0.65x | 18.01 | 15.57 | 9.05 | 10.05 | 4.7 | 4.4 | 1 | 659 | 324 |
| overwrite ! | 4.95 (4.69-5.09, 4%) | 5.55 (5.06-6.34, 9%) | 3.02 (2.76-3.38, 8%) | 3.42 (2.84-3.81, 11% !) | 1.12x | 0.61x | 0.69x | 16.49 | 19.49 | 8.74 | 10.65 | 11.4 | 10.8 | 0 | 769 | 505 |
| readrandom | 11.64 (10.56-12.80, 8%) | 12.29 (11.18-13.06, 5%) | 6.82 (6.36-7.22, 5%) | 8.53 (7.66-9.26, 7%) | 1.06x | 0.59x | 0.73x | 48.63 | 50.88 | 21.47 | 40.25 | 19.3 | 19.1 | 0 | 35 | 78 |
| seekrandom ! | 42.64 (40.08-46.08, 5%) | 47.43 (44.34-54.20, 8%) | 23.42 (14.28-27.64, 24% !) | 28.75 (25.82-29.84, 5%) | 1.11x | 0.55x | 0.67x | 114 | 129 | 75.55 | 82.07 | 0.2 | 0.1 | 2 | 47 | 12 |
| waitforcompaction ! | 25.06 (23.44-27.11, 6%) | 31.85 (27.25-39.11, 13% !) | 24.53 (23.63-26.29, 4%) | 27.09 (23.00-49.36, 36% !) | 1.27x | 0.98x | 1.08x | - | - | - | - | 40.0 | 40.0 | 0 | 0 | 0 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| num=300000, value=100B, threads=1, compression=none | fillseq | c-native | 17.6% | 3.50 | 3.14-4.66 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillseq | managed | 10.5% | 1.66 | 1.54-1.98 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillrandom | c-native | 15.3% | 7.12 | 6.20-9.11 | 5 |
| num=300000, value=100B, threads=1, compression=none | overwrite | c-native | 16.1% | 7.68 | 6.89-10.20 | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | rocksdb-sharp | 11.5% | 127.5 ms | 123.1 ms-158.4 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | readrandom | managed | 11.5% | 4.59 | 4.17-5.45 | 5 |
| num=300000, value=100B, threads=1, compression=none | readmissing | c-native | 36.8% | 1.25 | 1.03-2.28 | 5 |
| num=300000, value=100B, threads=1, compression=none | readmissing | rocksdb-sharp | 10.5% | 1.28 | 1.15-1.48 | 5 |
| num=300000, value=100B, threads=1, compression=none | readmissing | managed | 13.3% | 0.49 | 0.42-0.59 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | c-native | 17.7% | 0.66 | 0.61-0.90 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | rocksdb-sharp | 16.2% | 0.90 | 0.75-1.13 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | managed | 19.0% | 0.34 | 0.32-0.49 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | managed-async | 18.5% | 0.33 | 0.32-0.47 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | rocksdb-sharp | 14.2% | 1.09 | 0.89-1.27 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | managed | 13.8% | 0.54 | 0.42-0.58 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | managed-async | 18.0% | 0.46 | 0.38-0.59 | 5 |
| num=300000, value=100B, threads=1, compression=none | multireadrandom | c-native | 13.2% | 5.78 | 5.33-7.39 | 5 |
| num=300000, value=100B, threads=1, compression=none | multireadrandom | managed | 12.1% | 4.35 | 4.08-5.50 | 5 |
| num=300000, value=100B, threads=1, compression=none | readwhilewriting | rocksdb-sharp | 11.8% | 9.68 | 8.05-11.07 | 5 |
| num=300000, value=100B, threads=1, compression=none | compact | rocksdb-sharp | 16.3% | 1078.9 ms | 988.8 ms-1407.5 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | compact | managed | 12.6% | 560.5 ms | 515.2 ms-672.3 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | c-native | 18.7% | 309 | 240-400 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | rocksdb-sharp | 12.2% | 349 | 268-376 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | managed | 15.8% | 328 | 245-353 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | managed-async | 10.7% | 312 | 259-346 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillseq | c-native | 15.8% | 5.20 | 3.87-5.98 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillseq | rocksdb-sharp | 13.8% | 5.30 | 4.24-6.08 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillseq | managed-async | 13.1% | 3.61 | 3.30-4.52 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillrandom | rocksdb-sharp | 10.4% | 6.37 | 5.55-7.06 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillrandom | managed | 12.5% | 8.68 | 7.35-9.66 | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | c-native | 15.4% | 112.2 ms | 98.5 ms-145.4 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | managed | 18.8% | 109.3 ms | 78.3 ms-133.3 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | managed-async | 13.7% | 97.5 ms | 88.5 ms-120.6 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | readrandom | managed | 16.9% | 1.74 | 1.69-2.42 | 5 |
| num=300000, value=100B, threads=4, compression=none | readrandom | managed-async | 10.3% | 2.20 | 1.95-2.52 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | rocksdb-sharp | 25.4% | 0.34 | 0.28-0.51 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | managed | 23.4% | 0.61 | 0.40-0.65 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | managed-async | 12.4% | 1.09 | 0.88-1.20 | 5 |
| num=300000, value=100B, threads=4, compression=none | seekrandom | rocksdb-sharp | 13.3% | 7.54 | 6.38-9.11 | 5 |
| num=300000, value=100B, threads=4, compression=none | seekrandom | managed | 11.7% | 3.80 | 3.29-4.43 | 5 |
| num=300000, value=100B, threads=4, compression=none | seekrandom | managed-async | 13.1% | 3.83 | 3.05-4.36 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | c-native | 11.2% | 0.21 | 0.18-0.23 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | managed | 23.6% | 0.12 | 0.08-0.13 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | managed-async | 12.0% | 0.11 | 0.08-0.11 | 5 |
| num=300000, value=100B, threads=4, compression=none | readreverse | c-native | 15.4% | 0.27 | 0.25-0.34 | 5 |
| num=300000, value=100B, threads=4, compression=none | readreverse | rocksdb-sharp | 21.5% | 0.28 | 0.24-0.41 | 5 |
| num=300000, value=100B, threads=4, compression=none | readreverse | managed | 21.4% | 0.11 | 0.10-0.17 | 5 |
| num=300000, value=100B, threads=4, compression=none | readreverse | managed-async | 20.8% | 0.13 | 0.09-0.16 | 5 |
| num=300000, value=100B, threads=4, compression=none | multireadrandom | rocksdb-sharp | 12.2% | 2.21 | 1.79-2.52 | 5 |
| num=300000, value=100B, threads=4, compression=none | multireadrandom | managed | 14.3% | 1.38 | 1.13-1.60 | 5 |
| num=300000, value=100B, threads=4, compression=none | multireadrandom | managed-async | 18.6% | 1.56 | 1.18-1.78 | 5 |
| num=300000, value=100B, threads=4, compression=none | readwhilewriting | c-native | 11.2% | 3.67 | 3.41-4.51 | 5 |
| num=300000, value=100B, threads=4, compression=none | readwhilewriting | rocksdb-sharp | 14.1% | 3.54 | 3.07-4.46 | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | c-native | 10.3% | 790.4 ms | 656.1 ms-845.2 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | deleterandom | c-native | 12.4% | 5.59 | 4.73-6.53 | 5 |
| num=300000, value=100B, threads=4, compression=none | deleterandom | rocksdb-sharp | 15.8% | 5.80 | 4.22-6.53 | 5 |
| num=300000, value=100B, threads=4, compression=none | deleterandom | managed | 17.5% | 5.08 | 3.73-6.15 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | c-native | 21.3% | 175 | 150-248 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | managed | 17.4% | 173 | 128-202 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | c-native | 22.2% | 152.2 ms | 128.5 ms-213.9 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | rocksdb-sharp | 15.0% | 132.6 ms | 126.5 ms-173.3 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | managed | 15.2% | 127.3 ms | 114.8 ms-164.3 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | managed-async | 12.0% | 139.2 ms | 119.8 ms-163.5 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | c-native | 12.8% | 1.14 | 0.97-1.31 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | managed | 11.2% | 0.54 | 0.47-0.63 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | managed-async | 16.3% | 0.83 | 0.72-1.09 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | c-native | 10.7% | 0.64 | 0.59-0.76 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | managed | 14.7% | 0.36 | 0.33-0.47 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | managed-async | 15.4% | 0.38 | 0.34-0.49 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | managed | 18.6% | 0.55 | 0.42-0.64 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | managed-async | 20.0% | 0.44 | 0.43-0.66 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | c-native | 13.8% | 271 | 227-324 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | rocksdb-sharp | 16.8% | 313 | 282-421 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | managed | 12.7% | 267 | 260-344 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | managed-async | 10.2% | 300 | 261-345 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillseq | c-native | 11.7% | 5.22 | 4.35-6.04 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillseq | rocksdb-sharp | 16.2% | 5.08 | 4.15-6.37 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillseq | managed | 25.6% | 3.57 | 2.66-4.61 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillrandom | c-native | 11.1% | 7.58 | 5.90-7.89 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillrandom | managed | 10.2% | 8.74 | 8.10-10.44 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | overwrite | managed | 17.6% | 13.18 | 9.21-13.77 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | flush | rocksdb-sharp | 10.7% | 145.8 ms | 125.1 ms-159.6 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | flush | managed | 18.5% | 133.4 ms | 111.6 ms-173.4 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readrandom | c-native | 16.7% | 3.11 | 2.56-3.95 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readrandom | rocksdb-sharp | 16.4% | 3.33 | 2.29-3.61 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | c-native | 11.3% | 0.40 | 0.34-0.46 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | rocksdb-sharp | 22.3% | 0.38 | 0.34-0.53 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | managed | 19.5% | 0.57 | 0.47-0.73 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | managed-async | 17.8% | 0.78 | 0.65-1.00 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readseq | c-native | 17.4% | 0.21 | 0.15-0.24 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readseq | rocksdb-sharp | 17.1% | 0.21 | 0.18-0.28 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readseq | managed-async | 11.0% | 0.11 | 0.09-0.12 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readreverse | c-native | 27.0% | 0.26 | 0.19-0.38 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readreverse | rocksdb-sharp | 16.7% | 0.25 | 0.23-0.33 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readreverse | managed-async | 14.8% | 0.11 | 0.10-0.14 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | multireadrandom | rocksdb-sharp | 17.6% | 2.49 | 1.99-3.19 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | rocksdb-sharp | 11.3% | 3.72 | 3.49-4.55 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | managed-async | 11.5% | 2.84 | 2.25-3.04 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | compact | rocksdb-sharp | 16.3% | 578.2 ms | 517.3 ms-759.2 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | compact | managed-async | 20.0% | 543.2 ms | 326.9 ms-572.9 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | deleterandom | c-native | 14.5% | 5.66 | 4.73-7.00 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | c-native | 13.4% | 178 | 150-216 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | rocksdb-sharp | 10.3% | 202 | 177-219 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | managed | 11.9% | 187 | 149-205 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | managed-async | 10.0% | 140 | 116-149 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillseq | rocksdb-sharp | 12.9% | 5.87 | 5.41-7.49 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillseq | managed | 10.8% | 4.31 | 3.84-4.97 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillseq | managed-async | 10.2% | 4.07 | 3.73-4.60 | 5 |
| num=100000, value=1024B, threads=1, compression=none | overwrite | managed-async | 11.4% | 7.50 | 6.46-8.57 | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | c-native | 17.1% | 463.1 ms | 342.4 ms-545.8 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | rocksdb-sharp | 10.8% | 433.3 ms | 358.3 ms-475.2 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | c-native | 12.2% | 0.98 | 0.79-1.12 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | rocksdb-sharp | 21.2% | 1.23 | 0.87-1.60 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | managed | 24.0% | 0.39 | 0.31-0.56 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | managed-async | 16.6% | 0.50 | 0.48-0.70 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | c-native | 21.8% | 1.96 | 1.72-2.90 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | rocksdb-sharp | 30.2% | 2.15 | 1.66-3.49 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | managed | 18.7% | 1.10 | 1.00-1.54 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | managed-async | 10.9% | 1.08 | 1.03-1.31 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readreverse | managed | 23.5% | 1.24 | 1.18-1.94 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readreverse | managed-async | 14.3% | 1.31 | 1.18-1.67 | 5 |
| num=100000, value=1024B, threads=1, compression=none | multireadrandom | c-native | 20.6% | 6.34 | 5.21-8.83 | 5 |
| num=100000, value=1024B, threads=1, compression=none | multireadrandom | rocksdb-sharp | 13.2% | 7.46 | 5.87-8.35 | 5 |
| num=100000, value=1024B, threads=1, compression=none | compact | c-native | 42.3% | 1764.6 ms | 889.2 ms-2422.8 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | compact | rocksdb-sharp | 31.4% | 1809.3 ms | 929.3 ms-1926.3 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | c-native | 12.6% | 5.13 | 4.55-6.16 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | rocksdb-sharp | 14.4% | 5.08 | 4.35-6.35 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | managed | 10.7% | 2.57 | 2.11-2.63 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | managed-async | 13.6% | 2.91 | 2.44-3.43 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | c-native | 47.8% | 398 | 315-875 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | rocksdb-sharp | 113.0% | 326 | 287-1990 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | managed | 10.4% | 316 | 283-361 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | managed-async | 19.7% | 334 | 260-426 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillseq | c-native | 12.3% | 8.25 | 7.39-10.05 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillseq | rocksdb-sharp | 10.9% | 7.11 | 6.74-8.81 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillrandom | c-native | 14.0% | 9.52 | 7.37-10.62 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillrandom | managed | 27.7% | 11.72 | 9.67-17.72 | 5 |
| num=100000, value=1024B, threads=4, compression=none | overwrite | managed | 11.2% | 15.38 | 12.48-17.00 | 5 |
| num=100000, value=1024B, threads=4, compression=none | overwrite | managed-async | 11.2% | 9.04 | 8.33-11.07 | 5 |
| num=100000, value=1024B, threads=4, compression=none | flush | c-native | 14.2% | 377.4 ms | 296.5 ms-437.3 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | flush | rocksdb-sharp | 20.5% | 354.5 ms | 335.7 ms-528.5 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | flush | managed | 10.2% | 43.5 ms | 36.3 ms-46.8 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | readrandom | managed | 20.8% | 1.94 | 1.89-2.95 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readrandom | managed-async | 21.4% | 1.96 | 1.72-2.85 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | c-native | 13.3% | 0.80 | 0.77-1.01 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | rocksdb-sharp | 15.0% | 0.81 | 0.57-0.86 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | managed | 24.1% | 0.47 | 0.25-0.50 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | managed-async | 26.2% | 0.49 | 0.37-0.72 | 5 |
| num=100000, value=1024B, threads=4, compression=none | seekrandom | managed | 10.3% | 5.72 | 5.01-6.63 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readseq | c-native | 11.6% | 0.60 | 0.48-0.65 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readseq | rocksdb-sharp | 15.8% | 0.67 | 0.52-0.76 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readseq | managed | 13.4% | 0.39 | 0.30-0.41 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readreverse | c-native | 15.2% | 0.49 | 0.47-0.67 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readreverse | rocksdb-sharp | 17.0% | 0.63 | 0.57-0.84 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readreverse | managed | 11.7% | 0.42 | 0.34-0.45 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readreverse | managed-async | 10.8% | 0.40 | 0.32-0.44 | 5 |
| num=100000, value=1024B, threads=4, compression=none | multireadrandom | rocksdb-sharp | 16.5% | 2.57 | 1.90-2.90 | 5 |
| num=100000, value=1024B, threads=4, compression=none | multireadrandom | managed | 22.4% | 1.66 | 1.11-2.02 | 5 |
| num=100000, value=1024B, threads=4, compression=none | multireadrandom | managed-async | 12.7% | 1.85 | 1.68-2.29 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readwhilewriting | rocksdb-sharp | 10.7% | 4.14 | 3.67-4.69 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readwhilewriting | managed | 13.5% | 3.06 | 2.25-3.23 | 5 |
| num=100000, value=1024B, threads=4, compression=none | compact | rocksdb-sharp | 11.3% | 1294.3 ms | 1022.4 ms-1359.0 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | compact | managed | 12.0% | 587.1 ms | 525.3 ms-709.7 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | c-native | 12.9% | 5.04 | 4.02-5.65 | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | rocksdb-sharp | 18.1% | 4.91 | 3.45-5.68 | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | managed | 20.7% | 4.31 | 2.94-5.39 | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | managed-async | 19.4% | 4.07 | 3.07-5.01 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | c-native | 15.6% | 190 | 153-214 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | managed | 11.7% | 238 | 209-282 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | managed-async | 17.7% | 205 | 158-249 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillrandom | managed | 13.0% | 5.72 | 4.94-6.76 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillrandom | managed-async | 10.3% | 5.86 | 5.11-6.58 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | overwrite | managed-async | 13.7% | 6.50 | 5.79-8.18 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | c-native | 16.9% | 287.2 ms | 191.8 ms-305.4 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | rocksdb-sharp | 13.4% | 222.8 ms | 206.5 ms-274.7 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | managed | 17.7% | 81.0 ms | 75.7 ms-114.0 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | managed-async | 22.1% | 74.7 ms | 66.2 ms-111.6 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readrandom | rocksdb-sharp | 10.5% | 9.38 | 8.65-11.28 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | c-native | 31.4% | 0.86 | 0.80-1.56 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | rocksdb-sharp | 16.2% | 0.93 | 0.80-1.11 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | managed-async | 25.3% | 0.48 | 0.47-0.78 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | rocksdb-sharp | 12.5% | 1.87 | 1.84-2.44 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | managed-async | 12.0% | 1.05 | 0.95-1.27 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | c-native | 24.3% | 2.43 | 1.81-3.42 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | rocksdb-sharp | 12.6% | 2.59 | 2.26-3.10 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | managed | 12.0% | 1.22 | 1.12-1.53 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | managed-async | 13.9% | 1.31 | 1.14-1.61 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | rocksdb-sharp | 19.6% | 6.33 | 5.94-9.28 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | managed | 20.5% | 4.74 | 4.35-6.85 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | managed-async | 17.4% | 5.25 | 4.48-6.83 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readwhilewriting | managed-async | 15.3% | 7.53 | 7.24-10.21 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | compact | c-native | 18.9% | 1057.1 ms | 810.2 ms-1349.0 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | deleterandom | c-native | 11.1% | 4.59 | 4.36-5.59 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | deleterandom | rocksdb-sharp | 10.6% | 5.46 | 4.43-5.79 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | deleterandom | managed | 13.6% | 2.47 | 2.29-3.18 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | c-native | 22.7% | 409 | 258-484 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | rocksdb-sharp | 21.7% | 318 | 274-470 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | managed | 12.7% | 356 | 323-424 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | managed-async | 25.5% | 375 | 264-534 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillseq | rocksdb-sharp | 12.1% | 7.93 | 6.86-9.53 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillseq | managed | 15.4% | 7.16 | 5.95-8.46 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillseq | managed-async | 10.2% | 6.39 | 5.80-7.27 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillrandom | managed | 16.8% | 11.66 | 9.08-14.62 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillrandom | managed-async | 10.3% | 8.40 | 7.82-9.89 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | overwrite | managed | 22.1% | 14.01 | 9.72-16.52 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | c-native | 36.7% | 204.1 ms | 133.7 ms-344.4 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | rocksdb-sharp | 29.2% | 155.8 ms | 107.1 ms-217.0 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | managed-async | 25.1% | 104.9 ms | 70.0 ms-132.9 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readrandom | c-native | 16.6% | 4.52 | 3.20-5.16 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readrandom | rocksdb-sharp | 14.1% | 4.56 | 3.85-5.57 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readrandom | managed-async | 12.1% | 3.31 | 2.87-3.87 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | c-native | 16.6% | 0.93 | 0.70-1.11 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | rocksdb-sharp | 22.1% | 0.93 | 0.75-1.27 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | managed-async | 19.8% | 0.65 | 0.52-0.83 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | seekrandom | c-native | 53.8% | 11.56 | 10.04-29.66 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | seekrandom | rocksdb-sharp | 50.4% | 12.19 | 8.68-26.84 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | seekrandom | managed | 17.0% | 5.81 | 4.59-7.18 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | seekrandom | managed-async | 11.6% | 6.97 | 5.95-8.15 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readseq | c-native | 18.3% | 0.62 | 0.49-0.80 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readseq | rocksdb-sharp | 18.0% | 0.67 | 0.51-0.84 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readseq | managed | 27.6% | 0.30 | 0.29-0.51 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readseq | managed-async | 15.8% | 0.40 | 0.30-0.47 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readreverse | c-native | 14.4% | 0.62 | 0.49-0.74 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readreverse | rocksdb-sharp | 13.4% | 0.66 | 0.62-0.85 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readreverse | managed | 16.0% | 0.40 | 0.35-0.52 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readreverse | managed-async | 22.3% | 0.49 | 0.34-0.61 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | multireadrandom | c-native | 22.5% | 2.63 | 2.06-3.51 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | multireadrandom | rocksdb-sharp | 12.9% | 2.88 | 2.74-3.63 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | multireadrandom | managed | 10.8% | 2.04 | 1.88-2.47 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | multireadrandom | managed-async | 10.4% | 2.12 | 1.85-2.34 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readwhilewriting | c-native | 19.4% | 4.55 | 3.93-6.28 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readwhilewriting | rocksdb-sharp | 10.9% | 4.84 | 3.97-5.32 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readwhilewriting | managed-async | 11.7% | 3.41 | 2.68-3.69 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | compact | rocksdb-sharp | 12.2% | 887.7 ms | 834.6 ms-1115.3 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | deleterandom | rocksdb-sharp | 11.2% | 6.02 | 5.39-7.17 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | c-native | 14.7% | 203 | 175-247 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | managed | 10.5% | 234 | 226-290 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | managed-async | 15.2% | 194 | 152-221 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | fillrandom | managed-async | 11.4% | 3.34 | 2.99-3.96 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | overwrite | managed-async | 10.8% | 3.42 | 2.84-3.81 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | seekrandom | managed | 23.8% | 23.42 | 14.28-27.64 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | waitforcompaction | rocksdb-sharp | 13.1% | 31.85 | 27.25-39.11 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | waitforcompaction | managed-async | 35.8% | 27.09 | 23.00-49.36 | 5 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

Over the repetitions: write-amp median (min-max), and the most frequent LSM shape with the number of repetitions that had it.

| configuration | engine | runs | write-amp median (min-max) | written MB median | most frequent files per level (L0..L6) |
|---|---|---:|---:|---:|---|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 15` (4/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 15` (5/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 5 | 3.05 (2.96-3.15) | 337.9 | `2 0 0 0 0 3 15` (2/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 15` (4/5) |

Every repetition:

| configuration | engine | rep | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---:|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 1 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 2 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 2 | `2 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 2 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 3 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 3 | `0 0 0 0 0 4 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 3 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 4 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 4 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 4 | `2 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 4 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 5 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 5 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 5 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 5 | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |

Raw data: `2026-11-02-final-warm-r5.csv` `2026-11-02-final-warm-r5.lsm.csv` 
