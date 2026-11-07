# RocksDB engine comparison (final, 5 repetitions, warm, 2026-11-03): C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-03 06:11 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v230
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 1.92 2.30 3.99
- Repetitions: 5; UTC time and load average (1/5/15 min) at the start of each: rep 1 06:11 1.38 2.15 3.90; rep 2 06:27 1.35 1.74 2.50; rep 3 06:43 1.41 1.85 2.07; rep 4 06:59 1.13 1.62 1.81; rep 5 07:15 1.49 1.78 1.80
- 1-minute load average before each of the 45 engine-set runs: min 0.91, median 1.52, max 3.10
- Load average at end: 1.33 1.72 1.76; total runtime 79 min 59 s

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

Final statistically repeated run after the 2026-11-02 final run, on commit cfb30f9 (since then: per-processor SuperVersion cache, pinned table readers, AwaitState write wait, parallel memtable inserts for all groups, pipelined/unordered write options, reverse block pinning, range-delete skipping, compaction readahead, subcompactions, managed LZ4 encoder, read sampling for read-triggered compaction): REPEATS=5, LOAD_MAX=1.5, quiet machine (no other agents, nothing compiled during the measurements). Release build, warm-up on. Same sizes as 2026-11-02-final-warm-r5.

**Different CPU than 2026-11-02-final-warm-r5.** The VM now reports "Intel(R) Xeon(R) Processor @ 2.10GHz" (family 6, model 207, AVX-512 + AMX, kernel 6.18.44-fc-v230); the previous final run had "Intel(R) Xeon(R) Processor @ 2.80GHz" (kernel -fc-v185). On this CPU c-native and rocksdb-sharp are ~40% faster in absolute time (geometric mean of the median µs/op over all 116 cells: 0.60x / 0.61x of their 2026-11-02 times), the managed engines ~30% (0.71x / 0.68x), so the ratios vs c-native are not comparable 1:1 with the previous run. An interleaved A/B on this CPU (managed at 0f866b8, the tree of the previous final run, against cfb30f9 and c-native, 3 repetitions, 4 configurations; see docs/perf.md "Final run on a new VM CPU") puts the old tree at 0.71x of C and the current one at 0.76x: of the 18% shift from 0.66x to 0.78x about 7% is the CPU and about 6% is code (flush and 1 KiB seeks slower, 4-thread writes and reads faster; docs/perf.md), the rest is the difference between the 4 A/B configurations and the full matrix, and noise.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 5 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| num=300000, value=100B, threads=1, compression=none | 1.08x [1.05-1.14] (14) | 0.72x [0.70-0.76] (14) | 0.73x [0.70-0.76] (14) |
| num=300000, value=100B, threads=4, compression=none | 1.05x [0.99-1.11] (14) | 0.77x [0.73-0.82] (14) | 0.75x [0.71-0.80] (14) |
| num=300000, value=100B, threads=1, compression=lz4 | 1.07x [1.01-1.12] (14) | 0.71x [0.67-0.75] (14) | 0.74x [0.70-0.78] (14) |
| num=300000, value=100B, threads=4, compression=lz4 | 1.04x [0.99-1.12] (14) | 0.76x [0.73-0.81] (14) | 0.74x [0.71-0.80] (14) |
| num=100000, value=1024B, threads=1, compression=none | 1.05x [0.98-1.10] (14) | 0.80x [0.76-0.85] (14) | 0.81x [0.77-0.86] (14) |
| num=100000, value=1024B, threads=4, compression=none | 1.04x [0.96-1.13] (14) | 0.84x [0.77-0.91] (14) | 0.85x [0.78-0.92] (14) |
| num=100000, value=1024B, threads=1, compression=lz4 | 1.03x [0.97-1.08] (14) | 0.76x [0.72-0.81] (14) | 0.76x [0.72-0.80] (14) |
| num=100000, value=1024B, threads=4, compression=lz4 | 1.02x [0.96-1.07] (14) | 0.89x [0.82-0.92] (14) | 0.84x [0.79-0.87] (14) |
| compaction: num=500000, value=100B, threads=1, compression=none | 1.05x [0.94-1.15] (4) | 0.62x [0.53-0.66] (4) | 0.65x [0.58-0.73] (4) |
| **all matrix configurations** | **1.05x [1.02-1.07] (112)** | **0.78x [0.76-0.80] (112)** | **0.78x [0.76-0.79] (112)** |

### Per benchmark (geometric mean over the 8 matrix configurations of the median ratio vs c-native)

| benchmark | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
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

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| rocksdb-sharp | 585 | 61 | 0 | 10 (fillrandom, num=300000, value=100B, threads=4, compression=lz4, rep 5) |
| managed | 585 | 5579 | 1 | 585 (overwrite, compaction: num=500000, value=100B, threads=1, compression=none, rep 5) |
| managed-async | 585 | 5352 | 1 | 573 (fillrandom, compaction: num=500000, value=100B, threads=1, compression=none, rep 2) |

## num=300000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 1.78 (1.71-2.23, 11% !) | 2.06 (1.81-2.67, 16% !) | 1.03 (0.98-1.18, 7%) | 1.04 (1.01-1.14, 5%) | 1.16x | 0.58x | 0.58x | 5.62 | 9.49 | 3.67 | 3.53 | 0.2 | 0.2 | 1 | 1 | 2 |
| fillrandom | 4.51 (4.38-4.85, 4%) | 4.85 (4.43-5.31, 7%) | 3.14 (3.01-3.25, 3%) | 3.33 (3.22-3.52, 4%) | 1.08x | 0.70x | 0.74x | 14.99 | 18.44 | 9.69 | 9.98 | 0.2 | 0.2 | 0 | 1 | 1 |
| overwrite | 5.05 (4.70-5.29, 5%) | 5.16 (4.96-5.34, 3%) | 3.53 (3.38-4.00, 7%) | 3.75 (3.40-3.87, 6%) | 1.02x | 0.70x | 0.74x | 16.52 | 17.43 | 11.37 | 11.71 | 78.4 | 43.4 | 0 | 8 | 8 |
| flush ! | 94.1 ms (93.7 ms-120.1 ms, 11% !) | 101.0 ms (92.6 ms-122.3 ms, 12% !) | 124.4 ms (111.4 ms-158.9 ms, 15% !) | 105.1 ms (96.7 ms-122.0 ms, 10%) | 1.07x | 1.32x | 1.12x | - | - | - | - | 171336.0 | 210848.0 | 0 | 7 | 8 |
| readrandom ! | 4.20 (3.86-4.83, 9%) | 4.75 (4.45-5.38, 8%) | 3.12 (2.99-3.25, 3%) | 3.28 (3.05-4.52, 17% !) | 1.13x | 0.74x | 0.78x | 14.98 | 17.05 | 10.40 | 10.58 | 28.9 | 28.9 | 0 | 41 | 70 |
| readmissing ! | 0.70 (0.59-0.72, 8%) | 0.71 (0.69-0.82, 8%) | 0.36 (0.35-0.39, 5%) | 0.43 (0.37-0.53, 16% !) | 1.02x | 0.52x | 0.62x | 5.48 | 5.78 | 4.29 | 4.63 | 0.2 | 0.2 | 0 | 1 | 0 |
| seekrandom | 11.94 (11.11-12.61, 5%) | 13.74 (13.50-15.10, 5%) | 7.76 (7.59-8.06, 3%) | 7.91 (7.59-8.34, 4%) | 1.15x | 0.65x | 0.66x | 47.85 | 50.27 | 30.72 | 30.51 | 0.3 | 0.2 | 0 | 16 | 1 |
| readseq ! | 0.43 (0.39-0.55, 14% !) | 0.55 (0.49-0.61, 9%) | 0.27 (0.24-0.37, 20% !) | 0.26 (0.23-0.34, 16% !) | 1.29x | 0.65x | 0.61x | 4.17 | 4.68 | 2.16 | 2.88 | 9.4 | 0.2 | 0 | 14 | 1 |
| readreverse ! | 0.51 (0.46-0.62, 12% !) | 0.68 (0.59-0.77, 11% !) | 0.32 (0.31-0.42, 13% !) | 0.30 (0.27-0.41, 17% !) | 1.34x | 0.63x | 0.59x | 4.15 | 4.86 | 2.95 | 2.93 | 0.2 | 0.2 | 0 | 0 | 0 |
| multireadrandom ! | 3.68 (3.58-4.00, 5%) | 3.85 (3.78-4.76, 10% !) | 2.89 (2.78-3.33, 8%) | 2.91 (2.81-3.16, 5%) | 1.05x | 0.79x | 0.79x | 134 | 149 | 124 | 109 | 0.3 | 0.3 | 0 | 0 | 0 |
| readwhilewriting | 6.78 (6.40-7.24, 5%) | 6.37 (6.15-7.05, 6%) | 5.14 (5.08-5.81, 6%) | 5.17 (4.83-5.52, 5%) | 0.94x | 0.76x | 0.76x | 27.11 | 25.57 | 17.45 | 17.30 | 0.3 | 0.3 | 0 | 0 | 0 |
| compact | 542.6 ms (514.5 ms-579.8 ms, 5%) | 547.2 ms (516.3 ms-565.8 ms, 4%) | 469.8 ms (434.7 ms-496.4 ms, 5%) | 467.4 ms (431.2 ms-511.8 ms, 6%) | 1.01x | 0.87x | 0.86x | - | - | - | - | 31403200.0 | 39176176.0 | 0 | 27 | 27 |
| deleterandom ! | 3.57 (3.37-3.66, 3%) | 3.51 (3.31-4.07, 8%) | 2.09 (2.04-2.63, 13% !) | 2.20 (1.89-2.56, 11% !) | 0.98x | 0.59x | 0.62x | 12.35 | 12.60 | 6.87 | 6.62 | 0.2 | 0.2 | 0 | 1 | 0 |
| fillsync ! | 206 (138-218, 17% !) | 207 (189-223, 6%) | 182 (131-234, 20% !) | 183 (132-208, 17% !) | 1.01x | 0.89x | 0.89x | 524 | 623 | 573 | 446 | 206.0 | 204.3 | 0 | 0 | 0 |

## num=300000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 3.62 (3.33-4.31, 11% !) | 3.62 (3.04-3.82, 9%) | 2.86 (2.81-2.97, 2%) | 3.01 (2.89-3.11, 3%) | 1.00x | 0.79x | 0.83x | 45.85 | 50.93 | 38.21 | 37.06 | 0.5 | 14.6 | 1 | 4 | 8 |
| fillrandom | 5.06 (4.46-5.48, 9%) | 5.06 (4.97-5.16, 2%) | 4.17 (4.01-4.67, 6%) | 4.63 (4.10-5.02, 8%) | 1.00x | 0.82x | 0.91x | 68.31 | 65.28 | 57.50 | 55.44 | 0.5 | 12.5 | 0 | 1 | 2 |
| overwrite ! | 5.33 (5.08-6.06, 8%) | 5.38 (5.13-6.49, 11% !) | 4.89 (4.18-5.41, 10% !) | 5.05 (4.54-5.36, 7%) | 1.01x | 0.92x | 0.95x | 68.77 | 66.06 | 71.17 | 61.56 | 117.4 | 118.4 | 0 | 10 | 9 |
| flush ! | 94.6 ms (82.5 ms-113.5 ms, 14% !) | 98.3 ms (79.3 ms-106.2 ms, 13% !) | 110.6 ms (100.2 ms-122.1 ms, 8%) | 113.4 ms (101.4 ms-125.0 ms, 9%) | 1.04x | 1.17x | 1.20x | - | - | - | - | 40464.0 | 63712.0 | 0 | 7 | 2 |
| readrandom ! | 1.88 (1.84-2.18, 7%) | 1.93 (1.74-2.50, 14% !) | 1.19 (1.07-1.42, 12% !) | 1.28 (1.25-1.34, 3%) | 1.03x | 0.63x | 0.68x | 34.23 | 36.03 | 14.37 | 14.78 | 29.4 | 29.3 | 0 | 39 | 72 |
| readmissing ! | 0.24 (0.18-0.26, 19% !) | 0.26 (0.18-0.29, 17% !) | 0.15 (0.11-0.17, 15% !) | 0.19 (0.12-0.25, 25% !) | 1.06x | 0.61x | 0.77x | 6.52 | 6.35 | 4.91 | 5.46 | 0.6 | 0.5 | 0 | 1 | 0 |
| seekrandom | 5.05 (5.01-5.11, 1%) | 5.22 (5.10-5.81, 5%) | 2.91 (2.57-3.16, 8%) | 2.84 (2.73-3.02, 4%) | 1.03x | 0.58x | 0.56x | 63.04 | 65.21 | 43.50 | 42.02 | 3.5 | 0.6 | 0 | 20 | 2 |
| readseq ! | 0.14 (0.12-0.22, 24% !) | 0.15 (0.14-0.17, 8%) | 0.10 (0.08-0.11, 12% !) | 0.08 (0.07-0.10, 12% !) | 1.02x | 0.69x | 0.53x | 4.56 | 4.56 | 2.13 | 3.34 | 36.0 | 0.6 | 0 | 13 | 1 |
| readreverse ! | 0.16 (0.14-0.17, 8%) | 0.19 (0.18-0.20, 5%) | 0.09 (0.08-0.11, 9%) | 0.10 (0.09-0.14, 18% !) | 1.16x | 0.57x | 0.62x | 4.43 | 4.46 | 3.26 | 3.10 | 0.7 | 0.6 | 0 | 0 | 0 |
| multireadrandom | 1.79 (1.65-1.95, 6%) | 1.76 (1.65-1.79, 3%) | 1.15 (0.99-1.26, 9%) | 1.07 (1.00-1.10, 4%) | 0.99x | 0.64x | 0.60x | 252 | 243 | 148 | 151 | 0.9 | 0.8 | 0 | 1 | 1 |
| readwhilewriting ! | 2.73 (2.49-3.04, 8%) | 2.78 (2.60-3.16, 8%) | 2.02 (1.86-2.65, 15% !) | 1.95 (1.24-2.02, 18% !) | 1.02x | 0.74x | 0.72x | 39.44 | 40.92 | 18.92 | 18.38 | 0.7 | 0.6 | 0 | 0 | 2 |
| compact ! | 411.9 ms (355.4 ms-542.6 ms, 16% !) | 396.0 ms (358.1 ms-453.0 ms, 10%) | 308.7 ms (304.5 ms-445.7 ms, 18% !) | 296.0 ms (193.2 ms-355.3 ms, 21% !) | 0.96x | 0.75x | 0.72x | - | - | - | - | 30830880.0 | 38304936.0 | 0 | 27 | 11 |
| deleterandom ! | 3.89 (3.63-4.07, 5%) | 4.07 (3.79-4.36, 6%) | 3.36 (3.12-5.03, 21% !) | 3.42 (3.41-3.80, 5%) | 1.05x | 0.86x | 0.88x | 49.17 | 55.50 | 40.43 | 38.37 | 0.5 | 12.8 | 0 | 1 | 2 |
| fillsync ! | 105 (81.80-133, 17% !) | 150 (116-206, 24% !) | 131 (113-198, 26% !) | 84.64 (78.70-113, 15% !) | 1.44x | 1.25x | 0.81x | 1491 | 1720 | 1922 | 1343 | 518.7 | 552.3 | 0 | 0 | 0 |

## num=300000, value=100B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 1.90 (1.78-2.03, 5%) | 1.95 (1.73-2.35, 12% !) | 1.06 (1.00-1.12, 5%) | 1.12 (1.05-1.57, 19% !) | 1.03x | 0.55x | 0.59x | 6.73 | 7.34 | 3.65 | 4.18 | 0.2 | 0.2 | 1 | 1 | 4 |
| fillrandom ! | 4.65 (4.51-5.18, 6%) | 5.05 (4.27-5.69, 10% !) | 3.32 (3.11-3.66, 7%) | 3.84 (3.13-4.06, 10%) | 1.09x | 0.71x | 0.83x | 16.33 | 18.31 | 10.16 | 14.53 | 0.2 | 0.2 | 0 | 0 | 0 |
| overwrite ! | 5.55 (4.92-5.67, 6%) | 5.09 (5.00-5.84, 7%) | 4.03 (3.43-4.54, 11% !) | 3.74 (3.64-4.01, 4%) | 0.92x | 0.73x | 0.67x | 19.94 | 18.56 | 14.33 | 12.30 | 90.2 | 47.9 | 0 | 33 | 34 |
| flush ! | 111.2 ms (103.6 ms-131.0 ms, 9%) | 111.9 ms (101.5 ms-137.8 ms, 12% !) | 118.3 ms (111.1 ms-138.2 ms, 9%) | 122.0 ms (111.4 ms-130.9 ms, 7%) | 1.01x | 1.06x | 1.10x | - | - | - | - | 253864.0 | 253544.0 | 0 | 8 | 2 |
| readrandom | 4.54 (4.41-5.36, 8%) | 4.62 (4.44-5.09, 6%) | 3.14 (3.07-3.68, 8%) | 3.70 (3.46-4.00, 6%) | 1.02x | 0.69x | 0.82x | 16.15 | 16.39 | 10.42 | 12.75 | 28.9 | 28.9 | 0 | 41 | 70 |
| readmissing ! | 0.70 (0.67-0.95, 18% !) | 0.73 (0.69-0.79, 5%) | 0.35 (0.33-0.45, 14% !) | 0.45 (0.39-0.61, 17% !) | 1.04x | 0.50x | 0.64x | 5.85 | 6.03 | 4.25 | 4.97 | 0.2 | 0.2 | 0 | 1 | 0 |
| seekrandom ! | 13.22 (12.33-14.12, 5%) | 14.81 (13.02-18.13, 13% !) | 7.94 (7.64-8.71, 5%) | 8.71 (8.22-9.05, 4%) | 1.12x | 0.60x | 0.66x | 48.86 | 49.43 | 34.49 | 38.20 | 0.3 | 0.2 | 0 | 15 | 1 |
| readseq ! | 0.44 (0.41-0.52, 10%) | 0.55 (0.51-0.84, 22% !) | 0.26 (0.25-0.36, 17% !) | 0.26 (0.25-0.27, 2%) | 1.26x | 0.60x | 0.58x | 4.17 | 5.14 | 2.44 | 3.06 | 8.1 | 0.2 | 0 | 14 | 0 |
| readreverse ! | 0.54 (0.49-0.66, 11% !) | 0.65 (0.62-0.93, 19% !) | 0.32 (0.31-0.53, 26% !) | 0.33 (0.31-0.43, 14% !) | 1.20x | 0.59x | 0.62x | 4.34 | 5.53 | 3.15 | 3.30 | 0.3 | 0.2 | 0 | 0 | 0 |
| multireadrandom ! | 4.17 (3.75-4.33, 6%) | 4.29 (4.07-4.56, 5%) | 3.18 (2.98-3.41, 6%) | 3.23 (3.01-4.09, 13% !) | 1.03x | 0.76x | 0.77x | 157 | 161 | 122 | 126 | 0.3 | 0.3 | 0 | 0 | 0 |
| readwhilewriting ! | 6.94 (6.59-8.73, 12% !) | 6.87 (6.70-8.15, 8%) | 5.45 (5.00-5.94, 7%) | 5.72 (5.27-6.27, 7%) | 0.99x | 0.78x | 0.82x | 27.65 | 28.13 | 18.39 | 19.41 | 0.3 | 0.3 | 0 | 0 | 0 |
| compact ! | 581.1 ms (529.5 ms-614.1 ms, 6%) | 563.4 ms (431.2 ms-613.8 ms, 13% !) | 490.9 ms (444.2 ms-600.1 ms, 12% !) | 537.4 ms (456.7 ms-602.9 ms, 10%) | 0.97x | 0.84x | 0.92x | - | - | - | - | 34832376.0 | 38078072.0 | 0 | 38 | 17 |
| deleterandom ! | 3.44 (3.20-3.72, 6%) | 3.67 (3.39-4.10, 8%) | 2.17 (2.10-2.74, 12% !) | 2.16 (1.99-2.90, 16% !) | 1.07x | 0.63x | 0.63x | 11.90 | 12.71 | 7.45 | 7.18 | 0.2 | 0.2 | 0 | 0 | 0 |
| fillsync ! | 182 (154-268, 22% !) | 227 (185-296, 18% !) | 205 (149-302, 30% !) | 178 (153-280, 26% !) | 1.24x | 1.13x | 0.98x | 557 | 852 | 762 | 759 | 206.0 | 204.3 | 0 | 0 | 0 |

## num=300000, value=100B, threads=4, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 3.58 (3.26-4.02, 9%) | 3.82 (3.37-4.00, 8%) | 2.82 (2.62-3.12, 8%) | 3.22 (2.89-3.25, 5%) | 1.07x | 0.79x | 0.90x | 45.41 | 54.50 | 34.84 | 39.69 | 0.5 | 14.7 | 1 | 2 | 11 |
| fillrandom ! | 4.78 (4.41-5.90, 12% !) | 4.93 (4.69-5.60, 8%) | 4.44 (4.31-4.74, 4%) | 4.54 (4.10-5.01, 7%) | 1.03x | 0.93x | 0.95x | 59.99 | 65.50 | 60.79 | 54.85 | 0.5 | 12.2 | 10 | 1 | 5 |
| overwrite | 5.38 (5.18-6.41, 9%) | 5.53 (5.00-5.99, 7%) | 4.61 (4.42-5.20, 7%) | 4.74 (4.48-5.38, 8%) | 1.03x | 0.86x | 0.88x | 70.30 | 69.78 | 62.38 | 57.39 | 122.0 | 136.8 | 0 | 80 | 77 |
| flush ! | 102.2 ms (89.7 ms-104.3 ms, 6%) | 94.4 ms (85.6 ms-107.2 ms, 10% !) | 104.6 ms (100.0 ms-147.6 ms, 17% !) | 103.8 ms (100.9 ms-131.8 ms, 14% !) | 0.92x | 1.02x | 1.02x | - | - | - | - | 81320.0 | 171080.0 | 0 | 35 | 8 |
| readrandom | 2.10 (1.91-2.23, 6%) | 2.10 (1.96-2.17, 5%) | 1.34 (1.25-1.44, 6%) | 1.43 (1.33-1.65, 8%) | 1.00x | 0.64x | 0.68x | 36.24 | 36.94 | 15.11 | 17.46 | 29.4 | 29.3 | 0 | 39 | 68 |
| readmissing ! | 0.29 (0.17-0.34, 24% !) | 0.32 (0.24-0.39, 23% !) | 0.17 (0.15-0.23, 18% !) | 0.20 (0.14-0.22, 15% !) | 1.11x | 0.59x | 0.69x | 7.57 | 7.73 | 5.35 | 5.65 | 0.6 | 0.5 | 0 | 0 | 0 |
| seekrandom ! | 5.78 (5.33-5.98, 5%) | 5.90 (5.43-7.20, 12% !) | 2.98 (2.87-3.53, 9%) | 3.01 (2.90-3.11, 3%) | 1.02x | 0.52x | 0.52x | 69.57 | 72.05 | 41.09 | 40.44 | 3.5 | 0.6 | 0 | 16 | 2 |
| readseq ! | 0.12 (0.12-0.15, 9%) | 0.15 (0.14-0.20, 15% !) | 0.09 (0.07-0.10, 10% !) | 0.08 (0.08-0.10, 10%) | 1.23x | 0.69x | 0.64x | 4.93 | 5.49 | 2.48 | 4.65 | 33.3 | 0.6 | 0 | 12 | 2 |
| readreverse ! | 0.17 (0.15-0.19, 10%) | 0.20 (0.17-0.23, 11% !) | 0.10 (0.09-0.12, 10% !) | 0.09 (0.09-0.10, 1%) | 1.19x | 0.62x | 0.56x | 4.84 | 4.86 | 3.96 | 4.42 | 0.8 | 0.6 | 0 | 0 | 0 |
| multireadrandom ! | 1.95 (1.84-2.01, 3%) | 2.05 (1.84-2.38, 11% !) | 1.13 (1.09-1.21, 5%) | 1.14 (1.09-1.31, 9%) | 1.05x | 0.58x | 0.58x | 240 | 323 | 151 | 154 | 1.0 | 0.8 | 0 | 2 | 3 |
| readwhilewriting ! | 2.80 (2.61-2.98, 5%) | 2.86 (2.48-2.94, 7%) | 2.11 (2.09-2.23, 3%) | 2.01 (1.44-2.10, 14% !) | 1.02x | 0.75x | 0.72x | 41.06 | 44.36 | 18.59 | 18.58 | 0.7 | 0.6 | 0 | 1 | 2 |
| compact ! | 367.3 ms (347.8 ms-373.3 ms, 3%) | 377.1 ms (347.9 ms-416.4 ms, 7%) | 324.7 ms (315.1 ms-379.7 ms, 9%) | 302.3 ms (207.7 ms-324.3 ms, 17% !) | 1.03x | 0.88x | 0.82x | - | - | - | - | 33994400.0 | 38576864.0 | 0 | 59 | 50 |
| deleterandom ! | 4.00 (3.17-4.05, 10% !) | 3.98 (3.69-4.40, 8%) | 3.25 (3.24-4.03, 10% !) | 3.63 (3.32-3.65, 4%) | 1.00x | 0.81x | 0.91x | 55.75 | 51.84 | 44.03 | 41.32 | 0.5 | 12.9 | 0 | 0 | 2 |
| fillsync ! | 132 (103-173, 19% !) | 119 (94.78-215, 38% !) | 171 (109-213, 27% !) | 88.61 (83.12-180, 37% !) | 0.90x | 1.30x | 0.67x | 1835 | 1278 | 1982 | 1376 | 518.7 | 552.7 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 3.89 (3.66-4.01, 4%) | 3.99 (3.84-4.68, 8%) | 2.22 (2.08-2.31, 5%) | 2.49 (2.12-3.77, 23% !) | 1.03x | 0.57x | 0.64x | 12.46 | 14.26 | 7.83 | 10.02 | 4.1 | 14.6 | 1 | 4 | 9 |
| fillrandom ! | 6.50 (5.56-7.60, 13% !) | 6.49 (6.22-7.42, 7%) | 3.67 (3.55-4.89, 14% !) | 4.78 (3.68-5.69, 16% !) | 1.00x | 0.56x | 0.73x | 22.83 | 23.95 | 11.98 | 14.65 | 0.8 | 2.1 | 0 | 1 | 3 |
| overwrite ! | 6.49 (5.50-6.62, 9%) | 7.62 (5.75-7.94, 15% !) | 5.95 (5.25-6.76, 10% !) | 5.86 (5.02-7.09, 14% !) | 1.17x | 0.92x | 0.90x | 23.94 | 31.56 | 15.55 | 13.50 | 14.2 | 16.9 | 0 | 5 | 4 |
| flush ! | 39.4 ms (38.5 ms-50.0 ms, 12% !) | 41.6 ms (40.4 ms-51.9 ms, 11% !) | 81.2 ms (72.0 ms-86.5 ms, 7%) | 81.1 ms (75.4 ms-97.1 ms, 10%) | 1.06x | 2.06x | 2.06x | - | - | - | - | 56192.0 | 55792.0 | 0 | 1 | 8 |
| readrandom ! | 4.54 (4.40-6.39, 17% !) | 4.79 (4.55-5.55, 8%) | 3.50 (3.26-4.10, 10%) | 3.71 (3.20-4.30, 11% !) | 1.06x | 0.77x | 0.82x | 17.83 | 23.66 | 12.65 | 11.90 | 86.0 | 85.9 | 0 | 2 | 2 |
| readmissing ! | 0.52 (0.50-0.60, 7%) | 0.59 (0.52-0.78, 17% !) | 0.29 (0.26-0.40, 17% !) | 0.30 (0.29-0.40, 14% !) | 1.12x | 0.56x | 0.57x | 3.61 | 4.22 | 2.15 | 1.38 | 0.7 | 0.6 | 0 | 0 | 0 |
| seekrandom ! | 14.12 (13.16-16.07, 8%) | 16.01 (14.75-17.37, 6%) | 17.08 (15.83-20.56, 10% !) | 10.56 (9.45-11.74, 8%) | 1.13x | 1.21x | 0.75x | 56.85 | 60.09 | 64.14 | 47.09 | 3.3 | 0.7 | 1 | 11 | 13 |
| readseq ! | 0.93 (0.91-1.33, 18% !) | 1.07 (1.05-1.55, 18% !) | 0.77 (0.58-1.08, 23% !) | 0.69 (0.67-0.74, 4%) | 1.16x | 0.83x | 0.75x | 4.51 | 4.85 | 4.28 | 3.27 | 7.2 | 0.6 | 0 | 0 | 0 |
| readreverse ! | 1.37 (1.16-1.40, 9%) | 1.40 (1.22-1.82, 16% !) | 0.73 (0.70-1.06, 19% !) | 0.81 (0.77-0.86, 4%) | 1.02x | 0.54x | 0.60x | 5.91 | 5.57 | 3.55 | 3.57 | 0.7 | 0.6 | 0 | 1 | 2 |
| multireadrandom ! | 4.15 (3.77-4.46, 7%) | 3.95 (3.59-4.18, 5%) | 3.15 (3.08-3.90, 10% !) | 3.15 (2.97-3.70, 9%) | 0.95x | 0.76x | 0.76x | 142 | 137 | 124 | 122 | 1.0 | 1.0 | 0 | 0 | 0 |
| readwhilewriting ! | 7.10 (6.75-8.84, 13% !) | 7.08 (6.61-7.93, 7%) | 6.29 (6.06-7.59, 9%) | 6.67 (5.95-7.29, 7%) | 1.00x | 0.89x | 0.94x | 29.57 | 26.48 | 25.67 | 28.50 | 3.5 | 1.5 | 3 | 5 | 7 |
| compact ! | 563.9 ms (559.4 ms-630.0 ms, 5%) | 556.6 ms (483.2 ms-650.6 ms, 11% !) | 453.5 ms (434.4 ms-557.0 ms, 11% !) | 506.4 ms (438.7 ms-537.3 ms, 8%) | 0.99x | 0.80x | 0.90x | - | - | - | - | 690568.0 | 3567944.0 | 0 | 12 | 13 |
| deleterandom ! | 2.90 (2.39-3.86, 21% !) | 2.68 (2.51-2.72, 4%) | 1.54 (1.50-2.03, 14% !) | 1.74 (1.61-2.10, 11% !) | 0.92x | 0.53x | 0.60x | 11.02 | 8.52 | 5.35 | 5.38 | 0.6 | 0.6 | 0 | 3 | 0 |
| fillsync ! | 240 (202-309, 16% !) | 279 (173-305, 26% !) | 249 (199-342, 21% !) | 247 (167-281, 19% !) | 1.17x | 1.04x | 1.03x | 713 | 918 | 778 | 860 | 618.0 | 612.8 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 6.68 (6.30-7.61, 7%) | 6.89 (6.43-7.57, 7%) | 5.03 (4.76-5.46, 6%) | 5.13 (4.58-5.87, 11% !) | 1.03x | 0.75x | 0.77x | 88.48 | 93.15 | 130 | 69.12 | 12.3 | 27.2 | 1 | 5 | 13 |
| fillrandom | 7.76 (7.52-8.80, 7%) | 7.88 (7.56-8.92, 7%) | 6.32 (5.75-6.95, 7%) | 6.39 (5.93-7.22, 7%) | 1.02x | 0.81x | 0.82x | 96.68 | 98.55 | 143 | 88.21 | 3.3 | 16.8 | 0 | 5 | 0 |
| overwrite | 7.71 (7.59-7.89, 2%) | 7.88 (7.37-8.21, 4%) | 7.17 (6.72-8.14, 8%) | 8.15 (6.63-8.36, 9%) | 1.02x | 0.93x | 1.06x | 93.18 | 89.28 | 161 | 112 | 10.7 | 25.8 | 0 | 4 | 4 |
| flush | 39.8 ms (37.1 ms-44.9 ms, 7%) | 39.6 ms (35.6 ms-43.6 ms, 8%) | 79.3 ms (67.7 ms-84.5 ms, 9%) | 88.3 ms (80.0 ms-101.0 ms, 9%) | 0.99x | 1.99x | 2.22x | - | - | - | - | 55080.0 | 127784.0 | 0 | 1 | 1 |
| readrandom ! | 2.58 (2.12-2.96, 12% !) | 2.51 (2.39-2.76, 6%) | 1.55 (1.33-1.74, 11% !) | 1.48 (1.36-1.64, 7%) | 0.97x | 0.60x | 0.57x | 40.35 | 42.72 | 16.48 | 18.69 | 87.5 | 87.2 | 0 | 4 | 8 |
| readmissing ! | 0.17 (0.14-0.24, 20% !) | 0.22 (0.15-0.23, 18% !) | 0.14 (0.10-0.17, 24% !) | 0.16 (0.13-0.19, 14% !) | 1.29x | 0.81x | 0.92x | 3.65 | 3.85 | 2.39 | 2.88 | 1.7 | 1.6 | 0 | 0 | 0 |
| seekrandom | 7.57 (7.40-7.74, 2%) | 8.39 (7.91-8.68, 4%) | 5.71 (5.48-5.95, 3%) | 4.65 (4.17-4.81, 6%) | 1.11x | 0.75x | 0.61x | 85.66 | 94.54 | 69.08 | 55.82 | 12.5 | 1.7 | 0 | 11 | 13 |
| readseq ! | 0.42 (0.41-0.45, 4%) | 0.45 (0.42-0.50, 7%) | 0.27 (0.25-0.50, 34% !) | 0.35 (0.32-0.35, 3%) | 1.08x | 0.64x | 0.84x | 21.60 | 22.23 | 9.05 | 11.24 | 45.8 | 2.0 | 0 | 0 | 1 |
| readreverse ! | 0.49 (0.41-0.58, 13% !) | 0.48 (0.46-0.51, 4%) | 0.34 (0.33-0.41, 9%) | 0.33 (0.33-0.35, 3%) | 0.98x | 0.70x | 0.68x | 21.23 | 19.80 | 8.55 | 8.01 | 2.4 | 2.1 | 0 | 1 | 2 |
| multireadrandom ! | 1.90 (1.80-2.21, 8%) | 1.99 (1.91-2.40, 10%) | 1.41 (1.16-1.71, 14% !) | 1.44 (1.18-1.51, 10%) | 1.05x | 0.75x | 0.76x | 261 | 314 | 201 | 183 | 3.3 | 2.7 | 0 | 0 | 0 |
| readwhilewriting ! | 2.90 (2.00-4.00, 25% !) | 3.11 (2.91-3.33, 6%) | 2.26 (2.12-2.45, 6%) | 2.32 (1.95-2.51, 9%) | 1.07x | 0.78x | 0.80x | 43.35 | 48.96 | 19.84 | 23.42 | 2.1 | 1.9 | 0 | 0 | 0 |
| compact ! | 529.0 ms (152.1 ms-1400.5 ms, 74% !) | 544.1 ms (497.9 ms-567.0 ms, 5%) | 459.4 ms (383.4 ms-508.4 ms, 10% !) | 433.3 ms (388.2 ms-439.9 ms, 5%) | 1.03x | 0.87x | 0.82x | - | - | - | - | 377440.0 | 1302872.0 | 0 | 9 | 10 |
| deleterandom ! | 3.51 (3.13-4.16, 11% !) | 3.66 (3.20-4.13, 9%) | 2.96 (2.83-4.22, 18% !) | 3.22 (3.01-3.26, 3%) | 1.04x | 0.85x | 0.92x | 50.94 | 49.99 | 39.58 | 34.63 | 1.6 | 15.6 | 0 | 3 | 1 |
| fillsync ! | 182 (118-268, 29% !) | 171 (131-227, 22% !) | 184 (145-226, 16% !) | 148 (124-188, 16% !) | 0.94x | 1.01x | 0.82x | 2720 | 2032 | 2048 | 2032 | 1556.2 | 1589.6 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=1, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq | 3.90 (3.83-4.11, 3%) | 3.95 (3.78-4.33, 6%) | 2.04 (2.01-2.39, 8%) | 2.17 (2.15-2.35, 4%) | 1.01x | 0.52x | 0.55x | 13.03 | 12.82 | 6.44 | 7.57 | 3.8 | 6.0 | 1 | 2 | 3 |
| fillrandom ! | 6.28 (5.74-6.98, 7%) | 6.13 (5.65-6.85, 8%) | 3.80 (3.68-5.44, 18% !) | 3.96 (3.46-4.24, 9%) | 0.98x | 0.60x | 0.63x | 22.78 | 22.86 | 12.54 | 12.73 | 3.3 | 2.1 | 0 | 1 | 1 |
| overwrite ! | 6.46 (5.91-6.56, 4%) | 6.39 (5.62-6.86, 8%) | 5.27 (4.84-6.41, 11% !) | 5.58 (4.75-6.10, 10% !) | 0.99x | 0.82x | 0.86x | 22.10 | 20.45 | 13.72 | 14.41 | 4.4 | 2.4 | 0 | 4 | 3 |
| flush ! | 55.6 ms (44.1 ms-59.1 ms, 13% !) | 45.1 ms (41.8 ms-52.1 ms, 9%) | 87.9 ms (84.4 ms-153.4 ms, 29% !) | 89.3 ms (81.5 ms-95.1 ms, 6%) | 0.81x | 1.58x | 1.60x | - | - | - | - | 48768.0 | 54376.0 | 0 | 1 | 3 |
| readrandom ! | 5.60 (5.04-6.50, 9%) | 5.51 (4.66-6.30, 12% !) | 3.75 (3.51-4.77, 13% !) | 4.08 (3.86-5.04, 12% !) | 0.98x | 0.67x | 0.73x | 26.23 | 24.74 | 12.34 | 14.17 | 95.9 | 95.8 | 0 | 6 | 6 |
| readmissing ! | 0.55 (0.48-0.58, 7%) | 0.59 (0.51-0.62, 7%) | 0.26 (0.24-0.36, 17% !) | 0.30 (0.29-0.37, 11% !) | 1.07x | 0.47x | 0.54x | 3.26 | 4.15 | 1.12 | 1.63 | 0.7 | 0.6 | 0 | 0 | 0 |
| seekrandom | 15.12 (14.71-15.86, 3%) | 15.56 (14.57-17.92, 8%) | 14.83 (14.22-16.25, 6%) | 10.80 (10.49-11.32, 3%) | 1.03x | 0.98x | 0.71x | 55.03 | 58.24 | 57.66 | 44.33 | 3.3 | 0.7 | 1 | 4 | 6 |
| readseq ! | 1.08 (0.98-1.57, 20% !) | 1.16 (1.02-1.44, 14% !) | 0.66 (0.57-0.72, 9%) | 0.73 (0.66-0.74, 5%) | 1.07x | 0.61x | 0.67x | 5.74 | 7.04 | 2.70 | 3.43 | 2.0 | 0.6 | 0 | 0 | 0 |
| readreverse ! | 1.15 (1.13-1.58, 16% !) | 1.50 (1.22-2.02, 21% !) | 0.81 (0.73-0.96, 12% !) | 0.84 (0.79-1.01, 10%) | 1.30x | 0.70x | 0.73x | 5.50 | 7.11 | 3.78 | 3.86 | 0.7 | 0.6 | 0 | 0 | 1 |
| multireadrandom ! | 3.89 (3.65-4.26, 6%) | 4.10 (3.77-5.09, 12% !) | 3.29 (3.08-4.00, 11% !) | 3.54 (3.20-4.10, 9%) | 1.06x | 0.85x | 0.91x | 150 | 147 | 121 | 129 | 1.1 | 1.0 | 0 | 0 | 0 |
| readwhilewriting ! | 7.90 (7.32-8.61, 7%) | 7.69 (7.31-8.54, 7%) | 6.32 (5.73-6.88, 7%) | 6.44 (6.04-8.06, 13% !) | 0.97x | 0.80x | 0.81x | 33.15 | 32.51 | 23.55 | 28.43 | 3.1 | 2.3 | 0 | 4 | 17 |
| compact ! | 569.4 ms (530.9 ms-676.9 ms, 10%) | 609.1 ms (506.4 ms-615.0 ms, 8%) | 561.2 ms (461.4 ms-617.6 ms, 13% !) | 526.6 ms (485.1 ms-635.3 ms, 12% !) | 1.07x | 0.99x | 0.92x | - | - | - | - | 1011488.0 | 8150920.0 | 0 | 23 | 18 |
| deleterandom | 2.64 (2.44-2.75, 5%) | 3.10 (2.64-3.34, 9%) | 1.53 (1.44-1.75, 8%) | 1.51 (1.41-1.71, 8%) | 1.17x | 0.58x | 0.57x | 8.78 | 10.29 | 5.65 | 4.47 | 0.6 | 0.6 | 0 | 2 | 0 |
| fillsync ! | 250 (234-439, 30% !) | 251 (196-371, 24% !) | 270 (205-316, 18% !) | 219 (197-269, 13% !) | 1.00x | 1.08x | 0.87x | 688 | 737 | 991 | 541 | 618.0 | 612.8 | 0 | 0 | 0 |

## num=100000, value=1024B, threads=4, compression=lz4

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillseq ! | 7.13 (6.31-8.02, 9%) | 6.45 (6.24-7.11, 5%) | 5.43 (5.15-5.97, 6%) | 4.30 (3.93-5.37, 14% !) | 0.90x | 0.76x | 0.60x | 91.81 | 79.53 | 183 | 65.23 | 24.1 | 30.1 | 1 | 23 | 7 |
| fillrandom | 8.08 (7.56-8.73, 6%) | 7.84 (7.31-8.67, 7%) | 6.65 (6.05-7.47, 9%) | 5.96 (5.50-6.64, 8%) | 0.97x | 0.82x | 0.74x | 96.77 | 102 | 169 | 86.91 | 2.0 | 13.7 | 0 | 7 | 3 |
| overwrite ! | 7.59 (6.74-8.71, 10%) | 8.00 (7.62-8.87, 6%) | 8.07 (6.56-8.77, 11% !) | 6.56 (6.31-7.97, 12% !) | 1.05x | 1.06x | 0.86x | 86.78 | 95.96 | 189 | 75.14 | 4.7 | 14.9 | 0 | 4 | 4 |
| flush ! | 51.2 ms (42.4 ms-66.7 ms, 20% !) | 46.3 ms (43.4 ms-51.6 ms, 8%) | 82.0 ms (80.3 ms-118.7 ms, 18% !) | 88.7 ms (82.6 ms-105.0 ms, 10%) | 0.91x | 1.60x | 1.73x | - | - | - | - | 56232.0 | 56608.0 | 0 | 35 | 3 |
| readrandom ! | 2.68 (2.61-2.93, 5%) | 2.67 (2.39-2.85, 6%) | 1.67 (1.40-2.03, 15% !) | 1.67 (1.40-1.98, 13% !) | 0.99x | 0.62x | 0.62x | 43.78 | 42.61 | 19.26 | 23.90 | 97.4 | 96.9 | 0 | 6 | 7 |
| readmissing ! | 0.15 (0.13-0.15, 5%) | 0.19 (0.14-0.25, 23% !) | 0.16 (0.08-0.17, 23% !) | 0.17 (0.15-0.20, 10%) | 1.29x | 1.08x | 1.20x | 3.19 | 3.71 | 2.52 | 3.10 | 1.9 | 1.6 | 0 | 0 | 1 |
| seekrandom ! | 8.07 (7.34-8.83, 7%) | 8.14 (7.68-8.88, 5%) | 5.65 (5.29-5.97, 5%) | 4.86 (4.21-5.53, 11% !) | 1.01x | 0.70x | 0.60x | 94.01 | 90.75 | 64.68 | 53.81 | 9.9 | 1.7 | 0 | 26 | 41 |
| readseq | 0.46 (0.45-0.51, 6%) | 0.53 (0.47-0.54, 6%) | 0.29 (0.27-0.33, 8%) | 0.37 (0.36-0.39, 2%) | 1.16x | 0.63x | 0.80x | 23.31 | 24.94 | 9.42 | 11.57 | 52.3 | 2.0 | 0 | 0 | 1 |
| readreverse | 0.46 (0.45-0.50, 4%) | 0.52 (0.47-0.58, 9%) | 0.37 (0.35-0.38, 3%) | 0.36 (0.34-0.37, 4%) | 1.14x | 0.80x | 0.79x | 20.12 | 22.05 | 8.94 | 8.33 | 3.4 | 2.3 | 0 | 4 | 9 |
| multireadrandom ! | 2.06 (1.87-2.21, 7%) | 2.20 (1.96-2.41, 7%) | 1.55 (1.34-1.81, 11% !) | 1.40 (1.31-1.50, 5%) | 1.07x | 0.75x | 0.68x | 287 | 290 | 180 | 179 | 3.5 | 2.7 | 0 | 0 | 0 |
| readwhilewriting ! | 3.06 (3.05-3.37, 4%) | 3.33 (2.98-3.49, 7%) | 2.41 (2.17-2.74, 9%) | 2.35 (1.55-2.61, 18% !) | 1.09x | 0.79x | 0.77x | 42.03 | 48.56 | 25.09 | 22.58 | 2.3 | 1.9 | 0 | 1 | 0 |
| compact ! | 551.5 ms (450.4 ms-624.2 ms, 12% !) | 565.0 ms (546.7 ms-629.0 ms, 7%) | 607.0 ms (410.2 ms-821.9 ms, 26% !) | 523.5 ms (369.3 ms-585.1 ms, 18% !) | 1.02x | 1.10x | 0.95x | - | - | - | - | 1298960.0 | 2338016.0 | 0 | 13 | 9 |
| deleterandom ! | 3.77 (3.25-4.33, 11% !) | 3.54 (3.29-4.24, 10%) | 3.35 (3.21-4.26, 13% !) | 3.50 (3.26-3.82, 6%) | 0.94x | 0.89x | 0.93x | 53.14 | 49.10 | 49.56 | 41.35 | 1.6 | 15.7 | 0 | 3 | 3 |
| fillsync ! | 178 (148-246, 20% !) | 154 (128-266, 33% !) | 219 (192-286, 17% !) | 169 (146-242, 21% !) | 0.86x | 1.23x | 0.95x | 2163 | 2556 | 2473 | 2163 | 1556.2 | 1586.9 | 0 | 0 | 0 |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | rocksdb-sharp µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed-async µs/op median (min-max, CV) | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 3.01 (2.50-3.09, 8%) | 2.80 (2.67-3.37, 10%) | 2.07 (1.78-2.09, 7%) | 2.08 (1.85-2.14, 6%) | 0.93x | 0.69x | 0.69x | 11.60 | 10.34 | 6.81 | 6.65 | 76.9 | 76.8 | 1 | 437 | 573 |
| overwrite ! | 2.61 (2.47-3.86, 20% !) | 2.94 (2.50-3.59, 16% !) | 1.94 (1.80-2.09, 7%) | 2.04 (1.92-2.26, 6%) | 1.13x | 0.74x | 0.78x | 9.01 | 9.85 | 6.09 | 7.10 | 119.1 | 123.8 | 0 | 585 | 515 |
| readrandom ! | 7.29 (7.05-9.28, 12% !) | 7.42 (7.15-10.35, 17% !) | 4.63 (4.48-5.14, 6%) | 5.10 (5.04-5.48, 4%) | 1.02x | 0.64x | 0.70x | 31.64 | 29.45 | 14.96 | 16.21 | 18.9 | 18.9 | 1 | 40 | 63 |
| seekrandom ! | 26.62 (26.10-28.47, 3%) | 30.52 (27.87-33.73, 7%) | 12.45 (9.01-16.45, 24% !) | 12.92 (10.64-20.15, 26% !) | 1.15x | 0.47x | 0.49x | 81.45 | 87.40 | 47.85 | 48.18 | 0.7 | 0.1 | 0 | 6 | 74 |
| waitforcompaction ! | 28.92 (20.29-30.66, 15% !) | 41.66 (40.13-48.35, 8%) | 32.29 (23.57-82.54, 60% !) | 30.93 (25.98-33.11, 9%) | 1.44x | 1.12x | 1.07x | - | - | - | - | 40.0 | 40.0 | 0 | 0 | 0 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| num=300000, value=100B, threads=1, compression=none | fillseq | c-native | 11.2% | 1.78 | 1.71-2.23 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillseq | rocksdb-sharp | 16.2% | 2.06 | 1.81-2.67 | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | c-native | 11.4% | 94.1 ms | 93.7 ms-120.1 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | rocksdb-sharp | 11.5% | 101.0 ms | 92.6 ms-122.3 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | flush | managed | 15.3% | 124.4 ms | 111.4 ms-158.9 ms | 5 |
| num=300000, value=100B, threads=1, compression=none | readrandom | managed-async | 16.8% | 3.28 | 3.05-4.52 | 5 |
| num=300000, value=100B, threads=1, compression=none | readmissing | managed-async | 16.4% | 0.43 | 0.37-0.53 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | c-native | 14.3% | 0.43 | 0.39-0.55 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | managed | 19.7% | 0.27 | 0.24-0.37 | 5 |
| num=300000, value=100B, threads=1, compression=none | readseq | managed-async | 16.3% | 0.26 | 0.23-0.34 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | c-native | 11.7% | 0.51 | 0.46-0.62 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | rocksdb-sharp | 11.1% | 0.68 | 0.59-0.77 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | managed | 13.4% | 0.32 | 0.31-0.42 | 5 |
| num=300000, value=100B, threads=1, compression=none | readreverse | managed-async | 16.8% | 0.30 | 0.27-0.41 | 5 |
| num=300000, value=100B, threads=1, compression=none | multireadrandom | rocksdb-sharp | 10.3% | 3.85 | 3.78-4.76 | 5 |
| num=300000, value=100B, threads=1, compression=none | deleterandom | managed | 13.0% | 2.09 | 2.04-2.63 | 5 |
| num=300000, value=100B, threads=1, compression=none | deleterandom | managed-async | 11.5% | 2.20 | 1.89-2.56 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | c-native | 16.8% | 206 | 138-218 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | managed | 20.2% | 182 | 131-234 | 5 |
| num=300000, value=100B, threads=1, compression=none | fillsync | managed-async | 17.3% | 183 | 132-208 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillseq | c-native | 11.1% | 3.62 | 3.33-4.31 | 5 |
| num=300000, value=100B, threads=4, compression=none | overwrite | rocksdb-sharp | 10.6% | 5.38 | 5.13-6.49 | 5 |
| num=300000, value=100B, threads=4, compression=none | overwrite | managed | 10.0% | 4.89 | 4.18-5.41 | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | c-native | 13.5% | 94.6 ms | 82.5 ms-113.5 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | flush | rocksdb-sharp | 13.4% | 98.3 ms | 79.3 ms-106.2 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | readrandom | rocksdb-sharp | 14.3% | 1.93 | 1.74-2.50 | 5 |
| num=300000, value=100B, threads=4, compression=none | readrandom | managed | 12.0% | 1.19 | 1.07-1.42 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | c-native | 18.5% | 0.24 | 0.18-0.26 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | rocksdb-sharp | 17.4% | 0.26 | 0.18-0.29 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | managed | 15.4% | 0.15 | 0.11-0.17 | 5 |
| num=300000, value=100B, threads=4, compression=none | readmissing | managed-async | 24.6% | 0.19 | 0.12-0.25 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | c-native | 23.8% | 0.14 | 0.12-0.22 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | managed | 12.1% | 0.10 | 0.08-0.11 | 5 |
| num=300000, value=100B, threads=4, compression=none | readseq | managed-async | 11.6% | 0.08 | 0.07-0.10 | 5 |
| num=300000, value=100B, threads=4, compression=none | readreverse | managed-async | 18.1% | 0.10 | 0.09-0.14 | 5 |
| num=300000, value=100B, threads=4, compression=none | readwhilewriting | managed | 14.7% | 2.02 | 1.86-2.65 | 5 |
| num=300000, value=100B, threads=4, compression=none | readwhilewriting | managed-async | 17.8% | 1.95 | 1.24-2.02 | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | c-native | 16.0% | 411.9 ms | 355.4 ms-542.6 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | managed | 17.8% | 308.7 ms | 304.5 ms-445.7 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | compact | managed-async | 20.6% | 296.0 ms | 193.2 ms-355.3 ms | 5 |
| num=300000, value=100B, threads=4, compression=none | deleterandom | managed | 21.4% | 3.36 | 3.12-5.03 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | c-native | 17.4% | 105 | 81.80-133 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | rocksdb-sharp | 24.0% | 150 | 116-206 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | managed | 26.4% | 131 | 113-198 | 5 |
| num=300000, value=100B, threads=4, compression=none | fillsync | managed-async | 15.3% | 84.64 | 78.70-113 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillseq | rocksdb-sharp | 11.9% | 1.95 | 1.73-2.35 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillseq | managed-async | 19.1% | 1.12 | 1.05-1.57 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillrandom | rocksdb-sharp | 10.0% | 5.05 | 4.27-5.69 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | overwrite | managed | 11.1% | 4.03 | 3.43-4.54 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | flush | rocksdb-sharp | 12.0% | 111.9 ms | 101.5 ms-137.8 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | c-native | 17.7% | 0.70 | 0.67-0.95 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | managed | 14.1% | 0.35 | 0.33-0.45 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readmissing | managed-async | 17.5% | 0.45 | 0.39-0.61 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | seekrandom | rocksdb-sharp | 12.7% | 14.81 | 13.02-18.13 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | rocksdb-sharp | 22.5% | 0.55 | 0.51-0.84 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readseq | managed | 16.5% | 0.26 | 0.25-0.36 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | c-native | 11.2% | 0.54 | 0.49-0.66 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | rocksdb-sharp | 18.6% | 0.65 | 0.62-0.93 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | managed | 26.1% | 0.32 | 0.31-0.53 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readreverse | managed-async | 14.3% | 0.33 | 0.31-0.43 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | multireadrandom | managed-async | 13.2% | 3.23 | 3.01-4.09 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | readwhilewriting | c-native | 11.8% | 6.94 | 6.59-8.73 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | compact | rocksdb-sharp | 13.2% | 563.4 ms | 431.2 ms-613.8 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | compact | managed | 11.7% | 490.9 ms | 444.2 ms-600.1 ms | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | deleterandom | managed | 11.6% | 2.17 | 2.10-2.74 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | deleterandom | managed-async | 15.9% | 2.16 | 1.99-2.90 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | c-native | 22.4% | 182 | 154-268 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | rocksdb-sharp | 17.5% | 227 | 185-296 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | managed | 29.7% | 205 | 149-302 | 5 |
| num=300000, value=100B, threads=1, compression=lz4 | fillsync | managed-async | 26.3% | 178 | 153-280 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillrandom | c-native | 12.4% | 4.78 | 4.41-5.90 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | flush | rocksdb-sharp | 10.2% | 94.4 ms | 85.6 ms-107.2 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | flush | managed | 17.5% | 104.6 ms | 100.0 ms-147.6 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | flush | managed-async | 13.9% | 103.8 ms | 100.9 ms-131.8 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | c-native | 23.7% | 0.29 | 0.17-0.34 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | rocksdb-sharp | 23.5% | 0.32 | 0.24-0.39 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | managed | 17.7% | 0.17 | 0.15-0.23 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readmissing | managed-async | 15.4% | 0.20 | 0.14-0.22 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | seekrandom | rocksdb-sharp | 11.9% | 5.90 | 5.43-7.20 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readseq | rocksdb-sharp | 14.7% | 0.15 | 0.14-0.20 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readseq | managed | 10.3% | 0.09 | 0.07-0.10 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readreverse | rocksdb-sharp | 10.8% | 0.20 | 0.17-0.23 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readreverse | managed | 10.2% | 0.10 | 0.09-0.12 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | multireadrandom | rocksdb-sharp | 10.8% | 2.05 | 1.84-2.38 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | readwhilewriting | managed-async | 14.0% | 2.01 | 1.44-2.10 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | compact | managed-async | 16.6% | 302.3 ms | 207.7 ms-324.3 ms | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | deleterandom | c-native | 10.1% | 4.00 | 3.17-4.05 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | deleterandom | managed | 10.1% | 3.25 | 3.24-4.03 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | c-native | 19.4% | 132 | 103-173 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | rocksdb-sharp | 38.1% | 119 | 94.78-215 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | managed | 26.8% | 171 | 109-213 | 5 |
| num=300000, value=100B, threads=4, compression=lz4 | fillsync | managed-async | 36.7% | 88.61 | 83.12-180 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillseq | managed-async | 23.4% | 2.49 | 2.12-3.77 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillrandom | c-native | 12.7% | 6.50 | 5.56-7.60 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillrandom | managed | 14.4% | 3.67 | 3.55-4.89 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillrandom | managed-async | 15.8% | 4.78 | 3.68-5.69 | 5 |
| num=100000, value=1024B, threads=1, compression=none | overwrite | rocksdb-sharp | 14.9% | 7.62 | 5.75-7.94 | 5 |
| num=100000, value=1024B, threads=1, compression=none | overwrite | managed | 10.0% | 5.95 | 5.25-6.76 | 5 |
| num=100000, value=1024B, threads=1, compression=none | overwrite | managed-async | 13.7% | 5.86 | 5.02-7.09 | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | c-native | 11.9% | 39.4 ms | 38.5 ms-50.0 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | flush | rocksdb-sharp | 10.9% | 41.6 ms | 40.4 ms-51.9 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | readrandom | c-native | 16.9% | 4.54 | 4.40-6.39 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readrandom | managed-async | 11.4% | 3.71 | 3.20-4.30 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | rocksdb-sharp | 17.0% | 0.59 | 0.52-0.78 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | managed | 17.3% | 0.29 | 0.26-0.40 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readmissing | managed-async | 14.1% | 0.30 | 0.29-0.40 | 5 |
| num=100000, value=1024B, threads=1, compression=none | seekrandom | managed | 10.3% | 17.08 | 15.83-20.56 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | c-native | 18.0% | 0.93 | 0.91-1.33 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | rocksdb-sharp | 18.3% | 1.07 | 1.05-1.55 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readseq | managed | 22.9% | 0.77 | 0.58-1.08 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readreverse | rocksdb-sharp | 15.6% | 1.40 | 1.22-1.82 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readreverse | managed | 19.0% | 0.73 | 0.70-1.06 | 5 |
| num=100000, value=1024B, threads=1, compression=none | multireadrandom | managed | 10.2% | 3.15 | 3.08-3.90 | 5 |
| num=100000, value=1024B, threads=1, compression=none | readwhilewriting | c-native | 12.7% | 7.10 | 6.75-8.84 | 5 |
| num=100000, value=1024B, threads=1, compression=none | compact | rocksdb-sharp | 11.1% | 556.6 ms | 483.2 ms-650.6 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | compact | managed | 11.0% | 453.5 ms | 434.4 ms-557.0 ms | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | c-native | 21.0% | 2.90 | 2.39-3.86 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | managed | 13.6% | 1.54 | 1.50-2.03 | 5 |
| num=100000, value=1024B, threads=1, compression=none | deleterandom | managed-async | 11.3% | 1.74 | 1.61-2.10 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | c-native | 16.3% | 240 | 202-309 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | rocksdb-sharp | 25.9% | 279 | 173-305 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | managed | 20.6% | 249 | 199-342 | 5 |
| num=100000, value=1024B, threads=1, compression=none | fillsync | managed-async | 18.9% | 247 | 167-281 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillseq | managed-async | 10.7% | 5.13 | 4.58-5.87 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readrandom | c-native | 12.1% | 2.58 | 2.12-2.96 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readrandom | managed | 10.6% | 1.55 | 1.33-1.74 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | c-native | 20.1% | 0.17 | 0.14-0.24 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | rocksdb-sharp | 18.3% | 0.22 | 0.15-0.23 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | managed | 23.6% | 0.14 | 0.10-0.17 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readmissing | managed-async | 14.4% | 0.16 | 0.13-0.19 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readseq | managed | 33.7% | 0.27 | 0.25-0.50 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readreverse | c-native | 12.8% | 0.49 | 0.41-0.58 | 5 |
| num=100000, value=1024B, threads=4, compression=none | multireadrandom | managed | 13.8% | 1.41 | 1.16-1.71 | 5 |
| num=100000, value=1024B, threads=4, compression=none | readwhilewriting | c-native | 24.6% | 2.90 | 2.00-4.00 | 5 |
| num=100000, value=1024B, threads=4, compression=none | compact | c-native | 73.6% | 529.0 ms | 152.1 ms-1400.5 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | compact | managed | 10.2% | 459.4 ms | 383.4 ms-508.4 ms | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | c-native | 11.3% | 3.51 | 3.13-4.16 | 5 |
| num=100000, value=1024B, threads=4, compression=none | deleterandom | managed | 18.1% | 2.96 | 2.83-4.22 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | c-native | 29.5% | 182 | 118-268 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | rocksdb-sharp | 22.3% | 171 | 131-227 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | managed | 16.2% | 184 | 145-226 | 5 |
| num=100000, value=1024B, threads=4, compression=none | fillsync | managed-async | 16.5% | 148 | 124-188 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillrandom | managed | 17.6% | 3.80 | 3.68-5.44 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | overwrite | managed | 10.8% | 5.27 | 4.84-6.41 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | overwrite | managed-async | 10.0% | 5.58 | 4.75-6.10 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | c-native | 13.3% | 55.6 ms | 44.1 ms-59.1 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | flush | managed | 29.1% | 87.9 ms | 84.4 ms-153.4 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readrandom | rocksdb-sharp | 11.8% | 5.51 | 4.66-6.30 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readrandom | managed | 12.6% | 3.75 | 3.51-4.77 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readrandom | managed-async | 11.7% | 4.08 | 3.86-5.04 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | managed | 16.8% | 0.26 | 0.24-0.36 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readmissing | managed-async | 10.8% | 0.30 | 0.29-0.37 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | c-native | 19.5% | 1.08 | 0.98-1.57 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readseq | rocksdb-sharp | 13.8% | 1.16 | 1.02-1.44 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | c-native | 15.6% | 1.15 | 1.13-1.58 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | rocksdb-sharp | 20.9% | 1.50 | 1.22-2.02 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readreverse | managed | 12.1% | 0.81 | 0.73-0.96 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | rocksdb-sharp | 12.3% | 4.10 | 3.77-5.09 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | multireadrandom | managed | 10.9% | 3.29 | 3.08-4.00 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | readwhilewriting | managed-async | 13.2% | 6.44 | 6.04-8.06 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | compact | managed | 12.6% | 561.2 ms | 461.4 ms-617.6 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | compact | managed-async | 12.5% | 526.6 ms | 485.1 ms-635.3 ms | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | c-native | 30.1% | 250 | 234-439 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | rocksdb-sharp | 23.9% | 251 | 196-371 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | managed | 17.6% | 270 | 205-316 | 5 |
| num=100000, value=1024B, threads=1, compression=lz4 | fillsync | managed-async | 13.2% | 219 | 197-269 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillseq | managed-async | 14.4% | 4.30 | 3.93-5.37 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | overwrite | managed | 11.1% | 8.07 | 6.56-8.77 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | overwrite | managed-async | 11.8% | 6.56 | 6.31-7.97 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | c-native | 20.2% | 51.2 ms | 42.4 ms-66.7 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | flush | managed | 18.4% | 82.0 ms | 80.3 ms-118.7 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readrandom | managed | 15.0% | 1.67 | 1.40-2.03 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readrandom | managed-async | 13.3% | 1.67 | 1.40-1.98 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | rocksdb-sharp | 23.3% | 0.19 | 0.14-0.25 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readmissing | managed | 23.4% | 0.16 | 0.08-0.17 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | seekrandom | managed-async | 10.6% | 4.86 | 4.21-5.53 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | multireadrandom | managed | 10.9% | 1.55 | 1.34-1.81 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | readwhilewriting | managed-async | 18.0% | 2.35 | 1.55-2.61 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | compact | c-native | 11.9% | 551.5 ms | 450.4 ms-624.2 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | compact | managed | 26.1% | 607.0 ms | 410.2 ms-821.9 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | compact | managed-async | 17.9% | 523.5 ms | 369.3 ms-585.1 ms | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | deleterandom | c-native | 10.8% | 3.77 | 3.25-4.33 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | deleterandom | managed | 12.6% | 3.35 | 3.21-4.26 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | c-native | 20.2% | 178 | 148-246 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | rocksdb-sharp | 33.3% | 154 | 128-266 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | managed | 17.5% | 219 | 192-286 | 5 |
| num=100000, value=1024B, threads=4, compression=lz4 | fillsync | managed-async | 21.0% | 169 | 146-242 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | overwrite | c-native | 19.7% | 2.61 | 2.47-3.86 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | overwrite | rocksdb-sharp | 15.7% | 2.94 | 2.50-3.59 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | readrandom | c-native | 11.9% | 7.29 | 7.05-9.28 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | readrandom | rocksdb-sharp | 16.7% | 7.42 | 7.15-10.35 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | seekrandom | managed | 24.1% | 12.45 | 9.01-16.45 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | seekrandom | managed-async | 26.2% | 12.92 | 10.64-20.15 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | waitforcompaction | c-native | 15.3% | 28.92 | 20.29-30.66 | 5 |
| compaction: num=500000, value=100B, threads=1, compression=none | waitforcompaction | managed | 60.1% | 32.29 | 23.57-82.54 | 5 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

Over the repetitions: write-amp median (min-max), and the most frequent LSM shape with the number of repetitions that had it.

| configuration | engine | runs | write-amp median (min-max) | written MB median | most frequent files per level (L0..L6) |
|---|---|---:|---:|---:|---|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 15` (5/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 5 | 2.96 (2.96-2.96) | 327.7 | `3 0 0 0 0 4 15` (5/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 5 | 3.15 (3.05-3.15) | 348.2 | `0 0 0 0 0 4 15` (1/5) |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 5 | 3.05 (2.96-3.15) | 337.9 | `1 0 0 0 0 3 15` (2/5) |

Every repetition:

| configuration | engine | rep | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---:|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 1 | `0 0 0 0 0 4 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 1 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 2 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 2 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 2 | `2 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 2 | `0 0 0 0 0 5 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 3 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 3 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 3 | `0 0 0 0 0 4 16` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 3 | `1 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 4 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 4 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 4 | `1 0 0 0 0 3 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 4 | `1 0 0 0 0 4 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | 5 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | 5 | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | 5 | `1 0 0 0 0 4 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | 5 | `1 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |

Raw data: `2026-11-03-final-warm-r5.csv` `2026-11-03-final-warm-r5.lsm.csv` 
