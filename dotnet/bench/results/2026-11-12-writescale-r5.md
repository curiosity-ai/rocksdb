# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-12 14:22 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v378
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 2.95 1.31 0.49
- Repetitions: 5; UTC time and load average (1/5/15 min) at the start of each: rep 1 14:23 0.91 1.03 0.45; rep 2 14:27 0.87 1.26 0.69; rep 3 14:31 0.91 1.41 0.89; rep 4 14:35 0.94 1.48 1.05; rep 5 14:38 0.87 1.48 1.16
- 1-minute load average before each of the 15 engine-set runs: min 0.87, median 1.04, max 1.64
- Load average at end: 2.60 1.82 1.32; total runtime 18 min 43 s

Matrix: value sizes [100] (num=2000000, reads=2000000; large values num=2000000, reads=2000000), threads [1 2 4],
compression [none], benchmarks `fillrandom,readrandomwriterandom`. Common options: 16-byte keys, values 50% compressible,
8 MiB LRU block cache, 10-bit full bloom filter, 64 MiB write buffer, 4 KiB blocks, format_version 6, WAL on, no sync
(fillsync: num/1000 synced puts). Fill benchmarks start from a fresh DB; the others run on the DB left by the previous step.
Ops are split across threads; readwhilewriting = N reader threads + 1 writer. Each engine runs in its own process.

Warm-up on for every engine (`--warmup true`): before the measured run, each process runs every benchmark of the list
on a separate temporary DB (same options, max(50k, num/10) <= 200k keys unless --warmup_ops; repeated benchmarks for >= 32
repetitions, >= warmup_ops ops and >= 1 s each) plus 3 reopen + flush/compaction cycles, deletes it, and (.NET) waits for
the JIT to quiesce; every measured benchmark starts after a settle pause (JIT quiet for 200 ms; the C leg sleeps 200 ms).
The measured run starts on a fresh DB and block cache. The jit columns show the methods compiled during each measurement.

write scaling re-measure

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 5 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | managed / c-native |
|---|---:|
| num=2000000, value=100B, threads=1, compression=none | 0.73x [0.70-0.74] (2) |
| num=2000000, value=100B, threads=2, compression=none | 0.81x [0.73-0.89] (2) |
| num=2000000, value=100B, threads=4, compression=none | 0.82x [0.78-0.86] (2) |
| **all matrix configurations** | **0.78x [0.76-0.81] (6)** |

### Per benchmark (geometric mean over the 3 matrix configurations of the median ratio vs c-native)

| benchmark | managed / c-native |
|---|---:|
| fillrandom | 0.80x [0.75-0.84] |
| readrandomwriterandom w100% | 0.77x [0.74-0.82] |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| managed | 30 | 3789 | 113 | 401 (fillrandom, num=2000000, value=100B, threads=4, compression=none, rep 2) |

## Mixed reads / writes (`readrandomwriterandom`)

Throughput, higher is better (medians over the repetitions): reads/s and writes/s = ops/s x the fraction of the ops that were reads / writes; vs C = ops/s of the engine / ops/s of c-native (< 1 = slower). alloc = managed bytes allocated per op.

### num=2000000, value=100B, threads=1, compression=none

| writes | c-native reads/s | c-native writes/s | c-native ops/s | managed reads/s | managed writes/s | managed ops/s | managed vs C | managed alloc B/op |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100% | 0 | 235.1k | 235.1k | 0 | 319.0k | 319.0k | 1.36x | 1.7 |

### num=2000000, value=100B, threads=2, compression=none

| writes | c-native reads/s | c-native writes/s | c-native ops/s | managed reads/s | managed writes/s | managed ops/s | managed vs C | managed alloc B/op |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100% | 0 | 185.0k | 185.0k | 0 | 240.2k | 240.2k | 1.30x | 3.5 |

### num=2000000, value=100B, threads=4, compression=none

| writes | c-native reads/s | c-native writes/s | c-native ops/s | managed reads/s | managed writes/s | managed ops/s | managed vs C | managed alloc B/op |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100% | 0 | 194.9k | 194.9k | 0 | 240.2k | 240.2k | 1.23x | 2.2 |

## num=2000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed / C | c-native p99 | managed p99 | managed alloc B/op | managed jit |
|---|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 4.05 (3.95-4.34, 4%) | 2.91 (2.90-2.93, 0%) | 0.72x | 11.24 | 7.54 | 14.8 | 192 |
| readrandomwriterandom w100% | 4.25 (4.24-4.41, 2%) | 3.13 (3.07-3.25, 2%) | 0.74x | 11.95 | 8.23 | 1.7 | 274 |

## num=2000000, value=100B, threads=2, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed / C | c-native p99 | managed p99 | managed alloc B/op | managed jit |
|---|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 5.11 (4.37-5.28, 7%) | 4.33 (3.56-4.35, 8%) | 0.85x | 33.37 | 29.03 | 21.8 | 126 |
| readrandomwriterandom w100% ! | 5.41 (4.89-5.57, 5%) | 4.16 (3.61-4.65, 10% !) | 0.77x | 34.55 | 28.32 | 3.5 | 143 |

## num=2000000, value=100B, threads=4, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed / C | c-native p99 | managed p99 | managed alloc B/op | managed jit |
|---|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 5.08 (4.77-5.40, 5%) | 4.21 (3.98-4.33, 4%) | 0.83x | 57.03 | 55.46 | 18.6 | 401 |
| readrandomwriterandom w100% | 5.13 (5.04-5.21, 1%) | 4.16 (3.89-4.46, 5%) | 0.81x | 61.20 | 53.86 | 2.2 | 161 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| num=2000000, value=100B, threads=2, compression=none | readrandomwriterandom w100% | managed | 10.2% | 4.16 | 3.61-4.65 | 5 |

Raw data: `writescale.csv` 
