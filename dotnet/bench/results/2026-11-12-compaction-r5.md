# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-12 14:41 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.10GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v378
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 2.47 1.80 1.32
- Repetitions: 5; UTC time and load average (1/5/15 min) at the start of each: rep 1 14:42 0.91 1.47 1.24; rep 2 14:43 0.93 1.38 1.22; rep 3 14:44 0.91 1.30 1.20; rep 4 14:45 0.98 1.26 1.19; rep 5 14:46 0.97 1.21 1.18
- 1-minute load average before each of the 5 engine-set runs: min 0.91, median 0.93, max 0.98
- Load average at end: 1.05 1.19 1.17; total runtime 6 min 26 s

Compaction-heavy configuration (label `compaction`): num=1000000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Warm-up on for every engine (`--warmup true`): before the measured run, each process runs every benchmark of the list
on a separate temporary DB (same options, max(50k, num/10) <= 200k keys unless --warmup_ops; repeated benchmarks for >= 32
repetitions, >= warmup_ops ops and >= 1 s each) plus 3 reopen + flush/compaction cycles, deletes it, and (.NET) waits for
the JIT to quiesce; every measured benchmark starts after a settle pause (JIT quiet for 200 ms; the C leg sleeps 200 ms).
The measured run starts on a fresh DB and block cache. The jit columns show the methods compiled during each measurement.

compaction lag re-measure

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

Repeated runs: up to 5 repetitions of every engine x configuration x benchmark, each on a fresh DB, engines interleaved within each repetition. Cells are the **median** µs/op with the min-max range and the coefficient of variation (sample stddev / mean) of the repetitions; ratios are median / c-native median; p99 and alloc are medians over the repetitions. Rows where any engine has CV > 10% are flagged noisy (`!`). Geometric means use the medians; the 95% confidence interval is a percentile bootstrap (2000 resamples of the repetitions of each engine and benchmark, fixed seed).

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`), largest over the repetitions; ~0 when the warm-up did its job.

## Summary (geometric mean of median µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation. Brackets: 95% bootstrap confidence interval; parentheses: number of benchmarks.

| configuration | managed / c-native |
|---|---:|
| compaction: num=1000000, value=100B, threads=1, compression=none | 0.65x [0.59-0.68] (4) |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations and repetitions):

| engine | measurements | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| managed | 25 | 2513 | 14 | 761 (fillrandom, compaction: num=1000000, value=100B, threads=1, compression=none, rep 5) |

## compaction: num=1000000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op median (min-max, CV) | managed µs/op median (min-max, CV) | managed / C | c-native p99 | managed p99 | managed alloc B/op | managed jit |
|---|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 2.65 (2.57-3.00, 7%) | 1.70 (1.66-1.74, 2%) | 0.64x | 7.60 | 4.73 | 5.9 | 761 |
| overwrite | 2.84 (2.54-3.27, 9%) | 1.68 (1.43-1.72, 9%) | 0.59x | 8.35 | 4.60 | 5.2 | 20 |
| waitforcompaction ! | 695.1 ms (611.0 ms-1080.8 ms, 25% !) | 785.4 ms (19.76-988.4 ms, 58% !) | 1.13x | - | - | 1146944.0 | 30 |
| readrandom | 5.93 (5.10-6.42, 9%) | 4.18 (4.07-4.37, 3%) | 0.71x | 18.43 | 11.94 | 8.7 | 2 |
| seekrandom ! | 12.12 (11.52-14.55, 10% !) | 7.86 (6.96-8.41, 7%) | 0.65x | 46.82 | 36.34 | 0.0 | 6 |

## Noisy rows (CV > 10%)

| configuration | benchmark | engine | CV | median µs/op | min-max | runs |
|---|---|---|---:|---:|---:|---:|
| compaction: num=1000000, value=100B, threads=1, compression=none | waitforcompaction | c-native | 25.0% | 695.1 ms | 611.0 ms-1080.8 ms | 5 |
| compaction: num=1000000, value=100B, threads=1, compression=none | waitforcompaction | managed | 57.6% | 785.4 ms | 19.76-988.4 ms | 5 |
| compaction: num=1000000, value=100B, threads=1, compression=none | seekrandom | c-native | 10.4% | 12.12 | 11.52-14.55 | 5 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

Over the repetitions: write-amp median (min-max), and the most frequent LSM shape with the number of repetitions that had it.

| configuration | engine | runs | write-amp median (min-max) | written MB median | most frequent files per level (L0..L6) |
|---|---|---:|---:|---:|---|
| compaction: num=1000000, value=100B, threads=1, compression=none | c-native | 5 | 4.35 (4.26-4.35) | 962.6 | `0 0 0 0 0 3 36` (1/5) |
| compaction: num=1000000, value=100B, threads=1, compression=none | managed | 5 | 4.12 (3.98-4.17) | 911.4 | `0 0 0 0 0 3 33` (1/5) |

Every repetition:

| configuration | engine | rep | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---:|---|---:|---:|---:|---:|
| compaction: num=1000000, value=100B, threads=1, compression=none | c-native | 1 | `0 0 0 0 0 3 36` | 221.3 | 962.6 | 4.35 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | managed | 1 | `0 0 0 0 0 3 33` | 221.3 | 911.4 | 4.12 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | c-native | 2 | `0 0 0 0 0 3 33` | 221.3 | 962.6 | 4.35 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | managed | 2 | `0 0 0 0 0 5 31` | 221.3 | 880.6 | 3.98 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | c-native | 3 | `0 0 0 0 0 4 32` | 221.3 | 962.6 | 4.35 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | managed | 3 | `0 0 0 0 0 4 33` | 221.3 | 911.4 | 4.12 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | c-native | 4 | `1 0 0 0 0 2 33` | 221.3 | 942.1 | 4.26 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | managed | 4 | `0 0 0 0 0 3 31` | 221.3 | 880.6 | 3.98 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | c-native | 5 | `1 0 0 0 0 3 31` | 221.3 | 952.3 | 4.30 | 0 |
| compaction: num=1000000, value=100B, threads=1, compression=none | managed | 5 | `0 0 0 0 0 3 30` | 221.3 | 921.6 | 4.17 | 0 |

Raw data: `2026-11-12-compaction-r5.csv` `2026-11-12-compaction-r5.lsm.csv` 
