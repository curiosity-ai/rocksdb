# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 16:22 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 1.04 3.20 6.02
- Load average at end: 1.09 2.52 5.44; total runtime 1 min 54 s

Compaction-heavy configuration (label `compaction`): num=500000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Warm-up off: every engine is measured from a cold process (the .NET engines include JIT compilation and tier-0 code).

COLD leg, pair 3. Compaction-heavy configuration (same flags as 2026-11-02-vm-perf3-compaction.md), on the tree with the compaction-picking parity fixes. Shared noisy 4-vCPU VM (other agents running). One of 3 alternated cold (--warmup false) / warm (default) pairs. This is pair 3 (load average 1-2), run 16:22-16:27 UTC after another agent's librocksdb source build (15:45-16:30 UTC) had wound down; it replaces the first warm compaction run (15:43 UTC), which overlapped that build. Medians of all 3 pairs: docs/perf.md, "Benchmark warm-up".

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`); ~0 when the warm-up did its job.

## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | 1.08x (4) | 0.51x (4) | 0.65x (4) |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations):

| engine | benchmarks | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| rocksdb-sharp | 5 | 213 | 41 | 90 (readrandom, compaction: num=500000, value=100B, threads=1, compression=none) |
| managed | 5 | 4992 | 1107 | 1819 (fillrandom, compaction: num=500000, value=100B, threads=1, compression=none) |
| managed-async | 5 | 5363 | 1063 | 2185 (fillrandom, compaction: num=500000, value=100B, threads=1, compression=none) |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op | rocksdb-sharp µs/op | managed µs/op | managed-async µs/op | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 6.40 | 6.50 | 4.01 | 5.50 | 1.02x | 0.63x | 0.86x | 26.17 | 26.38 | 14.11 | 20.57 | 25.6 | 23.3 | 44 | 1819 | 2185 |
| overwrite | 5.69 | 5.71 | 2.97 | 3.69 | 1.00x | 0.52x | 0.65x | 22.18 | 23.94 | 8.97 | 12.70 | 10.1 | 10.8 | 41 | 1107 | 1063 |
| waitforcompaction | 169.7 ms | 58.9 ms | 969.3 ms | 529.6 ms | 0.35x | 5.71x | 3.12x | - | - | - | - | 1421576.0 | 643088.0 | 6 | 1162 | 318 |
| readrandom | 11.96 | 15.21 | 7.85 | 9.41 | 1.27x | 0.66x | 0.79x | 49.13 | 64.80 | 36.91 | 43.23 | 18.8 | 18.9 | 90 | 685 | 1513 |
| seekrandom | 49.16 | 50.85 | 15.23 | 20.50 | 1.03x | 0.31x | 0.42x | 138 | 144 | 58.01 | 69.16 | 0.2 | 0.1 | 32 | 219 | 284 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | `0 0 0 0 0 5 15` | 110.6 | 348.2 | 3.15 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | `1 0 0 0 0 4 15` | 110.6 | 337.9 | 3.05 | 0 |

Raw data: `2026-11-02-vm-cold-compaction.csv` `2026-11-02-vm-cold-compaction.lsm.csv` 
