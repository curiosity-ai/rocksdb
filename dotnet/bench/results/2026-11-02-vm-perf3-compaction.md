# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 14:06 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 7.44 11.42 10.76
- Load average at end: 7.18 10.32 10.45; total runtime 1 min 49 s

Compaction-heavy configuration (label `compaction`): num=500000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Perf pass #3, compaction-heavy configuration (same flags as 2026-11-02-vm-perf2-compaction.md), managed-async included. Shared noisy VM (load 4-14 from other agents during the runs).

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | 1.14x (4) | 0.62x (4) | 0.78x (4) |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op | rocksdb-sharp µs/op | managed µs/op | managed-async µs/op | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 5.02 | 6.63 | 4.67 | 7.43 | 1.32x | 0.93x | 1.48x | 17.77 | 25.16 | 15.83 | 17.84 | 26.8 | 27.8 |
| overwrite | 5.84 | 6.43 | 4.03 | 4.86 | 1.10x | 0.69x | 0.83x | 21.44 | 20.67 | 10.45 | 11.12 | 6.7 | 8.7 |
| waitforcompaction | 144.4 ms | 100.5 ms | 827.1 ms | 494.0 ms | 0.70x | 5.73x | 3.42x | - | - | - | - | 1546704.0 | 781408.0 |
| readrandom | 11.60 | 13.22 | 8.15 | 8.52 | 1.14x | 0.70x | 0.74x | 47.67 | 57.12 | 38.55 | 40.64 | 18.9 | 18.5 |
| seekrandom | 44.50 | 45.95 | 14.59 | 18.58 | 1.03x | 0.33x | 0.42x | 119 | 133 | 54.68 | 64.72 | 0.2 | 0.1 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | `2 0 0 0 0 4 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | `0 0 0 0 0 3 16` | 110.6 | 368.6 | 3.33 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | `1 0 0 0 0 5 16` | 110.6 | 358.4 | 3.24 | 0 |

Raw data: `2026-11-02-vm-perf3-compaction.csv` `2026-11-02-vm-perf3-compaction.lsm.csv` 
