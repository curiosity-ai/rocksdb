# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 13:04 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 2.60 2.71 3.50
- Load average at end: 1.32 2.19 3.21; total runtime 1 min 52 s

Compaction-heavy configuration (label `compaction`): num=500000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Perf pass #2, compaction-heavy configuration (same flags as 2026-11-02-vm-compaction.md), managed-async included. Shared noisy VM.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | 1.03x (4) | 0.63x (4) | 0.70x (4) |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op | rocksdb-sharp µs/op | managed µs/op | managed-async µs/op | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 5.70 | 5.48 | 4.28 | 4.61 | 0.96x | 0.75x | 0.81x | 19.06 | 18.81 | 16.59 | 15.95 | 50.5 | 55.5 |
| overwrite | 5.14 | 5.57 | 3.39 | 3.72 | 1.08x | 0.66x | 0.72x | 16.50 | 21.12 | 9.34 | 10.82 | 18.1 | 40.4 |
| waitforcompaction | 17.95 | 4415 | 838.5 ms | 734.4 ms | 245.99x | 46723.38x | 40922.38x | - | - | - | - | 18795016.0 | 19030000.0 |
| readrandom | 12.02 | 12.01 | 8.12 | 10.03 | 1.00x | 0.68x | 0.83x | 52.38 | 50.09 | 39.62 | 46.74 | 18.8 | 18.9 |
| seekrandom | 45.31 | 48.25 | 21.79 | 22.05 | 1.06x | 0.48x | 0.49x | 126 | 128 | 72.19 | 73.61 | 0.2 | 0.2 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | `3 0 0 0 0 4 14` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | `2 0 0 0 0 3 13` | 110.6 | 317.4 | 2.87 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | `2 0 0 0 0 3 15` | 110.6 | 348.2 | 3.15 | 0 |

Raw data: `2026-11-02-vm-perf2-compaction.csv` `2026-11-02-vm-perf2-compaction.lsm.csv` 
