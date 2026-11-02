# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 15:23 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 6.92 7.28 7.67
- Load average at end: 8.34 7.77 7.82; total runtime 1 min 40 s

Compaction-heavy configuration (label `compaction`): num=500000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Compaction-picking parity pass (docs/perf.md 'Compaction-picking parity'): managed flush drops hidden versions, non-bottommost compaction outputs may grow to 2x target_file_size like upstream. Managed timings are COLD (fresh process, no warm-up pass: the runner had no --warmup yet); this run is about LSM shape and write amplification. Shared noisy VM (load 4-10).

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.

| configuration | rocksdb-sharp / c-native | managed / c-native |
|---|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | 0.98x (4) | 0.60x (4) |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op | rocksdb-sharp µs/op | managed µs/op | rocksdb-sharp / C | managed / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed alloc B/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 6.38 | 7.42 | 6.16 | 1.16x | 0.97x | 19.75 | 26.65 | 11.43 | 27.1 |
| overwrite | 7.41 | 6.97 | 5.29 | 0.94x | 0.71x | 26.51 | 21.94 | 8.24 | 9.2 |
| waitforcompaction | 376.2 ms | 406.8 ms | 827.3 ms | 1.08x | 2.20x | - | - | - | 1315848.0 |
| readrandom | 14.68 | 12.88 | 8.06 | 0.88x | 0.55x | 63.62 | 51.25 | 41.74 | 19.3 |
| seekrandom | 51.46 | 49.23 | 17.19 | 0.96x | 0.33x | 167 | 142 | 64.78 | 0.2 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | `0 0 0 0 0 3 16` | 110.6 | 348.2 | 3.15 | 0 |

Raw data: `2026-11-02-vm-picker-compaction.csv` `2026-11-02-vm-picker-compaction.lsm.csv` 
