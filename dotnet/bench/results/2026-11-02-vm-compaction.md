# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 10:29 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 6.78 9.30 8.13
- Load average at end: 9.32 9.43 8.31; total runtime 1 min 59 s

Compaction-heavy configuration (label `compaction`): num=500000, value 100 B, threads 1,
compression none, `--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU
block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled
compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks `fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`.
fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending.

Shared, noisy machine: other agents' builds and tests were running concurrently (load average well above the 4 cores, see Machine), so absolute and relative timings carry substantial noise; the LSM shape and write amplification are deterministic up to flush/compaction timing. Single run, one process per engine, managed-async not included.

Observations: all three engines end with the same LSM shape (dynamic level bytes: data in L5/L6, L0 below the trigger)
and write-amp within one 0.01 GB step of each other (2.96 native, 3.05 managed). waitforcompaction measures only the
background work left when overwrite returned (~1 ms for rocksdb-sharp, which had nothing left; 0.18 s c-native; 0.83 s
managed). managed overwrite (12.3 µs/op mean with p99 11 µs) is dominated by a few long write stalls, i.e. managed
background compaction keeps up less well than native under this load; the read-side wins of managed (readrandom,
seekrandom) are within the noise of this shared machine and were not reproduced.

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.

| configuration | rocksdb-sharp / c-native | managed / c-native |
|---|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | 1.15x (4) | 0.93x (4) |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op | rocksdb-sharp µs/op | managed µs/op | rocksdb-sharp / C | managed / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed alloc B/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 6.87 | 6.75 | 6.48 | 0.98x | 0.94x | 23.79 | 19.23 | 15.93 | 49.8 |
| overwrite | 5.49 | 8.09 | 12.29 | 1.47x | 2.24x | 17.31 | 32.03 | 11.36 | 36.4 |
| waitforcompaction | 183.0 ms | 1265 | 830.8 ms | 0.01x | 4.54x | - | - | - | 15887464.0 |
| readrandom | 14.02 | 13.22 | 10.56 | 0.94x | 0.75x | 60.25 | 54.26 | 37.55 | 18.6 |
| seekrandom | 53.85 | 69.41 | 25.22 | 1.29x | 0.47x | 138 | 191 | 79.39 | 1099.1 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | `2 0 0 0 0 5 15` | 110.6 | 337.9 | 3.05 | 0 |

Raw data: `2026-11-02-vm-compaction.csv` `2026-11-02-vm-compaction.lsm.csv` 
