# RocksDB engine comparison: C native vs rocksdb-sharp vs RocksDb.Managed

## Machine

- Date: 2026-11-02 16:24 UTC, host vm
- CPU: Intel(R) Xeon(R) Processor @ 2.80GHz, 4 cores (VM: KVM)
- Memory: 15 GiB
- Kernel: Linux 6.18.44-fc-v185
- .NET: SDK 11.0.100-rc.3.26522.148; benchmarks run on net10.0 (.NET 10.0.14)
- librocksdb: NuGet RocksDB 11.12.0.7385 (librocksdb.so.11.12), used by both c-native and rocksdb-sharp
- C compiler: cc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
- Load average at start: 1.08 2.50 5.42
- Load average at end: 1.09 1.96 4.79; total runtime 2 min 29 s

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

Warm-up on (default), pair 3. Compaction-heavy configuration (same flags as 2026-11-02-vm-perf3-compaction.md), on the tree with the compaction-picking parity fixes. Shared noisy 4-vCPU VM (other agents running). One of 3 alternated cold (--warmup false) / warm (default) pairs. This is pair 3 (load average 1-2), run 16:22-16:27 UTC after another agent's librocksdb source build (15:45-16:30 UTC) had wound down; it replaces the first warm compaction run (15:43 UTC), which overlapped that build. Medians of all 3 pairs: docs/perf.md, "Benchmark warm-up".

Engines: **c-native** = bench/native-c/rocksdb_bench (C API, same librocksdb.so as rocksdb-sharp); **rocksdb-sharp** = NuGet `RocksDB` binding; **managed** = RocksDb.Managed sync API; **managed-async** = RocksDb.Managed async API (one task per thread). Cells are µs/op (wall time / total ops, lower is better); ratios are relative to c-native (> 1 = slower). p99 is per-operation latency in µs (per 16-key batch for multireadrandom). alloc = managed bytes allocated per op.

jit = methods JIT-compiled (all threads, incl. the tiering background compiler) while the benchmark was measured, for the .NET engines (`System.Runtime.JitInfo`); ~0 when the warm-up did its job.

## Summary (geometric mean of µs/op ratio vs c-native over the benchmarks of each configuration)

`waitforcompaction` is left out: its time is the background work that happened to be left over, not the speed of an operation.

| configuration | rocksdb-sharp / c-native | managed / c-native | managed-async / c-native |
|---|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | 1.02x (4) | 0.57x (4) | 0.68x (4) |

JIT during the measurements (methods compiled while a benchmark was being timed, over all configurations):

| engine | benchmarks | total methods | median | max (benchmark) |
|---|---:|---:|---:|---|
| rocksdb-sharp | 5 | 2 | 0 | 1 (seekrandom, compaction: num=500000, value=100B, threads=1, compression=none) |
| managed | 5 | 747 | 6 | 605 (fillrandom, compaction: num=500000, value=100B, threads=1, compression=none) |
| managed-async | 5 | 836 | 78 | 596 (fillrandom, compaction: num=500000, value=100B, threads=1, compression=none) |

## compaction: num=500000, value=100B, threads=1, compression=none

| benchmark | c-native µs/op | rocksdb-sharp µs/op | managed µs/op | managed-async µs/op | rocksdb-sharp / C | managed / C | managed-async / C | c-native p99 | rocksdb-sharp p99 | managed p99 | managed-async p99 | managed alloc B/op | managed-async alloc B/op | rocksdb-sharp jit | managed jit | managed-async jit |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom | 5.25 | 5.49 | 3.26 | 3.85 | 1.05x | 0.62x | 0.73x | 16.50 | 19.68 | 10.54 | 12.68 | 7.0 | 4.6 | 1 | 605 | 596 |
| overwrite | 6.15 | 5.38 | 3.25 | 3.47 | 0.87x | 0.53x | 0.56x | 27.73 | 19.48 | 11.34 | 10.65 | 10.8 | 10.6 | 0 | 134 | 150 |
| waitforcompaction | 59.56 | 33.97 | 24.37 | 25.99 | 0.57x | 0.41x | 0.44x | - | - | - | - | 40.0 | 40.0 | 0 | 0 | 0 |
| readrandom | 11.91 | 12.19 | 7.46 | 9.51 | 1.02x | 0.63x | 0.80x | 50.05 | 50.71 | 36.70 | 44.06 | 19.2 | 19.1 | 0 | 2 | 78 |
| seekrandom | 45.79 | 52.31 | 22.76 | 29.68 | 1.14x | 0.50x | 0.65x | 129 | 150 | 71.51 | 86.21 | 0.2 | 0.1 | 1 | 6 | 12 |

## LSM shape and write amplification (`stats` benchmark)

files per level = rocksdb.num-files-at-level0..6; written = flush + compaction output bytes from the "Cumulative compaction: X GB write" line of rocksdb.stats (0.01 GB resolution); user = key + value bytes written by the benchmarks since the DB was created; write-amp = written / user (WAL not included).

| configuration | engine | files per level (L0..L6) | user MB | written MB | write-amp | pending compaction bytes |
|---|---|---|---:|---:|---:|---:|
| compaction: num=500000, value=100B, threads=1, compression=none | c-native | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | rocksdb-sharp | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed | `2 0 0 0 0 3 15` | 110.6 | 337.9 | 3.05 | 0 |
| compaction: num=500000, value=100B, threads=1, compression=none | managed-async | `3 0 0 0 0 4 15` | 110.6 | 327.7 | 2.96 | 0 |

Raw data: `2026-11-02-vm-warm-compaction.csv` `2026-11-02-vm-warm-compaction.lsm.csv` 
