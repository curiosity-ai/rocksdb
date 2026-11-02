# Benchmarks

This page explains how RocksDb.Managed is compared with native RocksDB, how to run the comparison and how to read the
results, and summarizes the latest numbers. [bench/README.md](../bench/README.md) is the reference for every flag and
workload of the runner. [perf.md](perf.md) records the optimization work with before/after measurements.

## Method: three engines, one workload

The same db_bench-style workload runs on three engines. Each engine runs in its own process on a fresh DB directory.

| Engine | What runs | Source |
|---|---|---|
| **c-native** | a db_bench-style C program on the RocksDB C API, linked to the same `librocksdb.so` that the `RocksDB` NuGet package ships | `bench/native-c/rocksdb_bench.c` |
| **rocksdb-sharp** | the NuGet `RocksDB` binding (`RocksDbSharp`), with its most efficient span APIs | `bench/RocksDb.Managed.Benchmarks/Native/` |
| **managed** | RocksDb.Managed, synchronous span APIs | `bench/RocksDb.Managed.Benchmarks/DbBench/ManagedEngine.cs` |
| **managed-async** | RocksDb.Managed, `ValueTask` APIs (one task per worker) | same, with `--async` |

c-native against rocksdb-sharp shows what the binding costs (P/Invoke, marshalling, copies). rocksdb-sharp against
managed is the choice a .NET user actually makes. c-native is the baseline for all ratios.

The engines are equivalent in everything except the engine itself (see `DbBench/Workload.cs`, which must stay in sync
with `rocksdb_bench.c`):

- **Keys and values**: db_bench's `GenerateKeyFromInt` keys (16 bytes) and `RandomGenerator` values (50%
  compressible). Random key sequences come from SplitMix64 seeded per benchmark and thread, so every engine reads and
  writes the same keys in the same order.
- **Options**: `create_if_missing`, the same compression on all levels, write buffer size, block-based table
  `format_version` 6, block size, an LRU (or HyperClock) block cache, a 10-bit full Bloom filter, WAL and sync from
  the flags. Everything else is each engine's default.
- **Measurement**: µs/op is wall time divided by total operations, as in db_bench. Percentiles come from a
  per-operation latency histogram (log-linear buckets, at most 1.6% error). The managed engines also report bytes
  allocated per operation (`GC.GetTotalAllocatedBytes`, all threads) and gen0/1/2 collection counts.
- **Point reads**: c-native uses `rocksdb_get`, as rocksdb-sharp's span `Get` does. MultiGet uses
  `rocksdb_batched_multi_get_cf` with pinnable slices in both native legs, and `MultiGetBatch` in managed.

## Running the comparison

```bash
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
cd dotnet
bench/run-all.sh                                   # full matrix + compaction-heavy configuration

# smaller matrix
NUM=200000 READS=100000 NUM_LARGE=50000 THREADS_LIST="1 4" COMPRESSIONS="none lz4" VALUE_SIZES="100 1024" bench/run-all.sh
# only the compaction-heavy configuration
RUN_MATRIX=0 NUM=500000 bench/run-all.sh
# only the integrated BlobDB configuration
RUN_MATRIX=0 RUN_COMPACTION=0 RUN_BLOB=1 NUM=200000 bench/run-all.sh
```

`run-all.sh` builds the benchmark project in Release and `bench/native-c` (`make -C bench/native-c`, against the
11.12 `c.h` in `native-c/include` and the NuGet `librocksdb.so`). Then it runs every configuration on every engine and
writes three files to `bench/results/`:

- `<date>-<host>.md`: the report (machine, summary, per-configuration tables, LSM shape)
- `<date>-<host>.csv`: the raw `RESULT` rows
- `<date>-<host>.lsm.csv`: the `STATS` rows

Every knob is an environment variable documented in the script header: `ENGINES`, `BENCHMARKS`, `THREADS_LIST`,
`COMPRESSIONS`, `VALUE_SIZES`, `EXTRA_ARGS`, `TFM`, `NOTES`, `RUN_COMPACTION`, `RUN_BLOB`, and the `COMPACTION_*` and
`BLOB_*` overrides.

The default matrix covers value sizes 100 B and 1 KiB, 1 and 4 threads, and no compression and LZ4, with the
benchmarks `fillseq, fillrandom, overwrite, flush, readrandom, readmissing, seekrandom, readseq, readreverse,
multireadrandom, readwhilewriting, compact, deleterandom, fillsync`. Fill benchmarks start from a fresh DB; the
others use the DB the previous step left behind.

Two additional configurations:

- **compaction** (on by default): 4 MiB write buffer and target file size and a 16 MiB `max_bytes_for_level_base`, so
  fills run against background flushes and compactions and reads hit a multi-level tree. The `stats` benchmark
  records each engine's LSM shape and write amplification.
- **blob** (`RUN_BLOB=1`): 1 KiB values in blob files (`--enable_blob_files 1 --min_blob_size 512`).

To run one engine directly, or to compare a custom set:

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench \
    --engine managed --benchmarks fillrandom,readrandom --num 1000000 --value_size 100
B="dotnet bench/RocksDb.Managed.Benchmarks/bin/Release/net10.0/RocksDb.Managed.Benchmarks.dll"
$B compare --engines c-native,rocksdb-sharp,managed,managed-async --db_base /tmp/bench --csv out.csv -- \
    --benchmarks fillrandom,readrandom --num 1000000 --threads 4 --compression lz4
$B report --csv out.csv --out out.md
```

## Reading a report

- **Cells** are µs/op (lower is better). `flush` and `compact` are milliseconds for one flush or one full compaction.
  Transaction benchmarks are µs per transaction.
- **Ratios** are engine / c-native. Below 1.00x means faster than C, and above means slower.
- **Summary**: the geometric mean of the ratios over a configuration's benchmarks (the count is in parentheses).
  `waitforcompaction` is excluded: its time is the leftover background work, not the speed of an operation.
- **p99** is per operation (per 16-key batch for `multireadrandom`).
- **alloc B/op** is managed bytes allocated per operation. Steady-state reads and seeks are expected to be about 0.
  Fills show the memtable arena (about 144 B per 100-byte entry), and `flush`/`compact` show per-job buffers.
- **LSM shape / write-amp** (compaction configuration): files per level from `rocksdb.num-files-at-level<N>`, and
  flush + compaction bytes written divided by user bytes. The WAL is not counted. The written bytes have a resolution
  of 0.01 GB.
- **Warm code only.** Benchmarks must measure warm code (CLAUDE.md). Every engine runs a warm-up first (`--warmup`,
  default on; see "Warm-up" in `bench/README.md`): the whole workload on a throwaway DB, including flushes and
  compactions, then the measurement on a fresh DB. The `jit` columns show how many methods were JIT-compiled while
  each benchmark was timed (0 - a few after the warm-up; a few hundred in the compaction-heavy fills). Reports before
  `2026-11-02-vm-warm*.md` (and the `-vm-cold*.md` counterparts) were measured cold: their managed numbers include
  JIT and tier-0 code, e.g. the first compaction of a process is 2.5-3.5x slower than later ones. See "Benchmark
  warm-up" in [perf.md](perf.md) for cold vs warm.
- **`fillsync`** measures the device's fsync latency more than the engine. Expect large swings between runs.

**Noise caveat.** The committed results come from a shared 4-vCPU cloud VM (KVM, Intel Xeon @ 2.80 GHz, 15 GiB)
while other agents ran builds and tests on it (load average 4-14). Absolute numbers vary by +-10-20% from run to run,
and single cells more. Use ratios measured in the same run, and re-run on a quiet machine before drawing conclusions
from one cell.

## Latest results

The source is `bench/results/2026-11-02-vm-warm.md` and `-vm-warm-compaction.md` (warm-up on, after perf pass #3 and
the compaction-picking parity fixes, .NET 10.0.14, librocksdb 11.12) and `-vm-blob.md` (cold, before the warm-up).
Matrix sizes were reduced: num=300k with 200k reads for 100-byte values, and 60k entries (200k reads) for 1 KiB
values. All runs were on the shared VM described above (load 1 - 7 for the warm runs).

Geometric mean of µs/op relative to c-native (lower is better; below 1.00x is faster than C):

| configuration | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| 100 B, 1 thread, no compression | 0.98x | 0.52x | 0.66x |
| 100 B, 4 threads, no compression | 1.05x | 0.60x | 0.63x |
| 100 B, 1 thread, LZ4 | 1.19x | 0.63x | 0.68x |
| 100 B, 4 threads, LZ4 | 1.10x | 0.61x | 0.59x |
| 1 KiB, 1 thread, no compression | 0.90x | 0.50x | 0.48x |
| 1 KiB, 4 threads, no compression | 0.99x | 0.73x | 0.72x |
| 1 KiB, 1 thread, LZ4 | 1.09x | 0.67x | 0.73x |
| 1 KiB, 4 threads, LZ4 | 0.89x | 0.81x | 0.72x |
| compaction-heavy (500k x 100 B, 1 thread; median of 3 runs) | 1.02x | 0.57x | 0.68x |
| integrated BlobDB (200k x 1 KiB, 1 thread; cold) | 0.84x | 0.55x | 0.53x |

Takeaways:

- **Writes** (`fillseq`, `fillrandom`, `overwrite`, `deleterandom`) are faster than C single-threaded (0.45-0.76x)
  and around parity with 4 threads (0.5-1.5x; `fillrandom` / `overwrite` with 1 KiB values and 4 threads are
  1.2-1.5x). In the compaction-heavy configuration `fillrandom` is 0.61x and `overwrite` 0.53x (medians of 3 runs).
  Writes allocate nothing per operation beyond the memtable arena.
- **Point reads, seeks and scans** are faster than C in every configuration of the warm matrix except a few
  4-thread cells near 1.0-1.3x: `readrandom` 0.63-0.95x, `multireadrandom` 0.46-0.84x, `seekrandom` 0.58-0.77x,
  `readseq` 0.35-0.95x, `readreverse` 0.30-0.86x; on the multi-level compaction-heavy tree `readrandom` 0.63x and
  `seekrandom` 0.50x. (The cold reports showed 1 KiB / 4-thread reads at 2.6-3.8x: mostly JIT and noise.)
- **Async** costs about the same as sync, because calls complete synchronously when nothing waits.
- **Flush and compaction** of a warm process are at or below native cost: one flush 0.28-0.97x without compression
  (0.76-1.21x with LZ4, where the K4os LZ4 encoder is slower than native's), one full compaction 0.27-0.93x. Cold
  (first in the process) they were 0.4-2.0x and 0.6-1.9x ([perf.md](perf.md), "Benchmark warm-up"). Single-shot rows
  vary by +-50% between runs, also for c-native.
- **LSM shape** in the compaction-heavy configuration: with flushes and compactions in lock step the managed engine
  produces the same LSM shapes, compactions and write-amp as native (`CompactionPickerParityTests`). Free-running,
  write-amp is 2.96-3.24 managed against 2.96 for c-native and 2.96-3.05 for rocksdb-sharp
  (`bench/results/2026-11-02-vm-picker-compaction.md`): the managed writer is faster, so its single compaction thread
  falls further behind (`waitforcompaction` 0.5-1.2 s against 0-0.4 s). See "Compaction-picking parity" in
  [perf.md](perf.md). The 6 cold / warm runs of the warm-up comparison give 2.96-3.15 managed and 2.87-3.05
  managed-async against 2.96 native.
- **rocksdb-sharp** pays 0-50% over c-native on single-threaded small operations (P/Invoke and copies), up to
  2.3-2.5x on 4-thread `readseq` / `readreverse` with 100 B values.

Older reports in `bench/results/` (`2026-11-02-vm*.md` before perf pass #2, `-vm-perf2*.md` after it) show how the
numbers changed with each optimization pass.

## Micro-benchmarks (BenchmarkDotNet)

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --list flat
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbHeadToHead
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbAsync --job short
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*TableBenchmarks*'
```

Add `--cli /root/.dotnet/dotnet` if `dotnet` is not on the child processes' `PATH`. Every class has
`[MemoryDiagnoser]`. The `Allocated` column must be `-` for hot paths.

| Class | Category | Measures |
|---|---|---|
| `DbPutBenchmarks`, `DbWriteBatchBenchmarks`, `DbGetBenchmarks`, `DbIteratorBenchmarks`, `DbMultiGetBenchmarks` | `DbHeadToHead` (+ `Put`, `WriteBatch`, `Get`, `Iterator`, `MultiGet`) | managed against rocksdb-sharp, per operation: Put, a 100-entry batch, Get from the memtable / a cached SST / a miss, Seek + 10 or 100 Next, MultiGet of 16 keys |
| `DbAsyncBenchmarks` | `DbAsync` | sync against `ValueTask` APIs for Put, TryGet, Seek + 10 Next and MultiGet(16), with blocks in the block cache or only in the page cache |
| `TableBenchmarks`, `TableConcurrentGetBenchmarks` | | SST point lookups (cached, uncached, concurrent) and table building |
| `IteratorBenchmarks` | | table, merging and DB iterator stepping |
| `MemTableBenchmarks`, `SkipListBenchmarks` | | memtable insert and lookup, the lock-free skip list |
| `CacheBenchmarks` | | LRU and HyperClock lookup and insert, single- and multi-threaded |
| `FilterBenchmarks`, `HashBenchmarks`, `CompressionBenchmarks` | `FiltersHashCompression` | Bloom and Ribbon build and query, XXH3 / xxHash64 / crc32c, the codecs (some against native counterparts) |
| `WalBenchmarks`, `WriteBatchBenchmarks` | | WAL record writing and reading, WriteBatch building and parsing |
| `EnvBenchmarks`, `UtilBenchmarks` | | file I/O paths (sync and async, `preadv2` fast path), varints and coding helpers |

For BenchmarkDotNet comparisons on a shared machine, follow [perf.md](perf.md#method): alternate the baseline and
the candidate in the same time window (`--job short --iterationCount 8`), and report medians.
