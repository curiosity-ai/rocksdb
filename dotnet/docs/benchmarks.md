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
- **`flush` and `compact` include JIT warm-up for managed**: each runs once in a fresh process, and the first
  compaction of a process is 2.5-3.5x slower than later ones (see "Perf pass #3" in [perf.md](perf.md)).
- **`fillsync`** measures the device's fsync latency more than the engine. Expect large swings between runs.

**Noise caveat.** The committed results come from a shared 4-vCPU cloud VM (KVM, Intel Xeon @ 2.80 GHz, 15 GiB)
while other agents ran builds and tests on it (load average 4-14). Absolute numbers vary by +-10-20% from run to run,
and single cells more. Use ratios measured in the same run, and re-run on a quiet machine before drawing conclusions
from one cell.

## Latest results

The source is `bench/results/2026-11-02-vm-perf3.md`, `-vm-perf3-compaction.md` and `-vm-blob.md` (after perf
pass #3, .NET 10.0.14, librocksdb 11.12). Matrix sizes were reduced: num=300k with 200k reads for 100-byte values,
and 60k entries for 1 KiB values. All runs were on the noisy shared VM described above.

Geometric mean of µs/op relative to c-native (lower is better; below 1.00x is faster than C):

| configuration | rocksdb-sharp | managed | managed-async |
|---|---:|---:|---:|
| 100 B, 1 thread, no compression | 1.48x | 1.37x | 1.33x |
| 100 B, 4 threads, no compression | 0.85x | 0.67x | 0.76x |
| 100 B, 1 thread, LZ4 | 1.14x | 0.98x | 1.04x |
| 100 B, 4 threads, LZ4 | 0.73x | 0.73x | 0.74x |
| 1 KiB, 1 thread, no compression | 0.91x | 0.77x | 0.84x |
| 1 KiB, 4 threads, no compression | 1.04x | 1.36x | 1.25x |
| 1 KiB, 1 thread, LZ4 | 1.10x | 1.03x | 1.15x |
| 1 KiB, 4 threads, LZ4 | 1.06x | 1.73x | 2.07x |
| compaction-heavy (500k x 100 B, 1 thread) | 1.14x | 0.62x | 0.78x |
| integrated BlobDB (200k x 1 KiB, 1 thread) | 0.84x | 0.55x | 0.53x |

Takeaways:

- **Writes** (`fillrandom`, `overwrite`, `deleterandom`) are at parity with C or faster in most configurations. For
  example, `fillrandom` with 100 B values and 1 thread is 0.69x, and with the compaction-heavy configuration
  `overwrite` is 0.69x. Writes allocate nothing per operation beyond the memtable arena.
- **Point reads and seeks** are close to C for 100 B values (`readrandom` 0.69-1.26x, `readmissing` 0.33-0.65x,
  `seekrandom` 0.46-0.93x). On the multi-level compaction-heavy tree they are faster (`readrandom` 0.70x,
  `seekrandom` 0.33x). With 1 KiB values and 4 threads they are slower (`readrandom` 2.6x, `readreverse` 3.8x); this
  configuration has the widest run-to-run spread and needs a quiet re-run.
- **Async** costs about the same as sync, because calls complete synchronously when nothing waits.
- **Flush and compaction** in a fresh process are mostly slower than C: one flush is 0.7-2.0x without compression and
  2.7-3.2x with LZ4, where the LZ4 codec's JIT warm-up dominates, and one full compaction is 0.96-2.35x. Later flushes
  and compactions in the same process are at or below native cost ([perf.md](perf.md), perf pass #3).
- **LSM shape** in the compaction-heavy configuration is close to native: write-amp 3.24-3.33 managed against 2.96 for
  c-native and 3.05 for rocksdb-sharp. Background compaction falls behind native under sustained writes
  (`waitforcompaction` 0.5-0.8 s against 0.1-0.15 s). This is a known gap in compaction picking.
- **rocksdb-sharp** pays 10-50% over c-native on single-threaded small operations (P/Invoke and copies), and much
  more on `readseq` (2.4x).

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
