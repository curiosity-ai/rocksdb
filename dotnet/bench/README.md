# Benchmarks

Reference for the benchmark runner. [docs/benchmarks.md](../docs/benchmarks.md) explains the methodology, how to
read a report and the latest results.

Three ways to measure RocksDb.Managed against native RocksDB:

| leg | what | where |
|---|---|---|
| **c-native** | db_bench-style program written against the RocksDB **C API**, linked to the very same `librocksdb.so` that ships in the `RocksDB` NuGet package | `native-c/` |
| **rocksdb-sharp** | the NuGet `RocksDB` binding (`RocksDbSharp`), most efficient span APIs | `RocksDb.Managed.Benchmarks/Native/` |
| **managed** / **managed-async** | RocksDb.Managed, synchronous span APIs / `ValueTask` APIs | `RocksDb.Managed.Benchmarks/DbBench/ManagedEngine.cs` |

c-native vs rocksdb-sharp isolates the binding cost (P/Invoke, marshalling, copies) from engine performance;
rocksdb-sharp vs managed is what a .NET user actually sees.

All commands below run from `dotnet/` with `export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet`.

## Full comparison matrix

```bash
bench/run-all.sh
# smaller / custom matrix:
NUM=200000 READS=100000 NUM_LARGE=50000 THREADS_LIST="1 4" COMPRESSIONS="none lz4" VALUE_SIZES="100 1024" bench/run-all.sh
# only the compaction-heavy configuration:
RUN_MATRIX=0 NUM=500000 ENGINES=c-native,rocksdb-sharp,managed bench/run-all.sh
```

After the matrix, `run-all.sh` runs one **compaction-heavy** configuration (label `compaction`, `RUN_COMPACTION=0` to skip):
4 MiB write buffer and target file size, 16 MiB `max_bytes_for_level_base`, benchmarks
`fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom`, so the fills run against background flushes and
compactions, the reads hit a multi-level LSM tree, and `stats` records the LSM shape and write amplification of every
engine (knobs `COMPACTION_NUM`, `COMPACTION_ARGS`, `COMPACTION_BENCHMARKS`, ... in the header of `run-all.sh`). Results:
`results/2026-11-02-vm-compaction.md`, then `-vm-perf2-compaction.md` and `-vm-perf3-compaction.md` after perf passes #2
and #3, and `-vm-picker-compaction.md` after the compaction-picking parity fixes (`docs/perf.md`), all cold; with the
warm-up: `-vm-warm-compaction.md` (alternated with cold runs: `-vm-cold-compaction.md`). Reduced matrix with the warm-up:
`results/2026-11-02-vm-warm.md` (cold counterpart from the same session: `-vm-cold.md`). The current reference is
`results/2026-11-02-final-warm-r5.md`: matrix + compaction-heavy configuration, 5 repetitions on a quiet machine
(medians, CVs, bootstrap CIs), with the BenchmarkDotNet head-to-head reports in `results/2026-11-02-final-bdn.md`.

All engines run a **warm-up phase** before the measurement (default on, `WARMUP=false` for cold-process numbers; see
"Warm-up" below), so the .NET engines are not measured while the JIT is still compiling and tiering up. Results before
`2026-11-02-vm-warm*.md` were measured cold: their `flush` / `compact` rows (one flush / one full compaction per process)
include the JIT of the flush / compaction code for the managed engines.

`RUN_BLOB=1` adds one **integrated BlobDB** configuration (label `blob`): 1 KiB values written to blob files
(`--enable_blob_files 1 --min_blob_size 512`, 16 MiB write buffer), benchmarks
`fillrandom,overwrite,flush,waitforcompaction,stats,readrandom,multireadrandom,seekrandom,readseq,compact,readrandom`
(knobs `BLOB_NUM`, `BLOB_VALUE_SIZE`, `BLOB_ARGS`, `BLOB_BENCHMARKS`, ...). Results: `results/2026-11-02-vm-blob.md`
(`RUN_MATRIX=0 RUN_COMPACTION=0 RUN_BLOB=1 NUM=200000 bench/run-all.sh`).

Builds the benchmark project (Release) and `native-c`, runs every configuration on every engine (each engine in its
own process on a fresh DB directory under `DB_BASE`, deleted afterwards), and writes
`bench/results/<date>-<host>.md` (summary + per-configuration tables with µs/op, ratios vs c-native, p99 and managed
allocations, LSM shape / write amplification, machine info), the raw `.csv` and the `stats` rows in `.lsm.csv`. See the header of `run-all.sh` for all knobs (`ENGINES`, `BENCHMARKS`,
`EXTRA_ARGS`, `TFM`, ...).

The two building blocks can also be used directly:

```bash
B="dotnet bench/RocksDb.Managed.Benchmarks/bin/Release/net10.0/RocksDb.Managed.Benchmarks.dll"
$B compare --engines c-native,rocksdb-sharp,managed,managed-async --db_base /tmp/bench --csv out.csv -- \
    --benchmarks fillrandom,readrandom --num 1000000 --threads 4 --compression lz4
$B report --csv out.csv --out out.md
```

### Repeated runs (`REPEATS`)

One run of a configuration varies by +-10-30% per cell, more for single-shot rows (`flush`, `compact`). For
statistically meaningful numbers, repeat the whole matrix:

```bash
REPEATS=5 LOAD_MAX=1.5 NUM=300000 NUM_LARGE=100000 COMPACTION_NUM=500000 bench/run-all.sh
```

- Repetition r runs every configuration (and the compaction / blob configurations) on all engines, in the order
  c-native, rocksdb-sharp, managed, managed-async, before repetition r+1 starts. Every engine run is a fresh process
  with its own warm-up and a fresh DB, so slow drift of the machine (thermal, other tenants) is spread over all engines
  instead of landing on one.
- `LOAD_MAX=1.5` waits before each repetition until the 1-minute load average is at most 1.5 (at most
  `LOAD_WAIT_MAX_S`, default 900 s). The report lists the load average at the start of each repetition and the
  min / median / max load before every engine-set run.
- `compare --rep N` writes the repetition number into the last CSV column (`rep`; also in `.lsm.csv`).
- `report` detects repeated rows and shows, per benchmark and engine, the **median** µs/op with the min-max range and
  the coefficient of variation (sample stddev / mean). Ratios use medians. Rows where any engine has CV > 10% are
  marked `!` and listed in a "Noisy rows" section. The summary geometric means use the medians and come with a 95%
  percentile-bootstrap confidence interval: 2000 resamples (fixed seed), each redrawing the repetitions of every
  engine and benchmark with replacement and recomputing medians, ratios and their geometric mean. The report also has
  a geometric mean over all matrix configurations and one per benchmark. The LSM section gives the median write-amp
  (min-max) and the most frequent LSM shape per engine, then every repetition.

## Warm-up

.NET compiles every method on first call (tier 0, unoptimized), patches hot loops to optimized code on stack (OSR), and
recompiles methods called 30+ times with full optimizations and Dynamic PGO (tier 1) in a background thread, after a
~100 ms delay that restarts whenever new tier-0 methods appear. A cold process therefore measures JIT time and tier-0
code: the first flush / compaction of a process is 2.5 - 3.5x slower than the following ones (`docs/perf.md`, perf
pass #3). rocksdb-sharp is affected too (its P/Invoke stubs and managed wrapper code are jitted). So the dbbench
runner (both .NET engines, sync and async) warms up first, and `native-c/rocksdb_bench` runs the identical warm-up
workload (no JIT there, but the page cache, allocator and file system then start from the same state in all three legs):

1. The whole `--benchmarks` list runs on a **separate DB** `<db>-warmup` with the same options and a key range of
   `--warmup_ops` keys (default max(50k, num/10), at most 200k and at most num); a fillrandom is prepended if the list
   does not start with a fill, `stats` is skipped.
2. Every benchmark except flush / compact / waitforcompaction is **repeated** in chunks of `warmup_ops / 32` ops until
   it has run >= 32 repetitions, >= `warmup_ops` ops and >= `--warmup_min_ms` (default 1000) ms, each repetition with
   a different random key stream. 32 repetitions get the per-run harness and engine methods (iterator / MultiGet
   context creation, ...) over the tier-1 call count; the minimum time keeps the per-op methods running well past the
   tiering delay so they are promoted (with Dynamic PGO) during the warm-up, not during the measurement.
3. Then 4 **flush / compaction cycles**: reopen the DB (and let the JIT quiesce), 16 x (overwrite of `warmup_ops / 50`
   keys + flush), waitforcompaction, compact. The 64 small flushes and the automatic compactions they trigger push the
   methods that run once per flush / table file / compaction / DB open through tier 0 -> instrumented -> tier 1 (each
   step needs ~30 calls), even when the list has no flush or compaction.
4. The warm-up DB (and its `-txn*` siblings) is deleted; C rebuilds its options, so the measured DB starts with a fresh
   block cache in every leg. .NET: a full blocking GC, then the runner waits until
   `JitInfo.GetCompiledMethodCount` has been stable for 200 ms.
5. Every **measured** benchmark starts after a settle pause: the .NET runner waits until the JIT has been quiet for
   200 ms (methods that reached their call count during the DB open or the previous benchmark are compiled then), the
   C leg sleeps 200 ms.

The .NET runner prints the JIT statistics of the warm-up (`warmup : done in ... s; JIT during warm-up N methods X ms,
while quiescing ...`) and, for every measured benchmark, the methods compiled while it was being timed (all threads,
including the tiering background compiler): `... alloc 3.1 B/op gc 0/0/0 jit 0 methods 0.0 ms`, and the `jit_methods`,
`jit_ms` columns of the RESULT line / CSV. The report shows them per benchmark (`jit` columns) and summarized per
engine. With the default settings they are 0 - a few methods (< 15 ms of JIT) per benchmark; `--warmup_min_ms 100`
is visibly too short (dozens of methods per read benchmark).

Flags (all engines, also passed by `compare` / `run-all.sh`): `--warmup true|false` (default true), `--warmup_ops N`,
`--warmup_min_ms MS`; `run-all.sh`: `WARMUP=true|false`, `WARMUP_OPS`, `WARMUP_MIN_MS`. A warm-up adds roughly
(benchmarks x 1 s + 2 - 3 s) per engine and configuration.

Runtime settings: the benchmark project keeps the .NET defaults (`<TieredPGO>true</TieredPGO>`, tiered compilation,
quick JIT for loops with OSR, ReadyToRun code of the framework used as the first tier), because that is what an
application gets. The warm-up makes the measurement steady-state under those settings. Knobs for experiments
(environment variables of the measured process; not set by any script):

| variable | effect |
|---|---|
| `DOTNET_TieredCompilation=0` | every method is compiled once, fully optimized, without Dynamic PGO (no guarded devirtualization); less warm-up needed, usually slower steady state |
| `DOTNET_TC_QuickJitForLoops=0` | methods with loops skip tier 0 / OSR and are compiled optimized on first call |
| `DOTNET_TieredPGO=0` | tiering without instrumentation (no Dynamic PGO) |
| `DOTNET_ReadyToRun=0` | ignore the precompiled framework code (more JIT at startup) |
| `DOTNET_TC_CallCountThreshold`, `DOTNET_TC_CallCountingDelayMs` | tier-1 call count (30) and tiering delay (100 ms) |

BenchmarkDotNet (below) does its own warm-up: every job runs pilot and warm-up iterations before the measured ones,
including `--job short` used in the commands of this file and `docs/perf.md`. `--job dry` / `RunStrategy.ColdStart`
measure single cold invocations and are only useful as smoke tests (no class in this project uses them).

## db_bench-style runner

```bash
make -C bench/native-c                                   # once; needs the NuGet package restored
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- dbbench \
    --engine managed|rocksdb-sharp|c-native [--async] \
    --benchmarks fillseq,fillrandom,overwrite,flush,readrandom --num 1000000 --value_size 100
bench/native-c/rocksdb_bench --benchmarks fillrandom,readrandom --num 1000000   # the C leg directly
```

`--engine c-native` runs `bench/native-c/rocksdb_bench` (or `--c_bench PATH` / `$ROCKSDB_C_BENCH`) with the same flags.

Benchmarks: `fillseq`, `fillrandom`, `overwrite`, `fillsync` (num/1000 synced puts), `readrandom`, `readmissing`,
`readseq`, `readreverse`, `seekrandom` (Seek + `--seek_nexts` Next), `multireadrandom` (batched MultiGet of
`--multiget_size` keys), `readwhilewriting` (`--threads` readers + 1 writer), `deleterandom`, `flush`, `compact`
(full-range CompactRange), `waitforcompaction` (DB::WaitForCompact with default options: `rocksdb_wait_for_compact` in C and
rocksdb-sharp, `WaitForCompact` in managed), `stats` (see below), `txnoptimistic` / `txnpessimistic` (num/txn_puts
transactions of `--txn_puts` random puts each, default 10, on a fresh OptimisticTransactionDB (parallel validation, the
upstream default) / TransactionDB (WriteCommitted) at `<db>-<benchmark>`; one op = one transaction, so micros/op is µs per
transaction; conflicts are rolled back and retried and counted in the output), `txnwriteprepared` / `txnwriteunprepared`
(the `txnpessimistic` workload on a WritePrepared / WriteUnprepared TransactionDB; WriteUnprepared transactions are named
and write their batch to the DB as an unprepared batch at half of their puts, `write_batch_flush_threshold`; native legs
select the policy with `rocksdb_transactiondb_options_set_write_policy`).

Flags (same for all engines): `--num`, `--reads` (default num), `--key_size` (16), `--value_size` (100), `--threads`,
`--batch_size` (puts per WriteBatch in fill benchmarks, default 1 = Put), `--multiget_size` (16), `--seek_nexts` (10), `--txn_puts` (10),
`--disable_wal`, `--sync`, `--cache_size` (8 MiB), `--cache_type` (lru|hyperclock), `--bloom_bits` (10),
`--write_buffer_size` (64 MiB), `--block_size` (4096), `--compression` (none|snappy|lz4|zstd), `--compression_parallel_threads` (1; managed only),
`--compression_ratio` (0.5), `--target_file_size_base` (64 MiB), `--max_bytes_for_level_base` (256 MiB),
`--level0_file_num_compaction_trigger` (4), `--disable_auto_compactions`, `--max_background_jobs` (2),
`--enable_blob_files` / `--min_blob_size` (0) (integrated BlobDB; set through the C API for c-native and rocksdb-sharp), `--use_existing_db`,
`--seed` (301), `--db`, `--histogram` (1), `--warmup` (true) / `--warmup_ops` / `--warmup_min_ms` (1000) (see "Warm-up"). `--async` (managed only) runs each worker as a task over
`PutAsync`/`TryGetAsync`/`DeleteAsync`/`WriteAsync` and the async iterator, and `flush`/`compact`/`waitforcompaction` over
`FlushAsync`/`CompactRangeAsync`/`WaitForCompactAsync`, and `multireadrandom` over `MultiGetAsync`.

`stats` prints, for every engine through `GetProperty` (`rocksdb_property_value` in C):

```
stats             : files per level [3 0 0 0 0 4 15]            rocksdb.num-files-at-level0..6
Cumulative compaction: 0.32 GB write, ...                       verbatim from rocksdb.stats (flush + compaction output)
stats             : user 110.6 MB, flush+compaction written 327.7 MB, write-amp 2.96, pending compaction 0 bytes
STATS,c-native,3 0 0 0 0 4 15,116000000,343597384,2.96,0        machine-readable (compare -> <csv>.lsm.csv)
```

`user` = key + value bytes written by `fillseq`/`fillrandom`/`overwrite`/`fillsync` and the `readwhilewriting` writer
since the DB was created; write-amp = flush + compaction bytes written / user bytes (the WAL is not counted). The written
bytes come from the "Cumulative compaction: X GB write" line, i.e. have a resolution of 0.01 GB.

Equivalence guarantees (see `DbBench/Workload.cs` and `native-c/rocksdb_bench.c`, which must stay in sync):

- keys: db_bench `GenerateKeyFromInt` — 8-byte big-endian number padded with `'0'`; `readmissing` replaces the last
  byte with `'.'`;
- values: db_bench `RandomGenerator` — 1 MiB of 100-byte pieces, each a random printable string of
  `100 * compression_ratio` bytes repeated; values are consecutive windows of it;
- random keys: SplitMix64 seeded per (benchmark position, thread), uniform in `[0, num)` — all engines read and write
  the same key sequence;
- total ops (`--num` for writes, `--reads` for reads) are split across threads; a fresh DB for
  `fillseq`/`fillrandom`/`fillsync` unless `--use_existing_db`;
- options: create_if_missing, compression on all levels, write_buffer_size, block-based table format_version 6,
  block_size, LRU (or HyperClock) block cache, full bloom filter (`bloomfilter:N:false`), WAL/sync from flags; all
  other options are the engines' defaults;
- output: `name : X micros/op Y ops/sec Z seconds N operations; M MB/s | p50 p99 p99.9 us`, then a
  machine-readable `RESULT,engine,benchmark,threads,ops,seconds,micros_per_op,ops_per_sec,mb_per_sec,p50_us,p99_us,p999_us,found,alloc_b_per_op,gc0,gc1,gc2,jit_methods,jit_ms`
  line. µs/op = wall time / total ops (like db_bench); percentiles come from a per-operation latency histogram
  (log-linear buckets, <= 1.6% error; per batch for `multireadrandom` and batched writes). The managed runner adds
  allocated bytes per op (all threads, `GC.GetTotalAllocatedBytes`), gen0/1/2 collection counts and the methods
  JIT-compiled during the measurement (`System.Runtime.JitInfo`, all threads); the C leg leaves these columns empty.
  The managed human-readable line also ends with `cpu X us/op`: process CPU time (all threads, incl. background jobs)
  per operation, which is far less sensitive to other load on the machine than wall time and shows spinning and
  cache-line contention (not in the CSV).
- managed-only flags (not accepted by the C leg, so not usable with `compare`): `--allow_concurrent_memtable_write`,
  `--enable_write_thread_adaptive_yield` (both default true, like the engines' defaults).

The C leg uses `rocksdb_get` (+ `rocksdb_free`) for point reads, like rocksdb-sharp's span `Get`, and
`rocksdb_batched_multi_get_cf` with pinnable slices for MultiGet, which rocksdb-sharp also calls (through
`RocksDbSharp.Native`, pinned buffers) instead of its allocating `MultiGet(byte[][])`.

### native-c build

`make -C bench/native-c` compiles `rocksdb_bench.c` against `native-c/include/rocksdb/c.h`, the header of the
**v11.12** release that matches the NuGet library (the parent tree's `include/rocksdb/c.h` is newer), and links the
NuGet `librocksdb.so` (path from `NativeRocksDbPackageVersion` in `Directory.Build.props`, override with
`ROCKSDB_LIB_DIR=`). The library's SONAME is `librocksdb.so.<major>.<minor>`, so the build creates
`native-c/lib/<soname>` as a symlink to the NuGet file and sets the binary's rpath to `$ORIGIN/lib`: it runs without
`LD_LIBRARY_PATH` and loads exactly the file rocksdb-sharp loads.

## BenchmarkDotNet micro benchmarks

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --list flat
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*'
```

(Add `--cli /root/.dotnet/dotnet` if `dotnet` is not on the PATH of the child processes.)

### Managed vs rocksdb-sharp head-to-head (`DbHeadToHeadBenchmarks.cs`)

Every class is parameterized by `Engine = managed | rocksdb-sharp`, has `[MemoryDiagnoser]` and the category
`DbHeadToHead` plus one per operation:

| category | class | measures (per op) |
|---|---|---|
| `Put` | `DbPutBenchmarks` | Put of a random 16 B key / 100 B value, WAL on |
| `WriteBatch` | `DbWriteBatchBenchmarks` | Clear + 100 Put + Write of a reusable batch (per batch) |
| `Get` | `DbGetBenchmarks` | Get with `Location` = `Memtable` (100k keys in the memtable), `SstCached` (flushed, all blocks in a 64 MiB cache), `Miss` (absent key, bloom filter) |
| `Iterator` | `DbIteratorBenchmarks` | Seek to a random key + `Nexts` = 10 / 100 Next, reading key and value (per seek) |
| `MultiGet` | `DbMultiGetBenchmarks` | MultiGet of 16 random existing keys from cached SST blocks (per batch) |

```bash
# all head-to-head benchmarks
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbHeadToHead
# one operation, short job
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories Get --job short
# by class name
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*DbIteratorBenchmarks*'
```

They use the same `IKeyValueEngine` adapters as the dbbench runner (interface dispatch costs ~1 ns per call on both
sides). The `Allocated` column should be `-` for both engines (managed `MultiGet` uses a reusable `MultiGetBatch`).

### Managed sync vs async (`DbAsyncBenchmarks.cs`, category `DbAsync`)

Put / TryGet / Seek + 10 Next / MultiGet(16) through the synchronous and the `ValueTask` API of RocksDb.Managed, with
`Blocks` = `BlockCache` (64 MiB cache, all blocks resident) or `PageCache` (1 MiB cache: most lookups read the block
from the file, which is in the OS page cache). `Allocated` should be `-` everywhere and async within a few percent of
sync (see `docs/perf.md`).

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbAsync --job short
```

### Component benchmarks

`TableBenchmarks`, `IteratorBenchmarks`, `MemTableBenchmarks`, `SkipListBenchmarks`, `CacheBenchmarks`,
`TableConcurrentGetBenchmarks`, `FilterBenchmarks`/`HashBenchmarks`/`CompressionBenchmarks`
(category `FiltersHashCompression`), `WalBenchmarks`, `WriteBatchBenchmarks`, `EnvBenchmarks`, `UtilBenchmarks`
measure managed internals (some against native counterparts); see `docs/perf.md` for methodology and results.

## Replication benchmarks (`bench/RocksDb.Managed.Replication.Benchmarks`)

A separate runner for the replication layer (so the main benchmark project does not depend on MagicOnion):

```bash
# the default suite: 8 stream configurations + switchover over both transports (~15 min on 4 vCPUs)
dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- all
# WAL-stream throughput and primary -> replica lag (--rate 0 = as fast as possible, else total ops/s)
dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- stream \
    --transport both --num 200000 --batch 1 --value_size 100 --threads 4 --rate 0 --chunk_bytes 262144 --repeat 1
# planned switchover: writes held per handoff, client-visible gap
dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- switchover \
    --transport both --handoffs 10 --writers 4 --interval_ms 300
```

`--transport` is `inmem`, `grpc` or `both`. `stream` runs writers against a primary on the local file system and a
replica that follows its WAL stream (`ReplicationPrimary.StreamUpdatesAsync` -> transport -> `ReplicationConsumer`),
over an in-memory channel transport (payloads copied) or MagicOnion/gRPC on loopback. Lag is measured per write, from
the write's completion on the primary to the replica having applied its sequence number. Each measured run follows a
full-size warm-up run on throwaway databases; the `JIT` column counts methods compiled during the measured run. At
`--rate 0` the lag is dominated by queueing (the transport buffers up to 16 chunks); use a fixed `--rate` for the
latency of an unsaturated stream. `switchover` runs a `PairNodeHost` pair with client writers that follow primary
hints and reports the first handoff of the process (cold promote path) separately from the rest. Results:
`docs/benchmarks.md`, "Replication".
