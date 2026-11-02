# Benchmarks

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
`results/2026-11-02-vm-compaction.md`.

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
transaction; conflicts are rolled back and retried and counted in the output).

Flags (same for all engines): `--num`, `--reads` (default num), `--key_size` (16), `--value_size` (100), `--threads`,
`--batch_size` (puts per WriteBatch in fill benchmarks, default 1 = Put), `--multiget_size` (16), `--seek_nexts` (10), `--txn_puts` (10),
`--disable_wal`, `--sync`, `--cache_size` (8 MiB), `--cache_type` (lru|hyperclock), `--bloom_bits` (10),
`--write_buffer_size` (64 MiB), `--block_size` (4096), `--compression` (none|snappy|lz4|zstd),
`--compression_ratio` (0.5), `--target_file_size_base` (64 MiB), `--max_bytes_for_level_base` (256 MiB),
`--level0_file_num_compaction_trigger` (4), `--disable_auto_compactions`, `--max_background_jobs` (2),
`--enable_blob_files` / `--min_blob_size` (0) (integrated BlobDB; set through the C API for c-native and rocksdb-sharp), `--use_existing_db`,
`--seed` (301), `--db`, `--histogram` (1). `--async` (managed only) runs each worker as a task over
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
  machine-readable `RESULT,engine,benchmark,threads,ops,seconds,micros_per_op,ops_per_sec,mb_per_sec,p50_us,p99_us,p999_us,found,alloc_b_per_op,gc0,gc1,gc2`
  line. µs/op = wall time / total ops (like db_bench); percentiles come from a per-operation latency histogram
  (log-linear buckets, <= 1.6% error; per batch for `multireadrandom` and batched writes). The managed runner adds
  allocated bytes per op (all threads, `GC.GetTotalAllocatedBytes`) and gen0/1/2 collection counts.

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
