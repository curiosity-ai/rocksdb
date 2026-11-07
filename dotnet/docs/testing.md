# Testing RocksDb.Managed

The port is tested at four levels:

1. **Unit tests**: the managed code on its own, mostly on an in-memory file system.
2. **Compat tests**: managed and native RocksDB run in the same process and exchange bytes and files.
3. **Round-trip tests**: both engines take turns on one database directory, and the native side runs in a child
   process.
4. **Replication tests**: the replication, switchover and Raft layer, in process and over gRPC.

All projects use MSTest on the Microsoft Testing Platform (`global.json` sets `"test": { "runner":
"Microsoft.Testing.Platform" }`) and target `net10.0` and `net11.0`. The native reference is the `RocksDB` NuGet
package (rocksdb-sharp, `librocksdb` 11.12, version in `Directory.Build.props` `NativeRocksDbPackageVersion`). It
ships prebuilt libraries for Linux, macOS and Windows, so no C++ build is needed.

| Project | Tests (net10.0) | Native code | What it covers |
|---|---:|---|---|
| `tests/RocksDb.Managed.Tests` | 1999 | none | formats, data structures, DB behavior, crash consistency, fault injection, concurrency, upstream test vectors |
| `tests/RocksDb.Managed.CompatTests` | 490 | in process | byte-level and file-level compatibility with `librocksdb` |
| `tests/RocksDb.Managed.RoundTrip` | 148 | child process | whole-DB managed and native alternation, option and crash matrices, 2PC, features |
| `tests/RocksDb.Managed.Replication.Tests` | 44 | in process (one test) | replication, switchover, Raft cluster, gRPC transport, native replica |
| `tools/RocksDb.Managed.Soak` (not a test project) | 14 self-test cases | child process (cross-check) | Jepsen-style soak for hours or days: kill, power loss, torn writes, I/O errors, pauses, crash during recovery, file corruption; history checked against a model; see [soak.md](soak.md) |

The counts are test cases as listed by `--list-tests` (data rows count separately) at the time of writing. Both TFMs
run the same tests.

## Running the tests

```bash
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet DOTNET_NOLOGO=1   # the cloud container's SDKs
cd dotnet
dotnet build RocksDb.Managed.slnx

dotnet test --project tests/RocksDb.Managed.Tests -f net10.0
dotnet test --project tests/RocksDb.Managed.Tests -f net11.0
dotnet test --project tests/RocksDb.Managed.CompatTests -f net10.0
dotnet test --project tests/RocksDb.Managed.RoundTrip -f net10.0
dotnet test --project tests/RocksDb.Managed.Replication.Tests -f net10.0

# Without -f, a project runs on both TFMs one after the other.
dotnet test --project tests/RocksDb.Managed.Tests

# A subset (VSTest-style filter expressions), and listing tests:
dotnet test --project tests/RocksDb.Managed.Tests -f net10.0 --filter "FullyQualifiedName~DbTimestampTests"
dotnet test --project tests/RocksDb.Managed.CompatTests -f net10.0 --filter "ClassName~FilterCompatTests"
dotnet test --project tests/RocksDb.Managed.RoundTrip -f net10.0 --no-build --list-tests
```

Add `--no-build` after a solution build to skip the per-project build. A test project is also a plain executable, so
`dotnet run --project tests/RocksDb.Managed.Tests -f net10.0 -- --filter ...` works too.

Before committing, the solution must build with zero warnings on both TFMs, and the unit and compat tests must pass.
The round-trip and replication suites take longer. Run them when you change formats, recovery, the WAL, compaction
or the replication layer.

## Unit tests (`tests/RocksDb.Managed.Tests`)

The folders mirror `src/RocksDb.Managed`: `Util`, `Compression`, `Wal`, `Table`, `Filters`, `Cache`, `Memtable`,
`Iterators`, `Merge`, `RangeDel`, `Options`, `Env`, `Db`, `Blob`, `Transactions`, `Features` (BackupEngine, custom
comparators, properties and statistics, SetOptions, wide columns, WriteBatchWithIndex), `Tracing` (query, block cache
and IO tracing, replay), `Tools` (every `ldb` / `sst_dump` command of `tools/RocksDb.Managed.Tools`, run in process
through `ToolMain.Run` on a `MemoryFileSystem`) and `Validation` (regression tests for the source audits in
[validation.md](validation.md)).

Most DB-level tests run on `MemoryFileSystem` (`DbTestUtil.NewFs()`, `DbTestUtil.NewOptions(fs)`), so they are fast
and leave nothing on disk. Tests that need real files (direct I/O, `LOCK`, hard links, `preadv2`) use
`LocalFileSystem` and a temp directory.

Several groups go beyond single-feature tests:

- **Upstream test vectors**: ports of `db/db_iter_test.cc`, `util/coding_test.cc`, `db/dbformat_test.cc`,
  `db/log_test.cc` cases, `write_batch_test.cc` and the transaction and timestamp test files. The class summary names
  the C++ test it ports.
- **Crash consistency** (`Db/DbCrashConsistencyTests.cs`): a deterministic workload with puts, deletes, range deletes,
  sync writes, manual and background flushes, and compactions over two column families. It runs against
  `FaultInjectionFileSystem`, which "crashes" at the N-th mutating file-system operation, for about 300 values of N.
  After a simulated power loss (unsynced data and directory entries dropped) or process kill (torn last write), the
  DB must reopen without corruption, contain every acknowledged synced write, equal some prefix of the issued writes,
  and keep working.
- **Fault injection and background errors** (`Db/ErrorHandlerTests.cs`, `Db/FaultInjectionFileSystem.cs`): I/O
  errors injected into flushes, compactions, MANIFEST writes and WAL writes; the severity mapping, read-only mode,
  automatic recovery of retryable errors, `Resume`, and the listener callbacks.
- **Concurrency and stress** (`Db/DbColumnFamilyAndConcurrencyTests.cs`, `Db/DbLayerAuditTests.cs`,
  `Cache/*CacheTests.cs`, `Transactions/*`): sync and async writers racing flushes, compactions, column family drops
  and `Dispose`; readers during writes; cache stress; transaction contention, deadlock detection and parallel OCC
  validation.
- **Test hooks**: `Db/TestHooks.cs` in the library lets tests pause or fail background work at defined points, so
  races are reproduced deterministically instead of with sleeps.

## Compat tests (`tests/RocksDb.Managed.CompatTests`)

These tests load the native library in process through rocksdb-sharp. Its namespace is `RocksDbSharp`; always qualify
it (`RocksDbSharp.RocksDb`) to avoid clashing with the managed types. Each test has one of three shapes:

1. **Byte-identical**: both engines produce an artifact from the same input and options, and the bytes are compared.
   Examples: WriteBatch reps (native `WriteBatch.ToBytes()`), SST files from native `SstFileWriter` against managed
   `SstFileWriter` (data, index and filter blocks, properties, footer) for every `format_version`, checksum and filter
   type, filter blocks, MANIFEST records re-encoded, blob files, dictionary-compressed ZSTD blocks, 2PC WAL records.
2. **Native writes, managed reads**: native creates a WAL, SST, MANIFEST, OPTIONS file, blob file, backup or whole DB;
   the managed port opens it and must return the same data (iterators compared entry by entry, including snapshots
   and bounds).
3. **Managed writes, native reads**: the managed port creates the artifact; native opens it, ingests it or replays
   it, and must return the same data. Native `GetUpdatesSince` must return the same batches the managed WAL holds.

The folders follow the formats: `Wal` (WAL and WriteBatch), `Table` (SST, prefix and partitioned indexes and filters,
table integration), `Filters`, `Compression` (all codecs plus dictionaries), `Manifest`, `Memtable`, `Iterators`,
`Compaction` (DBs compacted by the other engine; a randomized differential against native; a lock-step LSM-shape and
write-amp parity test, `PARITY_TRACE=<file>` keeps its side-by-side trace), `Env` (the `LOCK` file
across engines), `Tracing` (query traces replayed in both directions through the native `rocksdb_new_default_replayer`
and the managed `Replayer`, native block cache / IO traces parsed by the managed readers, managed ones framed by the
native `rocksdb_trace_reader_read`), and `Api` (backup, BlobDB, custom comparators, properties and statistics,
replication, `RepairDB` in both directions, `SetOptions`, timestamps, the `ldb` / `sst_dump` tools on native DBs,
transactions, wide columns, `WriteBatchWithIndex`).

C API functions that rocksdb-sharp's `Native` class does not bind are resolved from the library it loaded (see
`Infrastructure/NativeExtras.cs` and `Tracing/TraceCompatTests.cs`).

### What "compat" guarantees

A passing compat suite means that, for librocksdb 11.12 and the option combinations under test:

- every persisted format listed in the [README](../README.md#status-and-compatibility) is read by both engines;
- the formats marked byte-identical are byte-identical, so checksums, file sizes and table properties match;
- a DB directory can move from one engine to the other in both directions, including recovery from WAL-only data,
  column families, merges, range deletions, blob files, timestamps and prepared transactions.

It does not mean that both engines make the same decisions. Compaction picking, file boundaries, flush timing and
compressed block sizes may differ between engines. Those choices are compared with the benchmarks (LSM shape and write
amplification), not with assertions. The known deviations are listed in the README and in
[validation.md](validation.md).

Tests that cannot run in an environment end with `Assert.Inconclusive` instead of failing: `LOCK` interop is
implemented on Linux and Windows only, and native rounds filter sizes with `malloc_usable_size` only on Linux. (The
native-backup tests used to be inconclusive under .NET 11: the packaged librocksdb needs `libstdc++.so.6` in the global
symbol scope, which `tests/Shared/NativeStdCxxPreload.cs` now provides in every executable that loads it; see the
backups section of validation.md.) A test that depends on a format newer than the NuGet
package's 11.12 (the C++ tree is 11.14) must be gated the same way.

## The WAL matrix

WAL settings and failure modes are crossed in two matrices; both print their outcome counts to the test output.

**Unit matrix** (`tests/RocksDb.Managed.Tests/Db/WalMatrixTests.cs`, about 10 s): 15 factors, covered pairwise by a
seeded greedy (AETG-style) covering array of 20 rows. Coverage is asserted by `PairwiseCoversEveryPairOfLevels`. The
factors are wal_recovery_mode (4 levels), paranoid_checks, recycle_log_file_num 0/1, track_and_verify_wals,
track_and_verify_wals_in_manifest, manual_wal_flush, wal_compression none/zstd, a separate wal_dir, WAL archival
(WAL_ttl_seconds), a small max_total_wal_size, avoid_flush_during_recovery, avoid_flush_during_shutdown, the write mode
(default, two_write_queues, pipelined, unordered, WritePrepared 2PC with a prepared section spanning WALs; 5 levels),
user-defined timestamps on one column family and blob direct write. Combinations upstream rejects are left out and
checked separately (`UnsupportedCombinationsAreRejected`): blob direct write with two_write_queues, pipelined or
unordered writes, and UDT with WritePrepared.

Every row runs against 15 faults on `FaultInjectionFileSystem`:

- crashes: a clean close, a process kill, a power loss, a WAL sync that fails before the power loss, and a MANIFEST
  write that fails at a flush before the power loss;
- damage: a torn last record, a corrupted middle record in the latest WAL and in the oldest WAL, a zeroed tail, the
  oldest or the latest WAL missing, a stale copy of an old WAL under a newer number, an extra empty WAL, the two latest
  WALs swapped, and a dropped record in the predecessor WAL.

That makes 300 scenarios. A model of the committed writes checks the invariants:

- A crash always reopens and keeps every write acknowledged before the last successful sync.
- Where upstream recovers a point-in-time prefix, the contents of both column families equal one prefix of the commits.
  That is after a crash, and after damage confined to the latest WAL with paranoid_checks outside
  kSkipAnyCorruptedRecords.
- Elsewhere every recovered value is one the key had.
- Damage fails only with Corruption, and the damage upstream recovers from must open.
- The recovered DB accepts a write and reopens with the same contents.

The class also holds the regression tests of the bugs the matrices found.

**Compat matrix** (`tests/RocksDb.Managed.CompatTests/Wal/WalMatrixCompatTests.cs`, about 30 s): crash images (the DB
directory copied while open) are written by either engine with 8 write-time factors, covered pairwise in 8 images. The
factors are the writer engine, recycle_log_file_num, track_and_verify_wals, track_and_verify_wals_in_manifest,
wal_compression, manual_wal_flush, wal_dir and avoid_flush_during_recovery. The workload uses two column families with
different flush progress, fragmented records, cross-CF batches, and a last sync write followed by unsynced writes; with
recycling the latest WAL is a reused one with a stale tail.

Each image gets the same 15 damage kinds as files on disk (`WalFault`). Every damaged copy is opened by the managed
port and by native librocksdb, in each recovery mode with and without paranoid_checks. The undamaged image is also
opened with a WAL filter returning IgnoreCurrentRecord, StopReplay or CorruptedRecord at the 8th record. The filter's
out-of-range values are left to the unit matrix, because the C API maps them to kCorruptedRecord. That makes 1,152
scenarios.

Both engines must agree on success or failure, the status code and message (paths normalized), the contents of both
column families and the last sequence number. Faults that keep the synced data must keep the last acknowledged sync
write. Native runs in process: corrupt WALs make it return a Status, and none of the 36,864 long-variant scenarios
crashed it. `WalRecyclingCompatTests` and `WalCrashFeatureCompatTests` add the recycling interop, recyclable record
bytes, the 2PC point-in-time case and blob direct write crash images.

Environment variables:

| Variable | Effect |
|---|---|
| `WAL_MATRIX_LONG=1` (or `ROUNDTRIP_LONG=1`) | unit: 400 random rows on top of the pairwise ones (6,300 scenarios, about 2.5 min); compat: all 256 write-time combinations (36,864 scenarios, about 15 min) |
| `WAL_MATRIX_REPORT=<file>` | write the outcome counts and every failure / disagreement to the file |
| `WAL_MATRIX_ONLY=<row>` | unit matrix: run only the row with this description (as printed in a failure) |

```bash
WAL_MATRIX_LONG=1 dotnet test --project tests/RocksDb.Managed.Tests -f net10.0 --filter "ClassName~WalMatrixTests"
WAL_MATRIX_LONG=1 dotnet test --project tests/RocksDb.Managed.CompatTests -f net10.0 --filter "ClassName~WalMatrixCompatTests"
```

## Round-trip tests (`tests/RocksDb.Managed.RoundTrip`)

These tests run whole databases through both engines: native creates a DB, managed reopens it and writes, native
reopens and verifies, and so on. The native engine always runs in a **child process**. The test executable starts
itself again with the argument `roundtrip-child` (see `Program.cs` and `Harness/ChildHost.cs`), so a native crash
(SIGSEGV, SIGABRT, access violation), a `Debug.Assert`, or a hang becomes a test failure with the decoded signal or
timeout, and the test host survives. The managed engine runs in process by default, and in a child for the
crash scenarios.

How a test works:

- `RoundTripHarness` owns a temp directory, a random **seed** and a `ReferenceModel` (the expected contents of every
  column family).
- The test builds **op scripts** (`Protocol/Script.cs`: put, delete, delete range, merge, batches, sync writes,
  flushes, compactions, snapshots, SST files and ingestion, checkpoints, backups, TTL, timestamps, verify, dump) with
  `ScriptBuilder` (`h.NewScript(...)`, `b.Add(Op.Put(cf, k, v))`, `b.Verify()`), and runs each one on an engine with
  `await h.RunAsync(EngineKind.Native | EngineKind.Managed, b)`.
- The engine executes the script and reports what it read. The harness compares that with the model and fails with
  the differences.
- `Op.CrashWriter` / `Op.Crash` end the child abruptly after acknowledged writes (no close), and `Op.CrashCheck`
  verifies on the other engine that every acknowledged batch survived, to test unclean shutdowns in both directions.

The suites are `AlternatingRoundTripTests` (randomized alternation with 6 option variants: compression, format version,
filter, write buffer size, auto compaction, WAL-only), `MatrixRoundTripTests` and `CrashMatrixRoundTripTests` (the
combinatorial matrices below), `CrashRecoveryRoundTripTests` (writer killed, the other engine recovers),
`TransactionRoundTripTests` (2PC across engine switches), `FeatureRoundTripTests` (edge-case keys and values up to
1.5 MiB, 200k to 1M keys across levels, SST ingestion, checkpoints, BlobDB, backups, TTL, OPTIONS files, user-defined
timestamps), `SecondaryRecyclingRoundTripTests` and `HarnessSelfTests` (the harness reports crashes, asserts, hangs
and verification failures correctly).

Every option a test uses is a property of `Protocol/DbSpec.cs` (`CfSpec` per column family, `DbWideSpec` for DB
options), applied by both engines (`Engines/NativeEngine.cs` through rocksdb-sharp and the C API,
`Engines/ManagedEngine.cs`). The few settings the C API has no setter for (the capped prefix extractor and the
built-in `max`, `bytesxor` and `StringAppendOperator` with another delimiter) are set natively with
`rocksdb_get_options_from_string`, so native runs the C++ implementations. A `TransactionDb` spec opens a pessimistic
TransactionDB (WriteCommitted) on both sides; natively through `rocksdb_transactiondb_*`, which rocksdb-sharp does not
wrap.

### The option matrix (`MatrixRoundTripTests`)

32 factors, crossed by a seeded greedy pairwise covering array (`Harness/Pairwise.cs`, seed `ArraySeed`): every pair of
levels of every two factors is in at least one case, which `CoveringArrayCoversEveryAllowedPair` asserts. The data
pattern is the exception: case i uses pattern i mod 8, so the 8 patterns don't dominate the array size.

| Group | Factors (levels) |
|---|---|
| table | format_version 2..7; index binary search / hash / two-level / two-level + partitioned filters / binary search with first key; filter Bloom / Ribbon / Bloom + fixed prefix / Bloom + fixed prefix without whole-key filtering + memtable prefix bloom / Ribbon + capped prefix + memtable bloom with whole-key filtering / none; checksum crc32c / xxHash / xxHash64 / XXH3; block size, block_restart_interval, index_block_restart_interval and the data block hash index (4 layouts); optimize_filters_for_hits x cache_index_and_filter_blocks |
| compression | none / Snappy / LZ4 with bottommost ZSTD / ZSTD level 5 / per-level with bottommost ZSTD and dictionaries / ZSTD with a dictionary (max_dict_bytes, zstd_max_train_bytes) and 3 parallel threads |
| LSM | level with dynamic level bytes and small targets (many files and levels) / static levels, num_levels 4 / level defaults / universal (size ratio 1, max_size_amplification_percent 50, trivial moves) / universal (size ratio 10, 5 levels) / FIFO / FIFO with intra-L0 compaction and ttl; compaction_pri (5); max_subcompactions 1 / 4 |
| memtable | skip list / skip list with inplace_update_support / vector / hash skip list; write buffer size, max_write_buffer_number, min_write_buffer_number_to_merge (4) |
| WAL | wal_recovery_mode (4); recycle_log_file_num 0 / 2; manual_wal_flush (with FlushWAL calls); wal_compression ZSTD; track_and_verify_wals / track_and_verify_wals_in_manifest; max_total_wal_size; avoid_flush_during_recovery x avoid_flush_during_shutdown; wal_dir |
| write path | default / pipelined / unordered_write / two_write_queues / no concurrent memtable writes; disableWAL writes mixed in; rare or frequent sync writes |
| data | merge operators of the varied CFs (stringappend ',' and '\|', max, bytesxor; one CF stays uint64add); blob files (min_blob_size 128, compression, GC); 4 or 10 column families with mixed options; ingestion (none / overlapping files with global seqno / ingest_behind / allow_global_seqno=false); manual compaction (default / bottommost force(optimized) + exclusive / change_level / DeleteFilesInRange + DeleteRange); the managed runs with the same or different compatible options (format version, block size, filter, index, checksum, compression, write buffer, WAL compression) |
| patterns | random, sequential, reverse, hot keys, 120-byte shared prefixes, tombstone-heavy (wide overlapping range deletes), merge-heavy chains, tiny and 1-3 MiB values |
| end of a run | clean close / reopen in the middle / process killed after FlushWAL(sync) (`Op.Abandon`) |

Each case has a fixed seed derived from its index (so its op streams repeat run after run) and its configuration in
its name. A case alternates native, managed, native, managed (300 generated ops each, snapshots held across flushes and
compactions), every run first verifies what the other engine left, and both engines then compact and verify the final
state. Combinations upstream rejects, that native 11.12 cannot run, or that are outside the model are excluded
(`Excluded`, with the reason for each; see also [validation.md](validation.md), "Combinatorial round trips").

### The crash matrix (`CrashMatrixRoundTripTests`)

14 factors, pairwise: the writer engine, wal_recovery_mode, recycling, manual_wal_flush, wal_compression, disableWAL
writes, atomic_flush, the write mode (default / pipelined / unordered / two queues), avoid_flush_during_recovery, WAL
tracking, wal_dir, the write buffer size, plain writes or 2PC transactions (each batch prepared and committed, and every
40th an extra transaction left prepared), and the sync interval. The writer is SIGKILLed after a random number of
acknowledged batches; the crash image is copied, the other engine recovers it in place and the writer's engine
recovers the copy. Both must give the same open outcome, batch count, prepared transactions and contents (the image is
random, so each engine is the oracle for the other), keep every acknowledged batch and a consistent batch prefix.
Then the recoverer commits or rolls back the prepared transactions and the writer's engine checks the result.

### 2PC round trips (`TransactionRoundTripTests`)

A TransactionDB alternately opened by both engines (6 rounds, 12 long): each round checks the transactions the
previous engine left prepared were recovered (and are invisible), resolves some, writes, prepares new multi-CF
transactions, flushes between prepare and commit, and ends cleanly or by killing the process.

Counts (net10.0): 148 round-trip test cases, of which 79 option-matrix and 20 crash-matrix cases; about 3 minutes on
4 cores. With `ROUNDTRIP_LONG=1`: 279 option-matrix cases (200 random valid rows on top, 900 ops per run, 6 runs) and
80 crash-matrix cases.

Environment variables:

| Variable | Effect |
|---|---|
| `ROUNDTRIP_SEED=<n>` | replay a failed run: the failure message prints `seed=<n> (rerun with ROUNDTRIP_SEED=<n>)`; it overrides the fixed seeds of the matrix cases too (the crash matrix still kills the writer at a random point) |
| `ROUNDTRIP_LONG=1` | enable `Long_ManyAlternations` (20 alternations x 6,000 ops x 6 variants; otherwise it is reported as inconclusive), add 200 random rows to the option matrix (with 3x longer runs) and 60 to the crash matrix, and 12 rounds to the 2PC test |
| `ROUNDTRIP_MANAGED_IN_CHILD=1` | run the managed engine in a child process too (for example, to capture a managed crash or hang) |
| `ROUNDTRIP_FAILURES_DIR=<dir>` | where failed runs are preserved, instead of `bin/<cfg>/<tfm>/TestResults/roundtrip-failures/` |

A failed test keeps its directory (`<test>-seed<n>/`): the DB, every script, the spec and `harness.log`. Passing
tests delete theirs. The default child timeout is 180 s.

## Replication tests (`tests/RocksDb.Managed.Replication.Tests`)

These tests cover `src/RocksDb.Managed.Replication` and `src/RocksDb.Managed.Replication.MagicOnion`:

- `BuildingBlockTests`: write gate, role state, adaptive commit delay, delta planning, Raft persistent state on
  `MemoryFileSystem`.
- `ReplicationTests`: initial sync through a checkpoint followed by WAL streaming, catch-up after restart, streaming
  under concurrent writes, WAL purge followed by a resync, gap and overlap rejection, archived-WAL retirement.
- `SwitchoverTests`: rolling upgrade both ways, lagging replica, lost or late promote replies, an unreachable target,
  forced failover and rebuild of a diverged log.
- `ClusterTests`: bootstrap to cluster mode, re-election, quorum commit, a far-behind follower catching up through a
  snapshot, term-log persistence.
- `ClusterPartitionTests`: the leader partitioned from the majority (a new leader in a later term, no acknowledgement
  on the minority, the old leader steps down and rebuilds after healing) and a follower partitioned and healed, with a
  client writing through whichever node claims leadership; no acknowledged write may be lost.
- `GrpcTests`: two-node replication, a switchover pair and a three-node cluster over real gRPC on localhost.
- `NativeReplicaTests`: a native rocksdb-sharp replica applies the managed WAL chunks.

Nodes talk over an in-memory transport (channels) except in `GrpcTests`. Its fault hooks (`InMemoryNetwork.SetPartitioned`,
`Isolate`, `Heal`) cut links: unary calls fail and the streams open across a cut link are torn down, like a dropped
connection. `testconfig.json` caps each test at 60 s.
The multi-process versions are the programs in `samples/ReplicationTest`, `samples/SwitchoverTest` and
`samples/ClusterTest` (`dotnet run --project samples/ClusterTest`). Each coordinator checks the final digests and
exits non-zero on a mismatch.

## Soak test (`tools/RocksDb.Managed.Soak`)

A Jepsen-style stability test to leave running for days: a supervisor runs a worker process on one database, ends it
with a fault (SIGKILL, simulated power loss and LazyFS-style torn writes on an in-process fault file system, I/O
errors, SIGSTOP pauses, a crash during recovery, a clean shutdown), and a verifier process checks the recovered
database against the recorded history (durability per fault type, per-writer prefixes, bank total, counters,
range deletes, scans, file checksums, a native cross-check). See [soak.md](soak.md).

```bash
tools/soak.sh --dir /data/soak --duration 72h      # the soak
tools/soak.sh --dir "$(mktemp -d)" --smoke         # 2-minute smoke run
tools/soak.sh selftest                             # every check catches its injected bug (about 1 minute)
```

## The QuickStart sample

`samples/QuickStart` runs every README example and checks its results. It exits with code 1 if an example fails:

```bash
dotnet run --project samples/QuickStart -f net10.0
python3 samples/QuickStart/sync-readme.py --check     # README snippets match the sample code
```

## Allocation profile (`tools/RocksDb.Managed.AllocationProfile`)

Not a test project, but the check for the "allocation-free hot paths" goal across the whole public API (BenchmarkDotNet
`[MemoryDiagnoser]` covers single micro-benchmarks). Each scenario runs in a child process: warm-up, an exact measured
phase (`GC.GetTotalAllocatedBytes(precise: true)` per operation, all threads, plus the calling thread's counter), then a
Memory.Introspect allocation trace of the steady-state loop for the allocating types and call stacks. Reports go to
`allocations.md` / `allocations.json`.

```bash
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- list
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- run get iterator --out /tmp/alloc
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- run all --native --trace 0   # exact numbers only
```

A full traced run takes about 45 minutes; `--trace 0` and a group filter make it a few minutes. After a change on a hot
path, run the affected group and compare the B/op column. A hot-path scenario that was 0 B/op must stay at 0. See
[allocations.md](allocations.md#measuring-allocations) for the options and for how to add a scenario.

## Adding a test

- **Pick the project by what you need**: managed behavior goes in the unit tests; anything persisted needs a
  compat test as well (CLAUDE.md: "every format component needs round-trip unit tests and a compat test against
  native RocksDB where the native side can produce or consume the artifact"); a change to whole-DB behavior across
  engines (recovery, compaction output, new file types) goes in a round-trip scenario.
- **Unit tests**: use `MemoryFileSystem` through `DbTestUtil` unless the test needs real files. Port upstream test
  cases where they exist and name the C++ test in the class or method summary.
- **Status checks**: assert the code, not just the exception type:
  `var ex = Assert.ThrowsExactly<RocksDbException>(() => ...); Assert.AreEqual(StatusCode.InvalidArgument, ex.Code);`
  (the transaction tests have an `AssertFails(code, action, subCode)` helper).
- **Compat tests**: use `Infrastructure/TempDir` (`using var dir = new TempDir("name")`) and the native helpers in
  `Wal/NativeDbHelper.cs` and `Filters/NativeSst.cs`. Prefer comparing bytes when the format is deterministic, and
  compare full iteration results otherwise.
- **Round-trip tests**: derive from `RoundTripTestBase`, create a harness with `CreateHarness(DbSpec.Standard(...),
  variant)`, and build scripts. A new kind of operation needs an `Op` in `Protocol/Script.cs`, an implementation in
  both `Engines/NativeEngine.cs` and `Engines/ManagedEngine.cs`, and model support in `Protocol/ReferenceModel.cs`.
- **No sleeps**: synchronize with `TestHooks`, `WaitForCompact`, `FlushAsync` or the listener callbacks.
- **Clean up**: temp directories go under `Path.GetTempPath()` and are deleted when the test finishes.
- **Flakiness**: run a new concurrency test many times before committing, for example
  `for i in $(seq 20); do dotnet test --project tests/RocksDb.Managed.Tests -f net10.0 --no-build --filter "Name=MyTest" || break; done`,
  on both TFMs.
