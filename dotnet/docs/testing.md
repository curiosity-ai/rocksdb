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
| `tests/RocksDb.Managed.Tests` | 855 | none | formats, data structures, DB behavior, crash consistency, fault injection, concurrency, upstream test vectors |
| `tests/RocksDb.Managed.CompatTests` | 218 | in process | byte-level and file-level compatibility with `librocksdb` |
| `tests/RocksDb.Managed.RoundTrip` | 41 | child process | whole-DB managed and native alternation, crashes, features |
| `tests/RocksDb.Managed.Replication.Tests` | 30 | in process (one test) | replication, switchover, Raft cluster, gRPC transport, native replica |

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
comparators, properties and statistics, SetOptions, wide columns, WriteBatchWithIndex) and `Validation` (regression
tests for the source audits in [validation.md](validation.md)).

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
`Compaction` (DBs compacted by the other engine; a randomized differential against native), `Env` (the `LOCK` file
across engines), and `Api` (backup, BlobDB, custom comparators, properties and statistics, replication, `SetOptions`,
timestamps, transactions, wide columns, `WriteBatchWithIndex`).

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
implemented on Linux and Windows only, native rounds filter sizes with `malloc_usable_size` only on Linux, and native
BackupEngine 11.12 writes empty meta files when hosted by .NET 11. A test that depends on a format newer than the NuGet
package's 11.12 (the C++ tree is 11.14) must be gated the same way.

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
filter, write buffer size, auto compaction, WAL-only), `CrashRecoveryRoundTripTests` (writer killed, the other engine
recovers), `FeatureRoundTripTests` (edge-case keys and values up to 1.5 MiB, 200k to 1M keys across levels, SST
ingestion, checkpoints, BlobDB, backups, TTL, OPTIONS files, user-defined timestamps) and `HarnessSelfTests` (the
harness reports crashes, asserts, hangs and verification failures correctly).

Environment variables:

| Variable | Effect |
|---|---|
| `ROUNDTRIP_SEED=<n>` | replay a failed run: the failure message prints `seed=<n> (rerun with ROUNDTRIP_SEED=<n>)` |
| `ROUNDTRIP_LONG=1` | enable `Long_ManyAlternations` (20 alternations x 6,000 ops x 6 variants); otherwise it is reported as inconclusive |
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
- `GrpcTests`: two-node replication, a switchover pair and a three-node cluster over real gRPC on localhost.
- `NativeReplicaTests`: a native rocksdb-sharp replica applies the managed WAL chunks.

Nodes talk over an in-memory transport (channels) except in `GrpcTests`. `testconfig.json` caps each test at 60 s.
The multi-process versions are the programs in `samples/ReplicationTest`, `samples/SwitchoverTest` and
`samples/ClusterTest` (`dotnet run --project samples/ClusterTest`). Each coordinator checks the final digests and
exits non-zero on a mismatch.

## The QuickStart sample

`samples/QuickStart` runs every README example and checks its results. It exits with code 1 if an example fails:

```bash
dotnet run --project samples/QuickStart -f net10.0
python3 samples/QuickStart/sync-readme.py --check     # README snippets match the sample code
```

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
