# RocksDB.Managed — C# port of RocksDB

This folder contains a **fully managed, async-first C# port of RocksDB** targeting
**.NET 10 and .NET 11**. The C++ sources in the parent directory are the reference
implementation; this port lives entirely under `dotnet/`.

The parent `../CLAUDE.md` applies to the C++ code. For work under `dotnet/`, this file wins.

## Goals (in priority order)

1. **On-disk binary compatibility** with upstream RocksDB. A database directory written
   by native RocksDB must open and read correctly with the managed port, and vice versa.
   This covers every persisted format:
   - WAL (`log_format.h`, `log_writer.cc`, `log_reader.cc`, incl. recyclable records)
   - WriteBatch representation (`db/write_batch.cc`) — also the replication/WAL-tailing wire format
   - MANIFEST / `VersionEdit` (`db/version_edit.cc`), `CURRENT`, `IDENTITY`, `LOCK`
   - `OPTIONS-xxxxxx` files (`options/options_parser.cc`)
   - Block-based SST tables (`table/block_based/*`, `table/format.cc`): blocks + restart
     points, index blocks, metaindex, properties, footer (all supported `format_version`s),
     checksums (crc32c, xxHash, xxHash64, XXH3), compression (none/snappy/lz4/zstd),
     full filters (FastLocalBloom, legacy Bloom, Ribbon)
   - File naming (`file/filename.cc`)
2. **Allocation-free hot paths.** Reads, writes, iteration and compaction must not allocate per
   operation in steady state. Use `Span<T>`/`ReadOnlySpan<T>`, `ArrayPool<byte>`,
   `PooledBufferWriter`, pooled objects, `ref struct` enumerators, `stackalloc` for small
   scratch, and `PoolingAsyncValueTaskMethodBuilder` for async methods on hot paths.
   Verify with BenchmarkDotNet `[MemoryDiagnoser]` — "Allocated" should be `-` for hot paths.
3. **Async first.** All I/O-bound public APIs are `ValueTask`-returning (`PutAsync`, `GetAsync`,
   `WriteAsync`, `FlushAsync`, `CompactRangeAsync`, `IAsyncEnumerable` scans...). Cache hits
   complete synchronously. Synchronous convenience overloads exist where they are cheap and
   don't hide blocking I/O on the thread pool. File I/O uses `RandomAccess` on `SafeFileHandle`.
4. **Performance parity** with native RocksDB (via rocksdb-sharp), measured by the benchmark
   project. Every perf-sensitive change should come with a benchmark number.
5. **Good ergonomics.** We are free to reshape the API (see "Public API design").
   We do NOT mirror the C API or rocksdb-sharp's low-level surface.

## Layout

```
dotnet/
  CLAUDE.md, todo.md            this file / progress tracker (update todo.md as you go)
  global.json                   pins the .NET 11 SDK (rollForward latestMajor)
  Directory.Build.props         net10.0;net11.0, nullable, unsafe, central package mgmt
  Directory.Packages.props      ALL package versions live here
  RocksDb.Managed.slnx
  src/RocksDb.Managed/          the port (namespace RocksDb.Managed[.Sub])
    Core/                       Status/exception types, public API types
    Util/                       coding, crc32c, hashing, pooled buffers, comparators
    Compression/                snappy/lz4/zstd adapters with RocksDB block compression framing
    Wal/                        log format reader/writer
    Db/                         dbformat, WriteBatch, memtable, version set, DB impl, flush, compaction
    Table/                      block-based table builder/reader, blocks, index, filters, footer
    Options/                    options model + OPTIONS file read/write
    Env/                        file system abstraction (async I/O)
    Cache/                      block cache
  tests/RocksDb.Managed.Tests/        unit tests (MSTest), pure managed
  tests/RocksDb.Managed.CompatTests/  binary-compat tests against native RocksDB (in-process)
  tests/RocksDb.Managed.RoundTrip/    managed<->native DB round trips; native side runs in a child process
                                      (the test host re-invokes itself) so native crashes are captured
  src/RocksDb.Managed.Replication/    replication / switchover / Raft cluster layer (ported from rocksdb-sharp), transport-agnostic
  src/RocksDb.Managed.Replication.MagicOnion/  optional gRPC (MagicOnion) services/clients for it
  tests/RocksDb.Managed.Replication.Tests/     in-process (in-memory transport), native-replica and gRPC tests
  samples/{ReplicationTest,SwitchoverTest,ClusterTest}/  multi-process gRPC programs (ports of rocksdb-sharp's Tests/*)
  bench/RocksDb.Managed.Benchmarks/   BenchmarkDotNet: managed port vs native RocksDB
```

## Relationship with rocksdb-sharp

rocksdb-sharp (`../../rocksdb-sharp`, NuGet `RocksDB`) stays a binding over the **native** library and is **not**
going to be integrated with or replaced by this port. This project must therefore implement everything users need
on its own, including features that rocksdb-sharp layers on top of native RocksDB (e.g. its `Replication/`,
`Replication/Switchover/` and `Cluster/` code is ported into `src/RocksDb.Managed.Replication`). rocksdb-sharp is used
here only as (a) the native reference implementation in compat/round-trip tests and benchmarks and (b) a source of
API/feature requirements. Never edit the rocksdb-sharp repo from this project's work.

## Reference implementation (native)

The native RocksDB is consumed through the **`RocksDB` NuGet package** (rocksdb-sharp,
`../../rocksdb-sharp` when checked out side by side; version in `Directory.Build.props`
`NativeRocksDbPackageVersion`). It ships prebuilt `librocksdb` for linux/osx/win, so we do
not compile C++ for tests. Its C# namespace is `RocksDbSharp` — always qualify it
(`RocksDbSharp.RocksDb`) in compat tests and benchmarks to avoid clashing with ours.

The NuGet package tracks the latest RocksDB release (11.12 at the time of writing) while the C++
tree here is `main` (11.14). On-disk formats are stable across these; if a compat test
depends on a feature newer than the package, gate it.

If a format can only be validated with a C++ helper (e.g. `ldb`/`sst_dump`), building that tool
from the parent tree is acceptable (`make ldb sst_dump -j$(nproc)`), see
`../../rocksdb-sharp/build-native/` for the build scripts used for the NuGet package.

## Build & test

```bash
export PATH=/root/.dotnet:$PATH       # SDKs installed in ~/.dotnet in the cloud container
cd dotnet
dotnet build RocksDb.Managed.slnx
dotnet test --project tests/RocksDb.Managed.Tests -f net10.0
dotnet test --project tests/RocksDb.Managed.CompatTests -f net10.0
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*'
```

Before committing: the solution builds with **zero warnings** in new code on both TFMs, and unit +
compat tests pass.

## Porting conventions

- **Port faithfully, then idiomatically.** Each managed file should cite the C++ file(s) it ports
  in its summary comment (e.g. `/// Port of table/block_based/block_builder.cc`). Keep algorithm
  structure recognisable so upstream fixes can be followed. Constants, enum values, magic numbers
  and field orderings that hit disk must be copied exactly — cite the source line when non-obvious.
- **Slice → `ReadOnlySpan<byte>`** (sync code) / `ReadOnlyMemory<byte>` (when it must cross an
  `await` or be stored). `std::string* dst` outputs → `IBufferWriter<byte>` (usually
  `PooledBufferWriter`).
- **Status → exceptions at the public boundary**, `bool Try...(out ...)` internally for expected
  failures (NotFound, truncated input). Corruption/IO errors throw `RocksDbException` with the
  matching `StatusCode`. Keep throw sites in `ThrowHelper` so hot methods stay inlinable.
- **Naming**: the DB class is `RocksDatabase` (a class named `RocksDb` breaks `RocksDb.Managed.*` qualified names); C++ `Version` is `DbVersion`; C++ `ValueType` is `Db.EntryType` (avoids clashing with `System.ValueType`). Public user-facing options live in `Options/` (`RocksDb.Managed.*Options`); runtime/resolved settings used internally by a subsystem get distinct names (e.g. `Table.TableFormatOptions`). Enums are defined once.
- **Encoding helpers**: use `Util.Coding` (varints/fixed LE) and `Util.Crc32C` — don't re-implement.
- **No LINQ, no closures, no boxing, no `params` arrays, no string formatting** on hot paths.
  Prefer `sealed` classes, `readonly struct`, `static` lambdas, `[SkipLocalsInit]` where it helps.
- **Unsafe code** is allowed where it measurably helps (pinning, `Unsafe.ReadUnaligned`), but
  prefer safe span code first.
- **Threading**: RocksDB's mutex/condvar designs map to `Lock` (.NET 9+), `SemaphoreSlim` for
  async waits, `Channel<T>` for queues, `Interlocked`/`Volatile` for atomics. Document
  synchronization assumptions like the C++ code does.
- **Endianness**: always `BinaryPrimitives`/`Coding` little-endian helpers; never `MemoryMarshal.Read`
  of multi-byte integers from disk data.
- **Tests**: every format component needs (a) round-trip unit tests and (b) a compat test against
  native RocksDB where the native side can produce/consume the artifact (WriteBatch bytes via
  `WriteBatch.ToBytes()`, SST files via `SstFileWriter`/`IngestExternalFiles`, whole DB dirs,
  WAL via `GetUpdatesSince`). Use temp directories under `Path.GetTempPath()` and clean up.
  Use `Assert.AreEqual(StatusCode.X, ex.Code)` rather than bare `Assert.ThrowsException`.

## Public API design

rocksdb-sharp's high-level API (`RocksDb`, `Iterator`, `WriteBatch`, `*Options`) was reviewed as
the starting point. What we keep and what we change:

| rocksdb-sharp | Problem | Managed port |
|---|---|---|
| `Get(...)` returns `byte[]`; `Iterator.Key()/Value()` return `byte[]` | allocation per call | `TryGet(key, IBufferWriter<byte>)`, `Get<TState,TResult>(key, state, ReadOnlySpanFunc)` zero-copy callback, `GetPooled(key)` → `PooledValue : IDisposable`; iterator exposes `ReadOnlySpan<byte> Key/Value` |
| `ISpanDeserializer<T>`, `GetFixedSizeValue(key, Span<byte>)` | good ideas | kept (generalised as span callbacks / `TryGet(key, Span<byte>, out int written)`) |
| Optional defaulted params everywhere (`cf = null, readOptions = null, encoding = null`) | hidden behaviour, overload explosion | column family is the receiver (`db.Get` = default CF, `cf.Get` on a `ColumnFamily` object); options are explicit overloads |
| `string` overloads with `Encoding` param | allocations, ambiguous | none in core; UTF-8 helpers as extension methods in a separate namespace |
| C-style `Iterator` (`Valid()`, `Next()`, IntPtr handle) | not idiomatic | `Iterator` keeps Seek/Next/Prev for power users, plus `foreach` via `ref struct` enumerator over a range and `IAsyncEnumerable<KeyValue>` for async scans |
| Options = opaque native handles with `SetXxx()` (C API can't read back) | can't inspect, mutable after open | plain classes with readable properties (`DbOptions`, `ColumnFamilyOptions`, `BlockBasedTableOptions`, `ReadOptions`, `WriteOptions`) round-trippable to the `OPTIONS` file format |
| `MergeOperator`/`Comparator` interfaces with `IntPtr` params, `Marshal.AllocHGlobal` per merge | unsafe + allocating | abstract classes over spans; merge results written to an `IBufferWriter<byte>` |
| `References` `dynamic`/`ExpandoObject` to keep delegates alive | not needed in managed code | removed |
| Sync only | blocks threads | `ValueTask` async APIs, async group commit for writes |
| `Checkpoint`, `GetUpdatesSince`, `GetLatestSequenceNumber`, `GetLiveFilesMetadata`, `IngestExternalFiles`, `SstFileWriter`, snapshots, column families, `Flush`, `CompactRange`, `GetProperty`, secondary/read-only open | used by downstream replication/cluster code | all kept as first-class APIs (WAL tailing returns pooled batches) |

Sketch of the target surface (update as it's implemented):

```csharp
await using var db = await RocksDatabase.OpenAsync("/data/db", new DbOptions { CreateIfMissing = true });
await db.PutAsync("k"u8.ToArray(), value);                   // ValueTask; batched group commit
db.Put("k"u8, value);                                        // sync convenience
if (db.TryGet("k"u8, bufferWriter)) { ... }                  // copies into caller buffer
int len = db.Get("k"u8, state, static (v, s) => v.Length);   // zero-copy callback
using PooledValue? pv = await db.GetPooledAsync(key);        // pooled ReadOnlyMemory<byte>

using var batch = db.CreateWriteBatch();                     // pooled, reusable
batch.Put(k1, v1).Delete(k2).Merge(cf, k3, v3);
await db.WriteAsync(batch, WriteOptions.Sync);

using var snap = db.GetSnapshot();
using var it = db.NewIterator(new ReadOptions { Snapshot = snap, UpperBound = end });
for (it.Seek(start); it.Valid; it.Next()) Use(it.Key, it.Value);   // spans, no allocations
await foreach (var kv in db.ScanAsync(start, end)) { ... }

ColumnFamily users = await db.CreateColumnFamilyAsync("users", new ColumnFamilyOptions());
users.Put(k, v);
await foreach (WalBatch b in db.GetUpdatesSince(seq)) { ... }      // replication
```

## Working with parallel agents

Work is split into modules that can proceed in parallel (see `todo.md`). Rules for agents:

- Each agent owns specific folders (e.g. `src/RocksDb.Managed/Wal/` + `tests/.../Wal/`). Don't edit
  files owned by another stream; if you need a change in shared code (`Util/`, `Core/`, `Db/DbFormat.cs`),
  make the minimal additive change and mention it in your report.
- Package versions go only in `Directory.Packages.props`; add new packages there.
- Don't edit `todo.md` from a parallel agent — report progress to the coordinator, who updates it.
- Commit on your branch with clear messages; the coordinator merges.
