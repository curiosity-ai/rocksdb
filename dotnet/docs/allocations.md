# Allocation-free usage guide

RocksDb.Managed is built so that the operations an application repeats (point reads, writes, batches, iteration,
transactions) allocate nothing on the GC heap once the database is warm. Some APIs return new objects, though, and
some usage patterns allocate on every call. This guide lists the APIs and patterns that stay allocation-free, the ones
that allocate (with the alternative to use), the settings that matter, and the paths that always allocate.

The numbers come from the allocation profiler in `tools/RocksDb.Managed.AllocationProfile` (see
[Measuring allocations](#measuring-allocations)). "B/op" counts the bytes allocated by every thread per operation in
steady state, including background flush and compaction work, with 16-byte keys and 100-byte values.

## Summary

| Area | Allocation-free (0 B/op) | Allocates | Instead |
|---|---|---|---|
| Point reads | `TryGet(key, Span<byte>, out len)`, `TryGet(key, IBufferWriter<byte>)`, `Get<TState,TResult>(key, state, callback)`, `GetPooled` + `Dispose`, `ContainsKey`, `KeyMayExist`, the async versions of each | `byte[]? Get` / `GetAsync` (the value array: 128 B for 100 B) | the span / writer / callback / pooled variants |
| Batched reads | `MultiGetBatch` reused with `Clear()`, `MultiGet` / `MultiGetAsync` / `MultiGetEntity`; `MultiGet(options, keys, values, valueRanges, statuses)` into caller memory and its async version | `byte[]?[] MultiGet(IReadOnlyList<byte[]>)` (the result arrays: 2.2 KB per 16 values of 100 B) | a reused `MultiGetBatch` or the caller-memory `MultiGet` |
| Writes | `Put` / `Delete` / `SingleDelete` / `Merge` / `PutEntity` with spans, and their async versions (0.0-0.3 B/op on the calling thread) | none on the caller; background work adds 1-3 B/op, see [Writes](#writes) | |
| Write batches | `db.CreateWriteBatch()` / `db.CreateWriteBatchWithIndex()` + `using`; one batch reused with `Clear()`; `WriteAsync` | `new WriteBatch()` that is never disposed (2.2 KB per batch); `new WriteBatchWithIndex()` per batch (1.1 KB) | `CreateWriteBatch` / `CreateWriteBatchWithIndex`, or `Clear()` |
| Iterators | a reused `Iterator`: `Seek` / `SeekForPrev` / `Next` / `Prev` / `Key` / `Value` / `Columns`, async moves, `foreach` over `Range(...)`, `ScanAsync`, `Refresh`, `SetBounds` | `NewIterator` / `NewIteratorAsync` + `Dispose` (56 B: the iterator handle), a `new ReadOptions` per scan (184 B) | reuse one iterator with `SetBounds` / `Refresh`, or one `ReadOptions` |
| Wide columns | `TryGetEntity(key, WideColumns)` and `TryGetEntityLazy(options, key, LazyWideColumns)` with a reused result, `MultiGetEntity`, `Iterator.Columns` | `GetEntity` (312 B), `GetEntityLazy` (1 KB) | `TryGetEntity` / `TryGetEntityLazy` with a reused result |
| Transactions | `BeginTransaction(writeOptions, options, oldTransaction)` + `SetName` / `Put` / `TryGetForUpdate` / `TryGet` / `Prepare` / `Commit` (sync and async, pessimistic and optimistic) | `using BeginTransaction(writeOptions)` per transaction (the transaction object, 350-470 B) | reuse the transaction object |
| WAL tailing and replication | a held `GetUpdatesSince` iterator (`Next`, `MoveNextTailingAsync`), `TailUpdatesAsync`, `WaitForSequenceAsync`, `WriteBatchReader` / `WriteBatch.Iterate` over a `WalBatch`, `WalTailer`, `ReplicationConsumer`, `ProgressReporter`, `RaftClusterNode.WaitForCommitAsync`, reads on secondary and read-only instances | `GetUpdatesSince` per poll (about 3 KB: WAL listing and file open), an idle `TryCatchUpWithPrimary` (about 1.4 KB: WAL directory listing, opening CURRENT) | keep one iterator or tail; see [Replication and WAL tailing](#replication-and-wal-tailing) |
| Monitoring | `Statistics` and `PerfContext` enabled (no allocation on reads), `GetTickerCount`, `GetHistogramData`, `TryGetIntProperty` / `TryGetAggregatedIntProperty`, `ColumnFamilies`, `GetColumnFamily(name)`, `GetLatestSequenceNumber`, `VisitLiveFiles`, `GetApproximateSizes(ranges, Span<ulong>)` | `GetProperty` (the result string), `GetLiveFilesMetadata` (about 400 B per file), `GetApproximateSizes` returning an array | the non-allocating variants; see [Monitoring and metadata APIs](#monitoring-and-metadata-apis) |

## Point reads

Every point read has an allocation-free form. They differ in where the value goes:

```csharp
// 1. Into a caller buffer (stack or reused array). valueLength > buffer.Length means the copy was truncated.
Span<byte> buffer = stackalloc byte[256];
if (db.TryGet(key, buffer, out int valueLength) && valueLength <= buffer.Length)
    Use(buffer[..valueLength]);

// 2. Appended to a reused IBufferWriter<byte> (ArrayBufferWriter, a pipe, PooledBufferWriter...).
writer.ResetWrittenCount();
if (db.TryGet(key, writer)) Use(writer.WrittenSpan);

// 3. Zero copy: the span points into the memtable or the cached block and is valid during the callback only.
//    Use a static lambda and pass state explicitly, so no closure is allocated.
long counter = db.Get(key, 0, static (value, _) => BinaryPrimitives.ReadInt64LittleEndian(value));

// 4. A pooled copy you own until Dispose.
using (PooledValue? v = db.GetPooled(key))
    if (v is not null) Use(v.Span);
```

- `byte[]? Get(key)` and `GetAsync(key)` allocate the result array (a 100-byte value is a 128-byte array). They are
  fine for configuration reads, not for request paths. The same goes for rocksdb-sharp's `byte[] Get`.
- `ContainsKey`, `KeyMayExist` (bloom filter / memtable only, ~130 ns) and misses allocate nothing.
- Reads from the memtable, from cached blocks, from blocks that miss the block cache (once the cache is full), from blob
  files, of merge operands, with user-defined timestamps and under a snapshot are all 0 B/op.
- **Reuse `ReadOptions`.** A `ReadOptions` object is about 180 bytes. Create it once (per snapshot, per thread, or as a
  static for fixed settings) and pass the same instance every time. `new ReadOptions { ... }` per call is the
  allocation, not the read.
- The async versions (`TryGetAsync`, `GetPooledAsync`, `MultiGetAsync`) complete synchronously on memtable and cache hits.
  When they do wait for a block read, they use pooled state machines: 0 B/op in the profile with blocks read from the
  page cache. Await each `ValueTask` once. Don't call `AsTask()` or store the task: that allocates a `Task`.

### Batched reads

Keep a `MultiGetBatch` and clear it between batches. It owns the keys, the results and all scratch space:

```csharp
var batch = new MultiGetBatch(capacity: 64);    // one per thread / request handler
batch.Clear();
foreach (byte[] k in keys) batch.Add(k);
db.MultiGet(batch);                             // or await db.MultiGetAsync(batch)
for (int i = 0; i < batch.Count; i++)
    if (batch.TryGetValue(i, out ReadOnlySpan<byte> v)) Use(v);
```

When the keys come from your own memory and the values should land in your own buffer, use the caller-memory
`MultiGet`. It runs a pooled batch internally, appends the found values back to back to an `IBufferWriter<byte>`, and
gives each key's position in `valueRanges` (relative to the first byte the call writes) and its outcome in `statuses`.
It does not allocate after warm-up (apart from the writer growing):

```csharp
ReadOnlyMemory<byte>[] keys = ...;                  // reused
var values = new ArrayBufferWriter<byte>(4096);     // reused
var ranges = new Range[keys.Length];
var statuses = new MultiGetStatus[keys.Length];

values.ResetWrittenCount();
db.MultiGet(null, keys, values, ranges, statuses);  // or await db.MultiGetAsync(null, keys, values, ranges, statuses)
for (int i = 0; i < keys.Length; i++)
    if (statuses[i] == MultiGetStatus.Found) Use(values.WrittenSpan[ranges[i]]);
```

A key that fails (I/O error, or `ReadTier.BlockCacheTier` with an uncached block) gets `MultiGetStatus.Error` without
stopping the others. Use a `MultiGetBatch` when you need the exception of each failed key.

`byte[]?[] MultiGet(IReadOnlyList<byte[]>)` also uses a pooled batch. It allocates only its results: the result array
and one array per found value (2.2 KB for 16 values of 100 B). It throws the first per-key failure.

## Writes

`Put`, `Delete`, `SingleDelete`, `Merge`, `PutEntity` (span overloads) and their async versions allocate nothing on the
calling thread: the bytes go into the WAL buffer and the memtable arena, and arena slabs are recycled after each flush.

The background work that writes cause does allocate a little: about 1-3 B per 100-byte Put with a 4 MiB write buffer,
mostly per new file (the table reader and the index and filter blocks it keeps, info LOG lines, the version). This
scales with the number of flushes and compactions, not with the number of writes: a larger `WriteBufferSize` (the
default is 64 MiB) means fewer files and less of it. Flushes and compactions run on long-lived background threads
(named `rocksdb:high:N`, `rocksdb:low:N` and `rocksdb:bottom:N`, shared by all databases of the process), so their
per-thread buffers are reused from one job to the next.

Steady state is reached after the first few memtables of a new database or column family. Before that, the memtable
arena and the block cache grow, which shows up as allocations but is the memory you configured.

Write options: reuse `WriteOptions` instances (`WriteOptions.SyncWrites` is a shared one). Synced writes
(`WriteOptions.SyncWrites`), `DisableWal`, writes to several column families and timestamped writes behave the same.

### Write batches

```csharp
// Pooled: Dispose returns the batch (and its buffer) to a process-wide pool.
using (WriteBatch batch = db.CreateWriteBatch())
{
    batch.Put(k1, v1).Delete(k2).Merge(cf, k3, v3);
    db.Write(batch);                                // or await db.WriteAsync(batch)
}

// Or keep one batch per writer and clear it.
batch.Clear();
batch.Put(k1, v1);
db.Write(batch);
```

Both patterns cost the same as single writes (about 3 B per Put, all of it background). `new WriteBatch()` is fine if
you dispose it. A batch that is dropped without `Dispose` takes its pooled buffer with it, which costs about 2.2 KB per
batch.

`WriteBatchWithIndex` works the same way: take a pooled one per batch, or keep one and call `Clear()` (both about 3 B
per Put, all of it background). `new WriteBatchWithIndex()` per batch builds its index from scratch (1.1 KB).
`TryGetFromBatchAndDB(db, key, writer)` allocates nothing.

```csharp
// Pooled (bytewise order; pass overwriteKey: true for overwrite_key mode). Dispose returns it to the pool.
using (WriteBatchWithIndex wbwi = db.CreateWriteBatchWithIndex())     // or WriteBatchWithIndex.Rent()
{
    wbwi.Put(k1, v1);
    if (wbwi.TryGetFromBatchAndDB(db, k2, writer)) Use(writer.WrittenSpan);
    db.Write(wbwi);
}
```

A pooled batch must not be used after `Dispose`, nor the `WriteBatch` or iterators taken from it: a call on it while
it sits in the pool throws `ObjectDisposedException`, but once another caller has rented it again, a stale reference
would see that caller's batch.

### Range deletions

`DeleteRange` allocates nothing on the calling thread. In the background, range tombstones are fragmented when a
memtable or table file is first read and rewritten by every flush and compaction that covers them; this costs the
fragmented copy of each tombstone (about 50 B per fragment, per memtable, table file and compaction) rather than per
read. A workload that issues one `DeleteRange` per 10 keys measures about 300 B per `DeleteRange` in total, and a batch
that mixes puts with one `DeleteRange` about 300 B per batch. Many narrow ranges still cost more than a few wide ones in
memory, read time and compaction work (as with upstream RocksDB), so prefer wide ranges when you have the choice.
Snapshots matter too: a flush or compaction keeps one tombstone per fragment for each snapshot that separates them.

## Iterators

Moving an iterator (`Seek`, `Next`, `Prev`, `Key`, `Value`, the async moves, `foreach` over `Range(...)`,
`await foreach` over `ScanAsync(...)`) allocates nothing. Creating one is cheap too: a disposed iterator returns its
internal iterator stack to a small pool of its column family, and the next `NewIterator` reuses it, so the per-request
pattern only allocates the 56-byte `Iterator` object.

```csharp
// Per request: fine. Only the Iterator handle (56 B) is allocated once the pool is warm.
using (Iterator it = db.NewIterator(readOptions))      // reuse readOptions too (184 B per new ReadOptions)
{
    for (it.Seek(start); it.Valid && Within(it.Key); it.Next())
        Use(it.Key, it.Value);                          // spans, valid until the iterator moves
}

// Many short scans on one thread: keep one iterator, change its range and refresh its view. Nothing is allocated.
using Iterator scanner = db.NewIterator();
foreach ((byte[] lower, byte[] upper) in ranges)
{
    scanner.Refresh();                                  // see writes made since the last scan (optional)
    scanner.SetBounds(lower, upper);                    // like IterateLowerBound / IterateUpperBound
    for (scanner.SeekToFirst(); scanner.Valid; scanner.Next()) Use(scanner.Key, scanner.Value);
}

// foreach over a key range uses a ref struct enumerator: no allocation either.
foreach ((ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) in it.Range(start, end)) Use(key, value);
```

- An iterator sees the database as of its creation (or its snapshot). `Refresh()` moves it to the latest state and
  `Refresh(snapshot)` to a snapshot, reusing everything it holds; `RefreshAsync` opens new table files with async I/O.
  Tailing iterators (`ReadOptions.Tailing = true`) follow new writes on their own; they, iterators of WritePrepared /
  WriteUnprepared transaction databases, and `SstFileReader` iterators cannot be refreshed (`NotSupported`).
- `SetBounds(lower, upper)` replaces the iterate bounds of an existing iterator without touching your `ReadOptions`;
  the bounds stay in effect across `Refresh`. On a column family with a prefix extractor it copies the read options
  once per call (184 B).
- The pooled stack does not keep files or memtables alive: `Dispose` still releases everything the iterator pinned,
  and using a disposed iterator throws `ObjectDisposedException`.
- Async: `NewIteratorAsync`, `SeekAsync` / `NextAsync` / `PrevAsync` allocate nothing beyond the handle, also when a
  block has to be read. `ScanAsync(start, end)` reuses one enumerator per iterator, and its items reuse one buffer
  (copy a `KeyValue` if you keep it). Start the next `ScanAsync` of an iterator after the previous enumeration was
  disposed (`await foreach` does that).
- Wide-column entries: `it.Columns` reuses one `WideColumns` per iterator.
- `WriteBatchWithIndex.NewIteratorWithBase(db.NewIterator())` allocates about 130 B per call and
  `Transaction.NewIterator` about 150 B (the iterator objects; the merge buffers are reused).
- A `Snapshot` per scan adds its handle (72 B).

## Wide columns

```csharp
var columns = new WideColumns();                  // reuse it
if (db.TryGetEntity(key, columns) && columns.TryGetValue("city"u8, out ReadOnlySpan<byte> city)) Use(city);

var lazy = new LazyWideColumns();                 // reuse it too; Reset/Dispose releases what it pins
if (db.DefaultColumnFamily.TryGetEntityLazy(null, key, lazy)) Use(lazy);
```

`GetEntity(key)` returns a new `WideColumns` (312 B for 3 small columns) and `GetEntityLazy` a new `LazyWideColumns`
(about 1 KB). `TryGetEntityLazy` with a reused result, `MultiGetEntityLazy` with a reused `LazyWideColumnsBatch`,
`MultiGetEntity` with a reused `MultiGetBatch` and `batch.TryGetEntity(i, columns)` allocate nothing. A lazy result
keeps the version it was read from alive until the next read into it, `Reset()` or `Dispose()`. Reset a result you keep
for later reuse, so it does not hold on to old files.

## Transactions

Reuse the transaction object: `BeginTransaction(writeOptions, transactionOptions, oldTransaction)` reinitialises a
committed or rolled-back transaction, and keeps its write batch and lock-tracking buffers.

```csharp
var txnOptions = new TransactionOptions();
Transaction txn = txnDb.BeginTransaction(writeOptions, txnOptions);
...
txn = txnDb.BeginTransaction(writeOptions, txnOptions, txn);   // next transaction, same object
txn.Put(key, value);
txn.TryGetForUpdate(null, other, writer);
txn.Commit();                                   // or await txn.CommitAsync()
```

- With reuse, pessimistic and optimistic transactions (`Put`, `TryGetForUpdate`, `TryGet`, `Commit`, `CommitAsync`)
  allocate nothing, also for two-phase commit with a different `SetName` per transaction.
- `using Transaction txn = txnDb.BeginTransaction(writeOptions)` (a new object per transaction) is fine too: disposing a
  transaction hands its write batch and lock tracker to the next one, so only the transaction object is new (about
  470 B pessimistic, 350 B optimistic). Dispose every transaction; one that is dropped without `Dispose` keeps its
  buffers, and the next one builds new ones (about 3 KB).

## Async usage

- All I/O-bound APIs return `ValueTask` and complete synchronously when nothing has to wait (memtable and cache hits,
  buffered WAL writes). When they do wait, they use pooled state machines.
- Await each `ValueTask` exactly once and don't convert it with `AsTask()`. Wrap a `ValueTask` in `Task.WhenAll` only on
  cold paths.
- Write your own hot async loops as `async ValueTask` methods with
  `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]`, so your own state machine is pooled as well.
- Concurrent async writers (many tasks awaiting `PutAsync` / `WriteAsync` in the same write groups) allocate nothing
  either: each waiting write owns its suspended state, so no tuning is needed.

## Replication and WAL tailing

A replica or WAL consumer that keeps up with a write stream allocates nothing per replicated batch in our code:
waiting for the next write, reading it from the WAL, applying it and reporting progress all reuse pooled state.

- **Keep one tail open.** Hold a `TransactionLogIterator` (or `TailUpdatesAsync` / `WalTailer.ReadChunksAsync`) for the
  life of the stream. It follows WAL rotations itself and waits for new writes without polling. Opening
  `GetUpdatesSince` per poll lists the WAL files and opens one each time (about 3 KB per call).

  ```csharp
  using TransactionLogIterator it = db.GetUpdatesSince(nextSeq);
  while (await it.MoveNextTailingAsync(ct))           // waits for the next write; no allocation per batch
  {
      WalBatch b = it.Current;                         // a view, valid until the iterator moves
      foreach (WriteBatchEntry e in b.GetReader()) Apply(e.Key, e.Value);
  }
  ```

- `WaitForSequenceAsync`, `MoveNextTailingAsync`, `TailUpdatesAsync`, `WalTailer.WaitForUpdatesAsync` and
  `RaftClusterNode.WaitForCommitAsync` allocate nothing while they wait, also with a cancellable token; a cancelled
  wait throws `OperationCanceledException`.
- **Apply without copies.** `ReplicationConsumer.IngestChunk` / `IngestChunkAsync` / `TryIngestChunkAsync` copy the
  batch into a pooled `WriteBatch`; `WalBatch.ToWriteBatch()` does the same for your own `Write` (dispose the batch).
  `WriteBatchReader` and `WriteBatch.Iterate(rep, ref handler)` with a struct handler read a batch without allocating.
- **Progress reports.** `ProgressReporter.Report` and `ReplicationPrimary.ReportProgress` /
  `ReplicationSource.RetireArchivedWals` allocate nothing while no archived WAL can be retired. They list the WAL files
  only after a WAL rotation or once the slowest replica has applied a whole archived WAL. `RocksDatabase.CurrentWalNumber`
  tells you cheaply whether the WAL rotated, if you write your own retention logic.
- **Hosts.** `PairNodeHost.WriteAsync`, `ClusterNodeHost.TryWriteAsLeaderAsync` and `WriteCommittedAsync` allocate
  nothing in steady state. `WriteCommittedAsync` pools its timeout source when its `CancellationToken` cannot be
  cancelled; with a cancellable token it creates a linked token source per write. The Raft heartbeat loop allocates a
  little per heartbeat (a payload and a timer per interval, not per write); an idle follower answers heartbeats with a
  cached result. A real transport adds its own serialization costs.
- **Secondary and read-only instances.** Reads (`TryGet`, `GetPooledAsync`, iterators) cost the same as on a primary.
  `TryCatchUpWithPrimary` lists the WAL directory and reads CURRENT on every call, like upstream (about 1.4 KB for an
  idle poll); applying new WAL data or MANIFEST edits adds a new super version per column family. Poll at the rate you
  need, not in a tight loop.

## Options and settings that matter

- **Block cache.** Set `BlockBasedTableOptions.BlockCache` explicitly (`BlockCache.CreateLru` / `CreateHyperClock`) and
  share one cache across databases and column families when they belong together. Without one, every `Open` creates a
  default 32 MiB cache for that database. Its table entries are created as the cache fills, so opening a database
  costs little for the cache itself. A cache miss allocates a block buffer only while the cache is still filling up.
  Once it is full, an evicted block's buffer is reused.
- **Statistics and PerfContext** cost no allocations on the read and write paths when enabled (`DbOptions.Statistics`,
  `PerfContext.Level`). Reading them doesn't either: `GetTickerCount`, `GetHistogramData` and
  `PerfContext.Current[metric]` allocate nothing. Only the text dumps (`ToString()`, `PerfContext.ToString()`) do.
- **Info LOG.** Flushes, compactions and file deletions write INFO lines and `EVENT_LOG_v1` records to the `LOG` file,
  one string per line. With `DbOptions.InfoLogLevel = InfoLogLevel.Warn` (or higher) they are not formatted at all,
  which removes about a fifth of the background bytes of a flush or compaction:

  ```csharp
  var options = new DbOptions { CreateIfMissing = true, InfoLogLevel = InfoLogLevel.Warn };
  ```

  A custom `DbOptions.InfoLog` receives each line as a `string`; its `Level` is checked before anything is formatted.
- **Write buffer size.** A larger memtable means fewer flushes and so fewer per-file allocations (see [Writes](#writes)).
- **Snapshots.** `GetSnapshot()` allocates the snapshot handle (72 B). Take one per logical read view, not per read, and
  pass it through a reused `ReadOptions`.

## Monitoring and metadata APIs

| API | Allocates |
|---|---|
| `TryGetIntProperty(name, out value)`, `GetIntProperty`, `TryGetAggregatedIntProperty` | nothing for the numeric properties (the value is computed as a number) |
| `GetProperty(name)` | the result string (40 B for a number) |
| `GetMapProperty`, `GetProperty(rocksdb.stats)` | 14-48 KB of strings |
| `ColumnFamilies` | nothing: a read-only snapshot, the same object until a column family is created or dropped |
| `GetColumnFamily(name)` / `TryGetColumnFamily` | nothing |
| `VisitLiveFiles(state, visitor)` | nothing with a static lambda |
| `GetLiveFilesMetadata(list)` | one record per file (about 400 B); the list is reused |
| `GetLiveFilesMetadata()` | a new list plus one record per file |
| `GetApproximateSizes(ranges, sizes)` (`Span<ulong>` output) | nothing |
| `GetApproximateSizes(ranges)` | the result array |
| `Statistics.GetHistogramData`, `GetTickerCount` | nothing |

Prefer `TryGetIntProperty` to `GetProperty` for numbers. To walk the table files, use `VisitLiveFiles` with a static
lambda and pass what it needs as the state. The `LiveFileInfo` it receives exposes the file's fields and keys as spans.
These spans are valid only during the call:

```csharp
var total = new StrongBox<ulong>();                // reused
total.Value = 0;
db.VisitLiveFiles(total, static (in LiveFileInfo f, StrongBox<ulong> sum) => sum.Value += f.Size);

ulong[] sizes = new ulong[ranges.Length];          // reused
db.GetApproximateSizes(ranges, sizes);             // or (SizeApproximationOptions, ranges, sizes)

if (db.TryGetIntProperty(DbProperties.EstimateNumKeys, out ulong keys)) Report(keys);
```

## What always allocates

These are one-time or per-file operations. Their allocations are reported for completeness and are not a concern for
steady-state throughput:

- `Open` / `Dispose` (version set, options, memtable, LOG): about 0.7 MB for an empty database; recovery from a WAL adds
  its replay buffers.
- `CreateColumnFamily` / `DropColumnFamily`: the new memtable's first arena slab and the OPTIONS file (about 0.8 MB).
- Flush and compaction: about 20-40 KB per output file (the new table's reader with its pinned index and filter blocks,
  its properties, the new version, file names and LOG lines) plus the per-entry building, which reuses pooled buffers.
  This includes dictionary compression (`CompressionOptions.MaxDictBytes`): the data blocks it buffers while sampling
  go into pooled memory.
- `SstFileWriter` + `IngestExternalFiles`: about 70 KB per ingested file.
- `CreateCheckpoint` (about 27 KB) and `BackupEngine.CreateNewBackup` (MBs: file lists and the table properties of every
  file).
- Text dumps: `GetProperty(rocksdb.stats)`, `Statistics.ToString()`, `PerfContext.ToString()`.

## Measuring allocations

`tools/RocksDb.Managed.AllocationProfile` runs about 150 scenarios that cover the public API. Each one runs in its own
child process: setup, warm-up, then a measured phase that counts the exact bytes (`GC.GetTotalAllocatedBytes(precise:
true)` and the calling thread's counter). After that the parent traces the child with
[Memory.Introspect](https://www.nuget.org/packages/Memory.Introspect/) (GC allocation ticks, about one per 100 KB) to
show which types were allocated and from which call stacks.

```bash
cd dotnet
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- list            # scenarios
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- run get iter.seek-next10.sst-cached
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- run all --native --out /tmp/alloc
dotnet run -c Release --project tools/RocksDb.Managed.AllocationProfile -f net10.0 -- run write batch --warmup 8 --measure 4
```

- Filters select a scenario name, a group (`write`, `batch`, `get`, `multiget`, `entity`, `iterator`, `txn`,
  `property`, `background`, `lifecycle`, `wal-tail`, `secondary`, `replication`, `raft`, `native`, ...) or a prefix with a trailing `*`. `all` runs every managed
  scenario, and `--native` adds the rocksdb-sharp comparisons.
- `--warmup S` / `--measure S` / `--trace S` set the phase lengths in seconds. Use a longer warm-up for write and
  transaction scenarios so memtable arenas and pools reach steady state. `--trace 0` skips tracing (exact numbers
  only, much faster).
- `--out DIR` gets `allocations.md` (a table plus per-scenario types and stacks) and `allocations.json`.
  `--work DIR` holds the databases and traces, which are deleted afterwards unless you pass `--keep-traces`.
- A scenario at 0 B/op still shows about 0.5 MB in 5-6 sampled ticks: that is the trace session starting up in the
  child, and the report excludes those stacks.
- To add a scenario, add an `S(name, group, path, op, setup)` entry in `tools/RocksDb.Managed.AllocationProfile/Scenarios/*.cs`.
  The setup builds the state and returns `Runner.Sync(n => ...)` or `Runner.Async(...)`, which performs exactly `n`
  operations.
