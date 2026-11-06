# Allocation-free usage guide

RocksDb.Managed is built so that the operations an application repeats (point reads, writes, batches, iteration,
transactions) allocate nothing on the GC heap once the database is warm. Some APIs return new objects, though, and
some usage patterns allocate on every call. This guide lists the APIs and patterns that stay allocation-free, the ones
that allocate (with the alternative to use), the settings that matter, and the paths that always allocate.

The numbers come from the allocation profiler in `tools/RocksDb.Managed.AllocationProfile` (see
[Measuring allocations](#measuring-allocations)). "B/op" counts the bytes allocated by every thread per operation in
steady state, including background flush and compaction work, with 16-byte keys and 100-byte values.

> **Planned additions.** More allocation-free variants are being added: iterator refresh / reuse, a pooled
> `MultiGet(IReadOnlyList<byte[]>)`, `TryGetIntProperty` without an intermediate string, span-based metadata and
> size APIs. This guide will be updated when they ship. Until then, the patterns below are what to use.

## Summary

| Area | Allocation-free (0 B/op) | Allocates | Instead |
|---|---|---|---|
| Point reads | `TryGet(key, Span<byte>, out len)`, `TryGet(key, IBufferWriter<byte>)`, `Get<TState,TResult>(key, state, callback)`, `GetPooled` + `Dispose`, `ContainsKey`, `KeyMayExist`, the async versions of each | `byte[]? Get` / `GetAsync` (the value array: 128 B for 100 B) | the span / writer / callback / pooled variants |
| Batched reads | `MultiGetBatch` reused with `Clear()`, `MultiGet` / `MultiGetAsync` / `MultiGetEntity` | `byte[]?[] MultiGet(IReadOnlyList<byte[]>)` (20 KB per 16 keys) | a reused `MultiGetBatch` |
| Writes | `Put` / `Delete` / `SingleDelete` / `Merge` / `PutEntity` with spans, and their async versions (0.0-0.3 B/op on the calling thread) | none on the caller; background work adds 1-3 B/op, see [Writes](#writes) | |
| Write batches | `db.CreateWriteBatch()` / `db.CreateWriteBatchWithIndex()` + `using`; one batch reused with `Clear()`; `WriteAsync` | `new WriteBatch()` that is never disposed (2.2 KB per batch); `new WriteBatchWithIndex()` per batch (1.1 KB) | `CreateWriteBatch` / `CreateWriteBatchWithIndex`, or `Clear()` |
| Iterators | a reused `Iterator`: `Seek` / `SeekForPrev` / `Next` / `Prev` / `Key` / `Value` / `Columns`, async moves, `foreach` over `Range(...)` | `NewIterator` (2-4 KB per iterator), `ScanAsync` (128 B per scan) | keep one iterator per thread and `Seek` it |
| Wide columns | `TryGetEntity(key, WideColumns)` with a reused result, `MultiGetEntity`, `Iterator.Columns` | `GetEntity` (312 B), `GetEntityLazy` (1 KB), `TryGetEntityLazy` (40 B) | `TryGetEntity`; reuse a `LazyWideColumns` |
| Transactions | `BeginTransaction(writeOptions, options, oldTransaction)` + `SetName` / `Put` / `TryGetForUpdate` / `TryGet` / `Prepare` / `Commit` (sync and async, pessimistic and optimistic) | `using BeginTransaction(writeOptions)` per transaction (the transaction object, 350-470 B) | reuse the transaction object |
| Monitoring | `Statistics` and `PerfContext` enabled (no allocation on reads), `GetColumnFamily(name)`, `GetLatestSequenceNumber` | `GetProperty` / `TryGetIntProperty` (40 B), `ColumnFamilies` (160 B), `GetLiveFilesMetadata`, `GetApproximateSizes`, `GetHistogramData` (896 B) | call them from monitoring code, not per request |

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

`byte[]?[] MultiGet(IReadOnlyList<byte[]>)` currently builds a new batch per call (about 20 KB for 16 keys), on top of
the result arrays. Use it only when the convenience matters more.

## Writes

`Put`, `Delete`, `SingleDelete`, `Merge`, `PutEntity` (span overloads) and their async versions allocate nothing on the
calling thread: the bytes go into the WAL buffer and the memtable arena, and arena slabs are recycled after each flush.

The background work that writes cause does allocate a little: about 1-3 B per 100-byte Put with a 4 MiB write buffer,
mostly per new file (the table reader and the index and filter blocks it keeps, info LOG lines, the version). This
scales with the number of flushes and compactions, not with the number of writes: a larger `WriteBufferSize` (the
default is 64 MiB) means fewer files and less of it. Flushes and compactions run on long-lived background threads
(named `rocksdb:high:N`, `rocksdb:low:N` and `rocksdb:bottom:N`, shared by all databases of the process), so their
per-thread buffers are reused from one job to the next. Steady state is reached after the first few memtables of a new database or column family. Before that, the
memtable arena and the block cache grow, which shows up as allocations but is the memory you configured.

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

An iterator holds a consistent view and a stack of per-level and per-file iterators. Creating one allocates that
structure: about 2 KB for `NewIterator`, plus about 1.2 KB for each table file it is first positioned in. Moving an
existing iterator allocates nothing.

```csharp
// Keep the iterator and position it again for every scan: Seek, Next, Prev, Key, Value are allocation-free.
using Iterator it = db.NewIterator(readOptions);
foreach (byte[] start in requests)
{
    for (it.Seek(start); it.Valid && Within(it.Key); it.Next())
        Use(it.Key, it.Value);                    // spans, valid until the iterator moves
}

// foreach over a key range uses a ref struct enumerator: no allocation either.
foreach ((ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) in it.Range(start, end)) Use(key, value);
```

- A reused iterator sees the database as of its creation (or its snapshot). To see newer writes, create a new one, or
  use a tailing iterator (`ReadOptions.Tailing = true`), which follows new writes without being recreated and stays
  allocation-free per `Seek` (the writes themselves aside).
- Bounds and prefix mode (`IterateUpperBound`, `PrefixSameAsStart`) are read when the iterator is created. For many short
  bounded scans, one long-lived iterator with a manual end check (as above) avoids creating an iterator and a
  `ReadOptions` per scan (together about 3.5 KB).
- Async: `SeekAsync` / `NextAsync` / `PrevAsync` on a reused iterator allocate nothing, also when a block has to be
  read. `ScanAsync(start, end)` (`await foreach`) allocates its enumerator (128 B) per scan; the items reuse one buffer
  (copy a `KeyValue` if you keep it).
- Wide-column entries: `it.Columns` reuses one `WideColumns` per iterator.
- `WriteBatchWithIndex.NewIteratorWithBase` and `Transaction.NewIterator` wrap a new database iterator, so they cost
  the same as `NewIterator`.

## Wide columns

```csharp
var columns = new WideColumns();                  // reuse it
if (db.TryGetEntity(key, columns) && columns.TryGetValue("city"u8, out ReadOnlySpan<byte> city)) Use(city);

var lazy = new LazyWideColumns();                 // reuse it too; Reset/Dispose releases what it pins
if (db.DefaultColumnFamily.TryGetEntityLazy(null, key, lazy)) Use(lazy);
```

`GetEntity(key)` returns a new `WideColumns` (312 B for 3 small columns) and `GetEntityLazy` a new `LazyWideColumns`
(about 1 KB). `TryGetEntityLazy` with a reused result currently allocates 40 B per call. `MultiGetEntity` with a
reused `MultiGetBatch` and `batch.TryGetEntity(i, columns)` allocate nothing.

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

## Options and settings that matter

- **Block cache.** Set `BlockBasedTableOptions.BlockCache` explicitly (`BlockCache.CreateLru` / `CreateHyperClock`) and
  share one cache across databases and column families when they belong together. Without one, every `Open` creates a
  default 32 MiB cache, which is several MB of allocations per open. A cache miss allocates a block buffer only while the
  cache is still filling up. Once it is full, an evicted block's buffer is reused.
- **Statistics and PerfContext** cost no allocations on the read and write paths when enabled (`DbOptions.Statistics`,
  `PerfContext.Level`). Reading them does: `Statistics.GetHistogramData` (896 B), `ToString()` dumps, and
  `PerfContext.ToString()`. `GetTickerCount` and `PerfContext.Current[metric]` don't allocate.
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

These return new objects and are meant for monitoring code, not per-request paths:

| API | Allocates |
|---|---|
| `GetProperty(name)` | the result string (40 B for a number) |
| `TryGetIntProperty(name, out value)` | 40 B today (an intermediate string) |
| `GetMapProperty`, `GetProperty(rocksdb.stats)` | 14-48 KB of strings |
| `ColumnFamilies` | a new list (160 B for 3 column families); `GetColumnFamily(name)` / `TryGetColumnFamily` allocate nothing |
| `GetLiveFilesMetadata()` | about 400 B per file |
| `GetApproximateSizes(ranges)` | about 450 B per call |
| `Statistics.GetHistogramData` | 896 B |

## What always allocates

These are one-time or per-file operations. Their allocations are reported for completeness and are not a concern for
steady-state throughput:

- `Open` / `Dispose` (version set, options, caches, LOG): MBs per open; recovery from a WAL adds its replay buffers.
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

`tools/RocksDb.Managed.AllocationProfile` runs about 120 scenarios that cover the public API. Each one runs in its own
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
  `property`, `background`, `lifecycle`, `native`, ...) or a prefix with a trailing `*`. `all` runs every managed
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
