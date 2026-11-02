# Architecture

RocksDb.Managed is a port, not a redesign. Each managed file names the C++ file(s) it ports in its summary comment,
and keeps the algorithm structure recognizable so upstream fixes can be followed. This page maps the C++ tree to the
C# tree, describes how the port runs work on threads and tasks and how it manages memory, and lists where it
deliberately differs from C++. [CLAUDE.md](../CLAUDE.md) has the porting conventions; [validation.md](validation.md)
has the file-by-file audit.

## Projects

```
dotnet/
  src/RocksDb.Managed/                         the engine (namespace RocksDb.Managed and sub-namespaces)
  src/RocksDb.Managed.Replication/             checkpoint + WAL streaming, switchover, Raft cluster (transport-agnostic)
  src/RocksDb.Managed.Replication.MagicOnion/  optional gRPC transport for the replication layer
  tests/RocksDb.Managed.Tests/                 unit tests (pure managed)
  tests/RocksDb.Managed.CompatTests/           in-process compatibility tests against librocksdb
  tests/RocksDb.Managed.RoundTrip/             whole-DB managed and native round trips (native in a child process)
  tests/RocksDb.Managed.Replication.Tests/     replication, switchover, cluster and gRPC tests
  samples/QuickStart/                          the README examples, runnable
  samples/{ReplicationTest,SwitchoverTest,ClusterTest}/  multi-process gRPC programs
  bench/RocksDb.Managed.Benchmarks/            db_bench-style runner + BenchmarkDotNet micro-benchmarks
  bench/native-c/                              the C-API benchmark used as the native baseline
```

## Module map (C++ to C#)

Paths on the left are relative to the repository root (C++). Paths on the right are relative to
`dotnet/src/RocksDb.Managed/`.

### Public API

| C++ | C# | Notes |
|---|---|---|
| `include/rocksdb/db.h`, `db/db_impl/db_impl*.cc` (public surface) | `RocksDatabase*.cs`, `ColumnFamily*.cs` | `RocksDatabase` is the DB, and the default column family's API; `ColumnFamily` is a handle with the same API. Partial files: replication and WAL, timestamps, entities, options, properties |
| `include/rocksdb/iterator.h` | `Db/Iterators/Iterator.cs` | spans, `foreach` range enumerator, `ScanAsync` |
| `DB::MultiGet` | `MultiGetBatch.cs` | reusable batch with per-key status |
| `PinnableSlice` | `PooledValue.cs`, `ReadOnlySpanFunc` callbacks | see "Memory" below |
| `include/rocksdb/status.h` | `Core/Status.cs` | `RocksDbException`, `StatusCode`, `StatusSubCode`, `ErrorSeverity` |
| `DB::Properties` | `Core/DbProperties.cs` | property name constants |
| `include/rocksdb/options.h`, `advanced_options.h`, `table.h` | `Options/DbOptions.cs`, `ColumnFamilyOptions.cs`, `BlockBasedTableOptions.cs`, `OperationOptions.cs` (Read/Write/Flush/CompactRange options), `CompressionOptions.cs`, `CompactionStyleOptions.cs`, `IngestExternalFileOptions.cs`, `OptionEnums.cs` | typed options with upstream defaults |
| `options/options_parser.cc`, `options/*_type_info` | `Options/OptionsFile.cs`, `OptionTypeInfo.cs`, `OptionsBase.cs`, `CustomizableOption.cs` | OPTIONS file read/write; the enum and string mapping is byte-identical |
| `include/rocksdb/slice_transform.h` | `Options/SliceTransform.cs` | fixed, capped and noop prefix extractors |

### Database core (`db/`)

| C++ | C# |
|---|---|
| `db/db_impl/db_impl.{h,cc}` | `Db/DbImpl.cs` (state, mutex, super versions, snapshots) |
| `db_impl_open.cc` (Open, Recover, WAL replay) | `Db/DbImpl.Open.cs` |
| `db_impl_write.cc`, `db/write_thread.cc` | `Db/DbImpl.Write.cs`, `Db/WriteThread.cs` |
| `db_impl.cc` Get, `version_set.cc` Version::Get, `db/get_context.cc` | `Db/DbImpl.Read.cs`, `Db/GetContext.cs` |
| `DBImpl::MultiGetImpl`, `Version::MultiGet` | `Db/DbImpl.MultiGet.cs` |
| `db_impl_compaction_flush.cc`, `db/flush_job.cc`, `db/builder.cc` | `Db/DbImpl.Flush.cs`, `Db/FlushJob.cs` |
| `db_impl_compaction_flush.cc` (compaction scheduling, CompactRange) | `Db/DbImpl.Compaction.cs` |
| `db/compaction/*` (picker level/universal/fifo, compaction, compaction_job, compaction_iterator), `db/write_controller.cc` | `Db/Compaction/*` |
| `db/column_family.cc` | `Db/ColumnFamilyData.cs`, `Db/DbImpl.ColumnFamilies.cs` |
| `db/version_set.cc`, `version_builder.cc`, `version_edit_handler.cc` | `Db/VersionSet.cs`, `Db/Version.cs`, `Db/Manifest.cs`, `Db/ManifestLog.cs` |
| `db/version_edit.{h,cc}`, `wal_edit.cc` | `Db/VersionEdit.cs`, `Db/FileMetaData.cs` |
| `db/dbformat.{h,cc}` | `Db/DbFormat.cs`, `Db/InternalKey.cs`, `Db/InternalKeyComparator.cs` |
| `db/write_batch.cc`, `write_batch_internal.h` | `Db/WriteBatch.cs`, `Db/WriteBatchReader.cs`, `Db/IWriteBatchHandler.cs` |
| `db/memtable.cc`, `memtable/inlineskiplist.h`, `memory/arena.cc`, `concurrent_arena.cc` | `Db/MemTable.cs`, `Db/MemTableRangeTombstones.cs`, `Memtable/InlineSkipList.cs`, `Memory/Arena.cs`, `Memory/ConcurrentArena.cs` |
| `db/merge_helper.cc`, `merge_operator.h`, `utilities/merge_operators/*` | `Db/MergeHelper.cs`, `Db/MergeOperator.cs`, `Db/MergeHelper.BuiltinOperators.cs` |
| `db/range_del_aggregator.cc`, `range_tombstone_fragmenter.cc` | `Db/RangeDelAggregator.cs` |
| `db/db_iter.cc`, `table/merging_iterator.cc`, `LevelIterator`, `memtable` iterator | `Db/Iterators/*` |
| `db/table_cache.cc` | `Db/TableCache.cs`, `Db/ITableCache.cs` |
| `db/external_sst_file_ingestion_job.cc`, `table/sst_file_writer.cc` | `Db/ExternalSstFileIngestionJob.cs`, `Db/DbImpl.Ingest.cs`, `Db/SstFileWriter.cs` |
| `db/transaction_log_impl.cc`, `wal_manager.cc` | `Db/TransactionLogIterator.cs`, `Db/DbImpl.Replication.cs` |
| `utilities/checkpoint/checkpoint_impl.cc` | `Db/DbImpl.Checkpoint.cs` |
| `db_impl_readonly.cc`, `db_impl_secondary.cc` | `Db/DbImpl.ReadOnly.cs` |
| `db/error_handler.cc`, `db/event_helpers.cc`, `include/rocksdb/listener.h` | `Db/ErrorHandler.cs`, `Db/EventListener.cs`, `Db/EventListener.Notifications.cs` |
| `db/blob/*` | `Db/Blob/*` |
| `db/wide/*` | `Db/Wide/*`, `ColumnFamily.Entities.cs` |
| `DBImpl::SetOptions`, `SetDBOptions` | `Db/DbImpl.Options.cs`, `ColumnFamily.Options.cs` |
| `db/internal_stats.cc`, `monitoring/statistics.cc`, `histogram.cc` | `Db/DbImpl.Properties.cs`, `Db/DbImpl.Monitoring.cs`, `Monitoring/*` |
| `db/compaction/compaction_filter.h` | `Db/CompactionFilter.cs` |
| `utilities/ttl/db_ttl_impl.cc` | `Db/DbWithTtl.cs` |
| `file/filename.cc` | `Db/FileNames.cs` |
| `test_util/sync_point.h` | `Db/TestHooks.cs` |

### Formats and storage

| C++ | C# |
|---|---|
| `db/log_format.h`, `log_writer.cc`, `log_reader.cc` | `Wal/LogFormat.cs`, `Wal/LogWriter.cs`, `Wal/LogReader.cs`, `Wal/FragmentBufferedLogReader.cs`, `Wal/WalMetadataRecords.cs`, `Wal/WalStreamingCompression.cs` |
| `table/block_based/block_builder.cc`, `block.cc`, `data_block_hash_index.cc` | `Table/BlockBuilder.cs`, `Table/Block.cs`, `Table/BlockIter.cs`, `Table/DataBlockHashIndex.cs` |
| `table/format.cc` (Footer, BlockHandle, checksums), `block_fetcher.cc` | `Table/Footer.cs`, `Table/BlockHandle.cs`, `Table/BlockChecksum.cs`, `Table/ChecksumType.cs`, `Table/BlockFetcher.cs` |
| `block_based_table_builder.cc`, `index_builder.cc`, `filter_block*.cc` | `Table/BlockBasedTableBuilder.cs`, `Table/IndexBuilder.cs`, `Table/FilterBlockBuilder.cs` |
| `block_based_table_reader.cc`, `_sync_and_async.h`, `block_based_table_iterator.cc`, partitioned index/filter readers | `Table/BlockBasedTableReader*.cs`, `Table/BlockBasedTableIterator.cs`, `Table/PinnedFilterPartitions.cs` |
| `table/table_properties.cc`, `meta_blocks.cc` | `Table/TableProperties.cs` |
| `table/block_based/filter_policy.cc`, `util/bloom_impl.h`, `util/ribbon_*.h` | `Table/Filters/*` |
| `util/compression.{h,cc}` | `Compression/*` (Snappier, K4os LZ4, ZstdSharp and `System.IO.Compression` underneath) |
| `util/coding.h`, `crc32c.cc`, `hash.cc`, `xxph3.h` | `Util/Coding.cs`, `Util/Crc32C.cs`, `Util/Hash.cs`, `Util/HashXxph3.cs` |
| `include/rocksdb/comparator.h`, `util/comparator.cc` | `Util/Comparator.cs`, `Util/Comparators.cs` |
| `cache/lru_cache.cc`, `clock_cache.cc`, `cache_key.cc` | `Cache/LruCache.cs`, `Cache/HyperClockCache.cs`, `Cache/CacheKey.cs`, `Cache/BlockCache.cs` |
| `env/fs_posix.cc`, `env/mock_env.cc`, `file/file_prefetch_buffer.cc`, `util/aligned_buffer.h` | `Env/LocalFileSystem.cs`, `Env/UnixNative.cs`, `Env/MemoryFileSystem.cs`, `Env/FilePrefetchBuffer.cs`, `Env/AlignedBuffer.cs`, `Env/FileContracts.cs` |
| `logging/auto_roll_logger.cc`, `env.h` Logger | `Logging/FileInfoLogger.cs`, `Logging/InfoLogger.cs` |

### Utilities (`utilities/`)

| C++ | C# |
|---|---|
| `utilities/transactions/optimistic_transaction*.cc` | `Utilities/Transactions/OptimisticTransactionDatabase.cs` |
| `pessimistic_transaction*.cc`, `lock/point/*`, `transaction_base.cc` | `Utilities/Transactions/TransactionDatabase.cs`, `PessimisticTransaction.cs`, `PointLockManager.cs`, `LockTracker.cs`, `Transaction.cs`, `TransactionIterator.cs` |
| `write_prepared_txn_db.cc`, `write_prepared_txn.cc`, `snapshot_checker.cc` | `Utilities/Transactions/WritePreparedTxnDb.cs` (commit cache, prepared heap, old_commit_map, read callback, snapshot checker), `WritePreparedTxn.cs` |
| `write_unprepared_txn.cc`, `write_unprepared_txn_db.cc` | `Utilities/Transactions/WriteUnpreparedTxn.cs`, recovery in `TransactionDatabase.cs` |
| `db/read_callback.h`, `db/snapshot_checker.h`, `db/pre_release_callback.h` | `Db/ReadCallback.cs` (`ReadCallback`, `SnapshotChecker`, `IPreReleaseCallback`, `SeqPerBatchHooks`) |
| `utilities/write_batch_with_index/*` | `Utilities/WriteBatchWithIndex/*` |
| `utilities/backup/backup_engine.cc` | `Utilities/Backup/*` |

## Threading and async model

### Locks

The DB mutex (`DBImpl::mutex_`) is a .NET 9+ `Lock` (`DbImpl._mutex`). It protects the same state as in C++:
versions, column family metadata, flush and compaction queues, and snapshots. It is never held across an `await`.
Code that needs it is marked `REQUIRES: mutex` in comments, as upstream does. Readers do not take it: a read
references the column family's current **SuperVersion** (memtable, immutable memtables, `Version`) under a short
lock and releases it at the end (`GetReferencedSuperVersion` / `ReturnSuperVersion`; there is no thread-local
caching). Atomics use `Interlocked` and `Volatile`; async waits use `SemaphoreSlim` or `IValueTaskSource`-based
signals. Nothing polls.

### Write path: group commit

`Db/WriteThread.cs` ports `rocksdb::WriteThread`. A writer joins the queue. The first writer becomes the **leader**,
collects compatible queued writers into a group (no sync write under a non-sync leader, no mixing of WAL-disabled or
`NoSlowdown` writers, a size cap of the leader's size + 128 KiB), writes one WAL record for the group, inserts into
the memtables, publishes the sequence number, and hands leadership to the next queued writer.

Followers wait in one of two ways:

- **async writers** (`PutAsync`, `WriteAsync`) wait on a pooled `IValueTaskSource`
  (`ManualResetValueTaskSourceCore`). Continuations run asynchronously, so they never run on the leader's thread.
- **sync writers** (`Put`, `Write`) wait on a `ManualResetEventSlim` that spins briefly (like upstream's adaptive
  yield). A sync writer that becomes leader does the group's I/O on its own thread, so the sync API never waits
  on thread-pool work.

"Unbatched" writers (memtable switches and other exclusive operations) form a group of their own. Write stalls and
delayed writes (`Db/Compaction/WriteController.cs`) delay the leader with a token bucket, as upstream does.
Transactions add a write callback for conflict validation and prepared-section tracking
(`Db/TransactionWriteContext.cs`). WritePrepared / WriteUnprepared DBs (`seq_per_batch`) consume one sequence number
per sub-batch and run a pre-release callback per writer after the WAL write (commit cache / prepared heap updates);
their reads carry a `ReadCallback` and their flushes / compactions a `SnapshotChecker` (`Db/ReadCallback.cs`).

### Background work

- **Flushes** are scheduled by `MaybeScheduleFlush` (the flush half of `MaybeScheduleFlushOrCompaction`), up to the
  flush job limit derived from `MaxBackgroundJobs`. Each job runs as a thread-pool task (`Task.Run`). Flushes of one
  column family are serialized; different column families flush in parallel.
- **Compactions** are picked under the mutex and run on dedicated threads (`TaskCreationOptions.LongRunning`), up to
  the compaction limit, because a compaction is long, CPU-bound work that should not occupy the thread pool.
  `CompactRange` and `WaitForCompact` have sync and async versions. With `DbOptions.BottomPriorityBackgroundThreads`
  (upstream's BOTTOM `Env` pool, per DB here) compactions to the last level are queued for that many extra threads;
  with `reduce_file_locking` they reserve only their newest input run while queued and are repicked when they start.
- **Periodic tasks** are `System.Threading.Timer`s: the stats dump, the compaction trigger
  (`max_compaction_trigger_wakeup_seconds`: re-evaluates TTL / periodic / read-triggered compaction on idle DBs) and,
  with `preserve_internal_time_seconds` / `preclude_last_level_data_seconds`, the seqno-to-time recorder.
- **Listener notifications** are queued and drained on the thread pool (`EventListener.Notifications.cs`), so a slow
  listener does not hold the mutex.
- **Error recovery** (`Db/ErrorHandler.cs`) runs automatic resume for retryable background errors as a task; `Resume`
  runs the same recovery on demand.
- On close, scheduling stops, running jobs finish or are cancelled, and anyone waiting in `FlushAsync` or
  `WaitForCompactAsync` is released with `ObjectDisposedException` / `ShutdownInProgress`.

### Async I/O fast paths

Every I/O-bound public API returns `ValueTask`. Internally, most code paths are written once as
`async ValueTask XxxAsync(..., bool synchronous)`. With `synchronous: true`, every awaited call completes inline, so
the sync API runs the same code without thread-pool hops. This keeps the sync and async paths from drifting apart.

The file layer (`Env/LocalFileSystem.cs`, `Env/UnixNative.cs`) uses `RandomAccess` on `SafeFileHandle`. Async reads
first try `preadv2(RWF_NOWAIT)`: a page-cache hit completes synchronously, and only a miss goes to the asynchronous
path. Async writes use `pwritev2(RWF_NOWAIT)` where the file system supports it, and otherwise write small buffered
writes (up to 256 KiB, such as WAL records) inline, as native RocksDB does. Large writes, direct I/O and rate-limited
writes use the thread pool. Block-cache and memtable hits never reach the file layer, so `GetAsync` and `NextAsync`
usually complete synchronously. The hot async methods use `PoolingAsyncValueTaskMethodBuilder`, so even an async
completion does not allocate.

The result: the async API costs within a few percent of the sync API and allocates nothing per operation
([perf.md](perf.md), perf pass #2).

## Memory and pooling

The target is zero allocations per operation in steady state for reads, writes, iteration and compaction. This is
verified with `[MemoryDiagnoser]` and the benchmark runner's allocated-bytes-per-op column.

- **Memtable arena**: `Arena` and `ConcurrentArena` allocate slabs on the pinned object heap
  (`GC.AllocateUninitializedArray(pinned: true)`), so the skip list can use raw pointers. Slabs are recycled through
  `ArenaSlabPool` (capped at 256 MiB) when a memtable is freed. `InlineSkipList<TComparer>` takes a struct comparer, so
  comparisons are not virtual calls; reads are lock-free, and concurrent inserts use CAS.
- **Block buffers**: blocks in the block cache are rented from `BlockBufferPool`, which has finer size classes than
  `ArrayPool<byte>.Shared` (at most 25% padding instead of up to 100%), and are returned when evicted. `CachedBlock`
  handles are reference counted. Iterators pin the blocks they point into, which is why `Key`/`Value` spans are valid
  until the iterator moves.
- **Pooled objects**: `WriteBatch.Rent()` / `CreateWriteBatch()`, the write-thread writer objects, `GetContext`,
  lookup keys (`LookupKey` uses `stackalloc` for small keys), `PooledBufferWriter.RentWriter()`, per-thread
  decompression buffers, and reused table iterators in `LevelIterator`.
- **Caller-owned results**: point reads write into a caller `Span<byte>` or `IBufferWriter<byte>`, call a
  `ReadOnlySpanFunc` callback with a span into the memtable or the cached block, or return a `PooledValue` (an
  `ArrayPool` buffer released by `Dispose`). `MultiGetBatch` and `WideColumns` own reusable buffers. `Get` returning
  `byte[]` exists for convenience only.
- **Flush and compaction** use pooled builder buffers and stage table blocks so that no large array is allocated per
  output file. The per-entry code is marked `MethodImplOptions.AggressiveOptimization`, so the first flush or
  compaction of a process does not run as tier-0 code.
- **Hot-path rules** (CLAUDE.md): no LINQ, closures, boxing, `params` arrays or string formatting; `sealed` classes,
  `readonly struct`, `ref struct` enumerators, `static` lambdas; little-endian `BinaryPrimitives` / `Coding` for every
  on-disk integer.

## Design deviations from C++

| C++ | C# | Why |
|---|---|---|
| `Status` return values | `RocksDbException` thrown at the public boundary (`Code`, `SubCode`, `Severity`, `Retryable`, `DataLoss`, `Scope`); `bool Try...` for expected misses; throw sites in `ThrowHelper` | idiomatic .NET; hot methods stay inlinable; the codes have the same values |
| `Slice`, `std::string*` outputs | `ReadOnlySpan<byte>` (sync), `ReadOnlyMemory<byte>` (across `await` or stored), `IBufferWriter<byte>` outputs | no copies, no native memory |
| `PinnableSlice` | span callbacks, `PooledValue`, iterator spans valid until the next move | lifetime expressed in the type |
| `ColumnFamilyHandle*` parameter on every call | the `ColumnFamily` object is the receiver; `RocksDatabase` methods act on the default column family | no defaulted or nullable column family parameters |
| defaulted arguments, `Options` structs passed by value | explicit overloads; options are classes with readable properties (`GetDBOptions()`, `ColumnFamily.Options`) | no hidden behavior; options can be inspected after open |
| string-typed option values, `CreateFromString` | C# enums and typed objects (`FilterPolicy`, `Comparator`, `MergeOperator`, `CompactionFilter`, `SliceTransform`, `BlockCache`); strings only in OPTIONS files and `SetOptions(name, value)` | compile-time checking; the persisted text stays byte-identical |
| `Env` / `FileSystem` | `IFileSystem` (`LocalFileSystem`, `MemoryFileSystem` with crash simulation) | async-first file contracts |
| `rocksdb::DB` | `RocksDatabase` | a class named `RocksDb` would clash with the `RocksDb.Managed` namespace |
| `Version`, `ValueType` | `Db.DbVersion` (internal), `Db.EntryType` | avoid clashing with `System.Version` / `System.ValueType` |
| `TEST_SYNC_POINT` | `Db/TestHooks.cs` | deterministic concurrency tests |
| thread-local SuperVersion caching | a short lock to reference the SuperVersion | simpler; not measurable at current throughput |
| `std::thread` background pools (`Env::Schedule`) | thread-pool tasks for flushes, `LongRunning` tasks for compactions | .NET scheduling; no fixed pool to size |
| no cancellation | `CancellationToken` on every async API; a cancelled write leader that is delayed by a stall fails its whole group with `OperationCanceledException` | .NET convention |
| write-group follower wait (spin + futex) | `IValueTaskSource` for async writers, spinning `ManualResetEventSlim` for sync writers | async callers do not block a thread |

Format-level differences (compression corner cases, flush not dropping overwritten entries) and the features that are
not ported are listed in the [README](../README.md#known-deviations-and-gaps).
