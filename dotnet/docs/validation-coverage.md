# Validation coverage

Every C# source file of `src/RocksDb.Managed`, `src/RocksDb.Managed.Replication` and
`src/RocksDb.Managed.Replication.MagicOnion`, with the C++ file(s) it ports (taken from its summary comment) and the
audit that covered it. Findings and deviations are in [validation.md](validation.md); this file is the index.

Status values:

- **line-by-line (#1)**, **(#2)**, **(#3)**: compared side by side with the cited C++ in audit #1 (formats), audit #2
  (DB layer) or audit #3. For audit #3 the entry names the functions compared and any fix.
- **line-by-line (feature audit: ...)**: compared when the feature was ported. The findings are in the named section of
  validation.md: "Background errors, listeners and backups", "Transactions", "User features", "User-defined
  timestamps" or "Integrated BlobDB". Audit #3 re-checked defaults and constants where noted.
- **compaction-features pass**: the pass that ported audit #3's remaining compaction deviations (validation.md,
  "Compaction features (audit #3 remaining deviations)"); the named functions were compared with `main` and with the
  11.12 sources (identical except where noted).
- **managed-only, design-reviewed (#3)**: no C++ counterpart. This covers the I/O layer, pooling, test hooks and the
  replication layer ported from rocksdb-sharp. These were reviewed for durability protocol, concurrency and resource
  handling instead.
- **API wrapper, design-reviewed (#3)**: public surface that validates arguments and delegates to the audited `DbImpl`.

No file is left as "spot-checked" or "not audited".

Summary (229 files, about 70,400 lines):

| Status | Files |
|---|---:|
| line-by-line (#1) | 78 |
| line-by-line (#2) | 27 |
| line-by-line (feature audits) | 57 |
| line-by-line (#3) | 17 |
| line-by-line (compaction-features pass) | 2 |
| managed-only, design-reviewed | 43 |
| API wrapper, design-reviewed | 5 |

Two areas are under active change by other work streams: WritePrepared / WriteUnprepared transactions, and blob direct
write with entity blob columns. Their files were audited at the revision named in validation.md ("Audit #3"). Later
changes there need their own review.

| Managed file | Lines | C++ source(s) | Audit status |
|---|---:|---|---|
| `RocksDb.Managed.Replication.MagicOnion/ClusterService.cs` | 237 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication.MagicOnion/Dtos.cs` | 271 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication.MagicOnion/GrpcNodeServer.cs` | 83 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication.MagicOnion/PairService.cs` | 201 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication.MagicOnion/ReplicationService.cs` | 83 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Cluster/ClusterTypes.cs` | 123 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Cluster/ElectionTimer.cs` | 86 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Cluster/PeerState.cs` | 67 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Cluster/RaftClusterNode.cs` | 701 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Cluster/RaftLogTerms.cs` | 109 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Cluster/RaftPersistentState.cs` | 163 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/GlobalUsings.cs` | 3 | - | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Hosting/ClusterNodeHost.cs` | 675 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Hosting/HostingCommon.cs` | 230 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Hosting/PairNodeHost.cs` | 543 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Hosting/ReplicationPrimary.cs` | 120 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Internal/FileSystemHelpers.cs` | 85 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Replication/AdaptiveCommitDelayController.cs` | 248 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Replication/ReplicationClasses.cs` | 357 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Replication/ReplicationConsumer.cs` | 204 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Replication/ReplicationDelta.cs` | 228 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Replication/ReplicationSource.cs` | 185 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Replication/WalTailer.cs` | 248 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Switchover/PrimaryWriteGate.cs` | 124 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Switchover/ReplicationRoleState.cs` | 227 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `RocksDb.Managed.Replication/Switchover/SwitchoverNode.cs` | 542 | rocksdb-sharp Replication/Switchover/Cluster (no C++ counterpart) | managed-only, design-reviewed (#3) |
| `AttributeGroups.cs` | 286 | include/rocksdb/attribute_groups.h, db/db_impl/db_impl.cc, db/db_impl/db_impl_write.cc | line-by-line (feature audit: user features (wide-column APIs)) |
| `Cache/BlockCache.cs` | 327 | include/rocksdb/advanced_cache.h | line-by-line (#1) |
| `Cache/CacheKey.cs` | 231 | cache/cache_key.h, cache/cache_key.{h,cc}, table/unique_id.cc | line-by-line (#1) |
| `Cache/HyperClockCache.cs` | 796 | include/rocksdb/cache.h, cache/clock_cache.{h,cc} | line-by-line (#3): FixedHyperClockTable / BaseClockTable (meta word, probing, eviction, strict/non-strict charging, standalone); compaction-features pass: eviction_effort_cap, CalcHashBits overshoot clamp, table metadata charged up front |
| `Cache/LruCache.cs` | 748 | include/rocksdb/cache.h, cache/lru_cache.{h,cc}, cache/sharded_cache.{h,cc} | line-by-line (#3): LRUCacheShard / LRUHandleTable (insert, release, pools, eviction); metadata charge = CalcuMetaCharge with glibc malloc_usable_size (88) |
| `ColumnFamily.Entities.cs` | 146 | include/rocksdb/db.h | line-by-line (feature audit: user features (wide columns)) |
| `ColumnFamily.Options.cs` | 87 | include/rocksdb/db.h (SetOptions) | API wrapper, design-reviewed (#3): delegates to audited DbImpl |
| `ColumnFamily.Properties.cs` | 145 | include/rocksdb/db.h (GetProperty, GetApproximateSizes, ...) | API wrapper, design-reviewed (#3): delegates to audited DbImpl |
| `ColumnFamily.Timestamps.cs` | 222 | include/rocksdb/db.h, db/db_impl/db_impl_write.cc | line-by-line (feature audit: user-defined timestamps) |
| `ColumnFamily.cs` | 552 | include/rocksdb/db.h (DB API per column family) | API wrapper, design-reviewed (#3): delegates to audited DbImpl |
| `Compression/BlockCompression.Dictionary.cs` | 257 | util/compression.cc | line-by-line (#1) |
| `Compression/BlockCompression.Impl.cs` | 457 | util/compression.cc | line-by-line (#1) |
| `Compression/BlockCompression.cs` | 24 | util/compression.h | line-by-line (#1) |
| `Compression/CompressionDictionary.cs` | 93 | util/compression.h | line-by-line (#1) |
| `Compression/CompressionType.cs` | 15 | include/rocksdb/compression_type.h | line-by-line (#1) |
| `Core/DbProperties.cs` | 117 | include/rocksdb/db.h, db/internal_stats.cc | line-by-line (feature audit: user features (properties)) |
| `Core/Status.cs` | 171 | include/rocksdb/status.h, include/rocksdb/io_status.h | line-by-line (feature audit: errors/listeners/backups) |
| `Db/Blob/BlobFileBuilder.cs` | 205 | include/rocksdb/listener.h, db/blob/blob_file_builder.cc, db/blob/blob_log_writer.cc | line-by-line (feature audit: integrated BlobDB) |
| `Db/Blob/BlobFileMetaData.cs` | 116 | db/blob/blob_file_meta.{h,cc}, db/version_builder.cc | line-by-line (feature audit: integrated BlobDB) |
| `Db/Blob/BlobFileReader.cs` | 174 | db/blob/blob_file_reader.cc | line-by-line (feature audit: integrated BlobDB) |
| `Db/Blob/BlobGarbageMeter.cs` | 128 | db/blob/blob_garbage_meter.{h,cc}, db/blob/blob_counting_iterator.h | line-by-line (feature audit: integrated BlobDB) |
| `Db/Blob/BlobIndex.cs` | 153 | db/blob/blob_index.h | line-by-line (feature audit: integrated BlobDB (+ #3 re-check)) |
| `Db/Blob/BlobLogFormat.cs` | 130 | db/blob/blob_log_format.{h,cc} | line-by-line (feature audit: integrated BlobDB (+ #3 re-check)) |
| `Db/Blob/BlobSource.cs` | 353 | db/blob/blob_source.cc, db/blob/blob_file_cache.cc, db/version_set.cc | line-by-line (feature audit: integrated BlobDB) |
| `Db/Blob/CompactionBlobProcessor.cs` | 110 | db/compaction/compaction_iterator.cc, db/blob/blob_fetcher.cc | line-by-line (feature audit: integrated BlobDB) |
| `Db/Blob/CompactionBlobResolver.cs` | 97 | db/compaction/compaction_iterator.{h,cc} (CompactionBlobResolver) | line-by-line (feature audit: user features (wide-column APIs)) |
| `Db/ColumnFamilyData.cs` | 257 | db/memtable_list.h, db/column_family.h, db/column_family.cc | line-by-line (#2) |
| `Db/Compaction/ClippingIterator.cs` | 101 | db/compaction/clipping_iterator.h | line-by-line (completeness pass) |
| `Db/Compaction/CompactionPicker.CompactFiles.cs` | 265 | db/compaction/compaction_picker.cc (SanitizeCompactionInputFilesForAllLevels, SanitizeAndConvertCompactionInputFiles, GetCompactionInputsFromFileNumbers, PickCompactionForCompactFiles) | line-by-line (completeness pass) |
| `Db/Compaction/ColumnFamilyCompactionState.cs` | 34 | db/compaction/compaction_picker.h (NullCompactionPicker), db/column_family.h | line-by-line (#2) |
| `Db/Compaction/Compaction.cs` | 446 | include/rocksdb/listener.h, db/compaction/compaction.{h,cc} | line-by-line (#2); #3: MaxOutputFileSize, MaxInputFileNewestKeyTime (new), MinInputFileOldestAncestorTime; compaction-features pass: output_split_key_, CreateSstPartitioner, IsTrivialMove partitioner check, GetOutputTemperature inputs |
| `Db/Compaction/CompactionIterator.cs` | 762 | db/compaction/compaction_iteration_stats.h, db/compaction/compaction_iterator.cc | line-by-line (#3): NextFromInput / InvokeFilterIfNeeded / PrepareOutput / findEarliestVisibleSnapshot vs compaction_iterator.cc (main) |
| `Db/Compaction/CompactionJob.cs` | 1347 | db/compaction/compaction_job.cc, db/compaction/compaction_outputs.cc, db/compaction/subcompaction_state.h | line-by-line (#2); #3: ShouldStopBefore / grandparent boundaries, output time properties (fixed newest_key_time); compaction-features pass: full ShouldStopBefore (TTL cuts, partitioner, upper-bound size, round-robin split), GetOutputTemperature, compression switch; completeness pass: GenSubcompactionBoundaries, subcompaction execution and aggregation, AddRangeDels subcompaction bounds, SetOutputSlitKey |
| `Db/Compaction/CompactionPicker.cs` | 623 | db/compaction/compaction_picker.{h,cc} | line-by-line (#2); compaction-features pass: enable_compression / output temperature / output split key in NewCompaction; completeness pass: split key from the input version's cursors |
| `Db/Compaction/FifoCompactionPicker.cs` | 311 | db/compaction/compaction_picker_fifo.cc | line-by-line (#3): PickTTL/Size/IntraL0/PickCompaction; **fixed** TTL expiry by newest key time; compaction-features pass: whole file (blob-aware sizing, PickRatioBasedIntraL0Compaction, PickTemperatureChangeCompaction) |
| `Db/Compaction/LevelCompactionPicker.cs` | 506 | db/compaction/compaction_picker_level.cc | line-by-line (#2); completeness pass: kBottommostFiles, cursors moved to the version |
| `Db/Compaction/UniversalCompactionPicker.cs` | 680 | db/compaction/compaction_picker_universal.cc | line-by-line (compaction-features pass): whole file (read-triggered compaction, require_max_output_level and ShouldSkipLastSortedRunForSizeAmpCompaction added in the tiering pass; periodic, size amp with MightExcludeNewL0sToReduceWriteStop and incremental mode, size ratio, max_read_amp, delete-triggered, compression_size_percent) |
| `Db/Compaction/VersionStorageInfo.Compaction.cs` | 1040 | db/version_set.cc | line-by-line (#2); #3: FIFO score / GetExpiredTtlFilesCount; compaction-features pass: blob-aware FIFO score, ShouldChangeFileTemperature, **fixed** periodic-compaction file age (file_creation_time first); completeness pass: compensated sizes, bottommost marking in ComputeCompactionScore, version cursors in SortFileByRoundRobin |
| `Db/Compaction/VersionStorageInfo.Inherited.cs` | 354 | db/version_set.{h,cc} (UpdateAccumulatedStats, MaybeInitializeFileMetaData, ComputeCompensatedSizes, GenerateBottommostFiles, UpdateOldestSnapshot, ComputeBottommostFilesMarkedForCompaction, compact_cursor_ / GetNextCompactCursor), db/dbformat.h (BottommostSeqnoCanBeZeroed) | line-by-line (completeness pass) |
| `Db/Compaction/WriteController.cs` | 346 | db/write_controller.{h,cc}, db/column_family.cc (RecalculateWriteStallConditions, SetupDelay, GetPendingCompactionBytesForCompactionSpeedup) | line-by-line (#2); completeness pass: **fixed** delay recovery reward, rate carried between delays, compaction-speedup thresholds |
| `Db/CompactionFilter.cs` | 95 | include/rocksdb/compaction_filter.h | line-by-line (#3): compaction_filter.h decisions / value types |
| `Db/DbFormat.cs` | 158 | db/dbformat.h | line-by-line (#1) |
| `Db/DbImpl.Checkpoint.cs` | 280 | include/rocksdb/metadata.h, db/db_filesnapshot.cc, utilities/checkpoint/checkpoint_impl.cc | line-by-line (#2) |
| `Db/DbImpl.ColumnFamilies.cs` | 128 | db/db_impl/db_impl.cc (CreateColumnFamilyImpl, DropColumnFamilyImpl) | line-by-line (#2) |
| `Db/DbImpl.Compaction.cs` | 912 | db/db_impl/db_impl_compaction_flush.cc, db/db_impl/db_impl_write.cc | line-by-line (#2) |
| `Db/DbImpl.Comparators.cs` | 136 | db/version_edit_handler.cc, db/version_builder.cc, util/udt_util.cc | line-by-line (feature audit: user features (comparators) + UDT memtable-only) |
| `Db/DbImpl.Flush.cs` | 418 | db/db_impl/db_impl_compaction_flush.cc (flush), db/flush_job.cc | line-by-line (#2) |
| `Db/DbImpl.Ingest.cs` | 107 | db/db_impl/db_impl.cc | line-by-line (#2) |
| `Db/DbImpl.LazyEntity.cs` | 34 | db/db_impl/db_impl.cc (GetEntityLazyImpl / GetEntityLazyForBatch) | line-by-line (feature audit: user features (wide-column APIs)) |
| `Db/DbImpl.Monitoring.cs` | 420 | db/event_helpers.cc | line-by-line (feature audit: user features (LOG)) |
| `Db/DbImpl.MultiGet.cs` | 234 | db/db_impl/db_impl.cc, db/version_set.cc | line-by-line (feature audit: UDT / MultiGet (+ #3 read-sequence rule)) |
| `Db/DbImpl.Open.cs` | 596 | db/db_impl/db_impl_open.cc | line-by-line (#2); #3: fixed WAL checksum, blob GC option validation, FIFO TTL reader preload |
| `Db/DbImpl.Options.cs` | 283 | db/db_impl/db_impl.cc, db/column_family.cc, options/cf_options.cc | line-by-line (feature audit: user features (SetOptions) (+ #3)) |
| `Db/DbImpl.Properties.cs` | 657 | db/internal_stats.cc, db/db_impl/db_impl.cc | line-by-line (feature audit: user features (properties)) |
| `Db/DbImpl.Read.cs` | 477 | db/db_impl/db_impl.cc (GetImpl, NewIterator), db/version_set.cc (Version::Get) | line-by-line (#2) |
| `Db/DbImpl.ReadOnly.cs` | 330 | db/db_impl/db_impl_readonly.cc, db/db_impl/db_impl_secondary.cc | line-by-line (#3): db_impl_readonly.cc / db_impl_secondary.cc; **fixed** WAL checksum |
| `Db/DbImpl.SeqnoTime.cs` | 165 | db/db_impl/db_impl.cc, db/db_impl/db_impl_open.cc (seqno-to-time recording) | line-by-line (tiering pass: validation.md "Compaction triggers and tiering") |
| `Db/DbImpl.Replication.cs` | 539 | db/db_impl/db_impl_files.cc, db/wal_manager.cc, db/db_filesnapshot.cc | line-by-line (#2) |
| `Db/DbImpl.Timestamps.cs` | 295 | db/db_impl/db_impl.h, db/db_impl/db_impl_compaction_flush.cc | line-by-line (feature audit: user-defined timestamps) |
| `Db/DbImpl.Transactions.cs` | 524 | db/logs_with_prep_tracker.cc, db/db_impl/db_impl_files.cc, db/db_impl/db_impl.cc | line-by-line (feature audit: transactions) |
| `Db/DbImpl.Write.cs` | 760 | db/db_impl/db_impl_write.cc, db/write_batch.cc (MemTableInserter) | line-by-line (#2) |
| `Db/DbImpl.cs` | 612 | db/db_impl/db_impl.cc, db/db_impl/db_impl_compaction_flush.cc | line-by-line (#2) |
| `Db/DbWithTtl.cs` | 310 | utilities/ttl/db_ttl_impl.{h,cc}, utilities/ttl/db_ttl_impl.cc | line-by-line (#3): utilities/ttl/db_ttl_impl.cc; **fixed** IsStale sign |
| `Db/ErrorHandler.cs` | 755 | db/error_handler.{h,cc}, db/event_helpers.cc, db/error_handler.cc | line-by-line (feature audit: errors/listeners/backups (+ #3 severity maps)) |
| `Db/EventListener.Notifications.cs` | 351 | db/event_helpers.cc, db/db_impl/db_impl_compaction_flush.cc (NotifyOn*) | line-by-line (feature audit: errors/listeners/backups) |
| `Db/EventListener.cs` | 293 | include/rocksdb/listener.h | line-by-line (feature audit: errors/listeners/backups) |
| `Db/ExternalSstFileIngestionJob.cs` | 428 | db/external_sst_file_ingestion_job.h, db/external_sst_file_ingestion_job.cc | line-by-line (#2) |
| `Db/FileMetaData.cs` | 253 | table/unique_id_impl.h, db/version_edit.h, include/rocksdb/file_checksum.h | line-by-line (#1); #3: in-memory NewestKeyTime |
| `Db/FileNames.cs` | 458 | include/rocksdb/types.h, include/rocksdb/transaction_log.h, file/filename.{h,cc} | line-by-line (#1) |
| `Db/FlushJob.cs` | 516 | db/flush_job.cc, db/builder.cc | line-by-line (#2); #3: table time properties (fixed newest_key_time); compaction-features pass: oldest_key_time / oldest_ancestor_time from the memtable first-insert time, rocksdb.creation.time, default_write_temperature |
| `Db/GetContext.cs` | 387 | table/get_context.cc, db/db_impl/db_impl.cc | line-by-line (#1) |
| `Db/ITableCache.cs` | 45 | db/table_cache.h | line-by-line (#2) |
| `Db/IWriteBatchHandler.cs` | 36 | include/rocksdb/write_batch.h | line-by-line (#3): write_batch.h Handler |
| `Db/InternalKey.cs` | 235 | db/dbformat.h, db/lookup_key.h, db/dbformat.cc | line-by-line (#1) |
| `Db/InternalKeyComparator.cs` | 198 | db/dbformat.h, db/dbformat.cc, table/block_based/index_builder.cc | line-by-line (#1) |
| `Db/Iterators/DbIter.cs` | 1495 | db/db_iter.cc | line-by-line (#1) |
| `Db/Iterators/DbIteratorFactory.cs` | 177 | db/db_impl/db_impl.cc, db/version_set.cc | line-by-line (#1) |
| `Db/Iterators/EmptyIterator.cs` | 49 | table/iterator.cc | line-by-line (#1) |
| `Db/Iterators/InternalIterator.cs` | 45 | table/internal_iterator.h | line-by-line (#1) |
| `Db/Iterators/Iterator.cs` | 444 | include/rocksdb/iterator.h | line-by-line (#1) |
| `Db/Iterators/LevelIterator.cs` | 384 | db/version_set.cc | line-by-line (#1) |
| `Db/Iterators/MemTableInternalIterator.cs` | 45 | db/memtable.cc | line-by-line (#1) |
| `Db/Iterators/MergingIterator.cs` | 363 | table/merging_iterator.cc | line-by-line (#1) |
| `Db/Iterators/MultiCfIterator.cs` | 335 | db/multi_cf_iterator_impl.h, db/coalescing_iterator.{h,cc}, db/attribute_group_iterator_impl.{h,cc} | line-by-line (feature audit: user features (wide-column APIs)) |
| `Db/Iterators/TableInternalIterator.cs` | 126 | table/block_based/block_based_table_iterator.cc | line-by-line (#1) |
| `Db/Manifest.cs` | 608 | db/log_reader.cc, db/log_writer.cc, db/blob/blob_file_meta.h | line-by-line (#1) |
| `Db/ManifestLog.cs` | 84 | db/version_set.cc | line-by-line (#1) |
| `Db/MemTable.cs` | 673 | db/memtable.h, db/memtable.cc, memtable/skiplistrep.cc | line-by-line (#1); compaction-features pass: UpdateOldestKeyTime / ApproximateOldestKeyTime |
| `Db/MemTableRangeTombstones.cs` | 291 | db/range_tombstone_fragmenter.{h,cc} | line-by-line (#1) |
| `Db/MemTableSupport.cs` | 185 | db/memtable.cc, db/column_family.cc, db/merge_context.h | line-by-line (#1) |
| `Db/MergeHelper.BuiltinOperators.cs` | 85 | utilities/merge_operators/uint64add.cc, utilities/merge_operators/string_append/stringappend.cc | line-by-line (#1) |
| `Db/MergeHelper.cs` | 517 | db/merge_context.h | line-by-line (#1) |
| `Db/MergeOperator.cs` | 88 | include/rocksdb/merge_operator.h, utilities/merge_operators.cc | line-by-line (#1) |
| `Db/RangeDelAggregator.cs` | 363 | db/range_del_aggregator.{h,cc}, db/dbformat.h | line-by-line (#1) |
| `Db/SeqnoToTimeMapping.cs` | 485 | db/seqno_to_time_mapping.{h,cc} | line-by-line (tiering pass: validation.md "Compaction triggers and tiering") |
| `Db/Snapshot.cs` | 101 | include/rocksdb/snapshot.h, db/snapshot_impl.h | line-by-line (#2) |
| `Db/SstFileWriter.cs` | 329 | include/rocksdb/sst_file_writer.h, table/sst_file_writer.cc | line-by-line (#2) |
| `Db/TableCache.cs` | 263 | db/table_cache.cc, db/db_impl/db_impl.cc | line-by-line (#2); #3: records newest_key_time from opened readers |
| `Db/TableFactory.cs` | 300 | table/table_builder.h, table/block_based/block_based_table_factory.cc, table/block_based/filter_policy.cc | line-by-line (feature audit: table prefix / comparators) |
| `Db/TestHooks.cs` | 46 | test_util/sync_point.h (lite) | managed-only, design-reviewed (#3): TEST_SYNC_POINT lite (DEBUG only) |
| `Db/TransactionLogIterator.cs` | 396 | db/transaction_log_impl.cc | line-by-line (#3): wal_iterator_impl.cc (renamed from transaction_log_impl.cc); compaction-features pass: **fixed** sequence gap ends the run with NotFound (identical in 11.12) |
| `Db/TransactionWriteContext.cs` | 60 | db/write_callback.h, db/db_impl/db_impl_write.cc | line-by-line (feature audit: transactions) |
| `Db/Version.cs` | 199 | db/version_set.h, db/version_set.cc, db/version_builder.cc | line-by-line (#2) |
| `Db/VersionEdit.cs` | 805 | db/version_edit.h, db/version_edit.{h,cc} | line-by-line (#1) |
| `Db/VersionSet.cs` | 523 | db/version_set.cc, db/column_family.cc | line-by-line (#2) |
| `Db/Wide/WideColumnSerialization.cs` | 264 | db/wide/wide_column_serialization.{h,cc} | line-by-line (feature audit: user features (wide columns)) |
| `Db/Wide/WideColumns.cs` | 140 | include/rocksdb/wide_columns.h, db/wide/wide_columns.cc | line-by-line (feature audit: user features (wide columns)) |
| `Db/WriteBatch.cs` | 812 | db/write_batch.cc, db/write_batch_internal.h, include/rocksdb/write_batch.h | line-by-line (#1) |
| `Db/WriteBatchReader.cs` | 300 | db/write_batch.cc | line-by-line (#1) |
| `Db/WriteThread.cs` | 251 | db/write_thread.cc | line-by-line (#2) |
| `Env/AlignedBuffer.cs` | 81 | util/aligned_buffer.h | managed-only, design-reviewed (#3): aligned buffers for O_DIRECT (util/aligned_buffer.h idea) |
| `Env/EnvFileOptions.cs` | 99 | include/rocksdb/env.h, include/rocksdb/rate_limiter.h, file/sequence_file_reader.cc | managed-only, design-reviewed (#3): file options records |
| `Env/FileContracts.cs` | 96 | file/writable_file_writer.h | managed-only, design-reviewed (#3): IFileSystem contracts (FileSystem / RandomAccessFileReader roles) |
| `Env/FilePrefetchBuffer.cs` | 136 | file/file_prefetch_buffer.{h,cc} (lite) | managed-only, design-reviewed (#3): lite readahead buffer (file_prefetch_buffer.cc semantics for auto-readahead) |
| `Env/LocalFileSystem.cs` | 1021 | env/fs_posix.cc, env/io_posix.cc, file/writable_file_writer.cc | managed-only, design-reviewed (#3): POSIX/Windows file system: rename(2) overwrite, fdatasync, dir fsync, OFD LOCK, truncate-on-close |
| `Env/MemoryFileSystem.cs` | 491 | env/mock_env.cc, utilities/fault_injection_fs.{h,cc} | managed-only, design-reviewed (#3): in-memory FS for tests (mock_env idea), crash simulation |
| `Env/UnixNative.cs` | 431 | env/io_posix.cc, env/fs_posix.cc | managed-only, design-reviewed (#3): P/Invoke (fdatasync, fsync dir, OFD locks, preadv2/pwritev2) |
| `LazyWideColumns.cs` | 483 | include/rocksdb/lazy_wide_columns.h, db/wide/lazy_wide_columns.cc, db/wide/read_path_blob_resolver.cc | line-by-line (feature audit: user features (wide-column APIs)) |
| `Logging/FileInfoLogger.cs` | 233 | logging/auto_roll_logger.cc, logging/env_logger.h | line-by-line (feature audit: user features (LOG)) |
| `Logging/InfoLogger.cs` | 72 | include/rocksdb/env.h, env/env.cc | line-by-line (feature audit: user features (LOG)) |
| `Memory/Arena.cs` | 225 | memory/allocator.h, memory/arena.h, memory/arena.cc | line-by-line (#1) |
| `Memory/BlockBufferPool.cs` | 114 | (managed buffer pool) | managed-only, design-reviewed (#3): pooled block buffers |
| `Memory/ConcurrentArena.cs` | 235 | memory/concurrent_arena.h, memory/concurrent_arena.cc | line-by-line (#1) |
| `Memtable/InlineSkipList.cs` | 649 | memtable/inlineskiplist.h | line-by-line (#1) |
| `Monitoring/FileReadSample.cs` | 86 | monitoring/file_read_sample.h | line-by-line (tiering pass: validation.md "Compaction triggers and tiering") |
| `Monitoring/InternalStats.cs` | 345 | db/internal_stats.h, db/internal_stats.{h,cc} | line-by-line (feature audit: user features (properties)) |
| `Monitoring/Statistics.cs` | 456 | include/rocksdb/statistics.h, monitoring/statistics.cc, monitoring/histogram.cc | line-by-line (feature audit: user features (statistics)) |
| `Monitoring/StatisticsNames.g.cs` | 1075 | monitoring/statistics.cc, include/rocksdb/statistics.h | line-by-line (feature audit: user features (statistics; generated from TickersNameMap)) |
| `MultiGetBatch.cs` | 332 | db/db_impl/db_impl.cc (MultiGet key context), table/multiget_context.h | line-by-line (feature audit: UDT / MultiGet) |
| `Options/BlockBasedTableOptions.cs` | 125 | include/rocksdb/table.h, table/block_based/block_based_table_factory.cc | line-by-line (#1) |
| `Options/ColumnFamilyOptions.cs` | 400 | include/rocksdb/options.h, options/cf_options.cc | line-by-line (#3): defaults vs advanced_options.h/options.h; **fixed** compaction_options_* persistence; compaction-features pass: sst_partitioner_factory object, target_file_size_is_upper_bound |
| `Options/CompactionStyleOptions.cs` | 240 | include/rocksdb/advanced_options.h, options/cf_options.cc, include/rocksdb/universal_compaction.h | line-by-line (#3): universal_compaction.h / advanced_options.h defaults; **fixed** OPTIONS struct; compaction-features pass: every member typed (FileTemperatureAge vector format) |
| `Options/CompressionOptions.cs` | 57 | include/rocksdb/advanced_compression.h | line-by-line (#3): defaults vs advanced_compression.h |
| `Options/CustomizableOption.cs` | 66 | options/customizable.cc | line-by-line (#3): customizable ids (options/customizable.cc) |
| `Options/DbOptions.cs` | 211 | include/rocksdb/options.h, options/db_options.cc | line-by-line (#3): defaults vs options.h |
| `Options/IngestExternalFileOptions.cs` | 33 | include/rocksdb/options.h | line-by-line (#3): defaults vs options.h |
| `Options/OperationOptions.cs` | 100 | include/rocksdb/options.h | line-by-line (#3): defaults vs ReadOptions/WriteOptions/FlushOptions/CompactRangeOptions |
| `Options/OptionEnums.cs` | 178 | include/rocksdb/types.h, include/rocksdb/advanced_options.h, include/rocksdb/options.h | line-by-line (#3): enum values / names |
| `Options/OptionTypeInfo.cs` | 484 | options/cf_options.cc, options/db_options.cc, table/block_based/block_based_table_factory.cc | line-by-line (#1) |
| `Options/OptionsBase.cs` | 77 | options/options_helper.cc (unknown-option preservation) | managed-only, design-reviewed (#3): OPTIONS round-trip plumbing |
| `Options/OptionsFile.cs` | 408 | options/options_parser.cc, include/rocksdb/version.h | line-by-line (#1) |
| `Options/SstPartitioner.cs` | 166 | include/rocksdb/sst_partitioner.h, db/compaction/sst_partitioner.cc | line-by-line (compaction-features pass): SstPartitioner / factory / fixed prefix, ShouldPartitionByPrefix, OPTIONS id form |
| `Options/SliceTransform.cs` | 180 | include/rocksdb/slice_transform.h, util/slice.cc | line-by-line (feature audit: table prefix (prefix extractor)) |
| `PooledValue.cs` | 92 | include/rocksdb/metadata.h | managed-only, design-reviewed (#3): pooled Get results / LiveFileMetadata record |
| `RocksDatabase.Replication.cs` | 295 | db/db_impl/db_impl_readonly.cc, db/db_impl/db_impl_secondary.cc | API wrapper, design-reviewed (#3): delegates to audited DbImpl |
| `RocksDatabase.Timestamps.cs` | 43 | include/rocksdb/db.h (timestamp APIs) | line-by-line (feature audit: user-defined timestamps) |
| `RocksDatabase.cs` | 408 | include/rocksdb/db.h | API wrapper, design-reviewed (#3): delegates to audited DbImpl |
| `Table/Block.cs` | 170 | table/block_based/block.{h,cc}, table/block_based/data_block_footer.cc | line-by-line (#1) |
| `Table/BlockBasedTableBuilder.cs` | 784 | table/block_based/block_based_table_builder.cc | line-by-line (#1); compaction-features pass: EstimatedTailSize |
| `Table/BlockBasedTableIterator.cs` | 420 | table/block_based/partitioned_index_iterator.cc, table/block_based/block_based_table_iterator.cc | line-by-line (#1) |
| `Table/BlockBasedTableReader.MultiGet.cs` | 138 | table/block_based/block_based_table_reader_sync_and_async.h | line-by-line (#1) |
| `Table/BlockBasedTableReader.cs` | 1393 | table/block_based/block_based_table_reader.{h,cc}, meta_blocks.cc | line-by-line (#1) |
| `Table/BlockBuilder.cs` | 363 | table/block_based/block_builder.{h,cc} | line-by-line (#1) |
| `Table/BlockCacheStatistics.cs` | 100 | table/block_based/block_based_table_reader.cc (UpdateCacheHit/MissMetrics) | line-by-line (#1) |
| `Table/BlockChecksum.cs` | 108 | table/format.cc, table/format.h, table/block_based/reader_common.cc | line-by-line (#1) |
| `Table/BlockFetcher.cs` | 320 | table/block_based/block_cache.h, table/block_fetcher.cc, table/format.cc | line-by-line (#1) |
| `Table/BlockHandle.cs` | 146 | table/format.h, table/format.cc | line-by-line (#1) |
| `Table/BlockIter.cs` | 895 | table/block_based/block.{h,cc}, block_util.h | line-by-line (#1) |
| `Table/ChecksumType.cs` | 11 | include/rocksdb/table.h | line-by-line (#1) |
| `Table/DataBlockHashIndex.cs` | 120 | table/block_based/data_block_hash_index.{h,cc}, util/hash.h | line-by-line (#1) |
| `Table/FilterBlockBuilder.cs` | 377 | table/block_based/filter_block.h, table/block_based/full_filter_block.cc, table/block_based/partitioned_filter_block.cc | line-by-line (#1); compaction-features pass: CurrentFilterSizeEstimate (approximated space) |
| `Table/Filters/BloomImpl.cs` | 198 | util/bloom_impl.h | line-by-line (#1) |
| `Table/Filters/BuiltinFilterPolicies.cs` | 216 | table/block_based/filter_policy.cc | line-by-line (#1) |
| `Table/Filters/FilterAllocationModel.cs` | 66 | table/block_based/filter_policy.cc | line-by-line (#1) |
| `Table/Filters/FilterBitsBuilders.cs` | 792 | table/block_based/filter_policy.cc, port/port_posix.h | line-by-line (#1) |
| `Table/Filters/FilterBitsReaders.cs` | 257 | table/block_based/filter_policy_internal.h | line-by-line (#1) |
| `Table/Filters/FilterContracts.cs` | 66 | table/block_based/filter_policy_internal.h, include/rocksdb/filter_policy.h | line-by-line (#1) |
| `Table/Filters/Standard128Ribbon.cs` | 509 | util/ribbon_impl.h, util/ribbon_alg.h, util/ribbon_config.cc | line-by-line (#1) |
| `Table/Footer.cs` | 220 | table/format.h, table/block_based/block_based_table_reader.h, table/format.cc | line-by-line (#1) |
| `Table/IndexBuilder.cs` | 480 | table/block_based/index_builder.h, table/block_based/index_builder.{h,cc} | line-by-line (#1); compaction-features pass: UpdateIndexSizeEstimate (shortened, partitioned) |
| `Table/PinnedFilterPartitions.cs` | 56 | table/block_based/partitioned_filter_block.cc | line-by-line (#1) |
| `Table/TableFormatOptions.cs` | 122 | include/rocksdb/table.h, table/table_builder.h | line-by-line (#1) |
| `Table/TableProperties.cs` | 373 | table/table_properties.cc, table/sst_file_writer.cc, include/rocksdb/table_properties.h | line-by-line (#1) |
| `Util/ByteCompare.cs` | 73 | (memcmp) | managed-only, design-reviewed (#3): memcmp |
| `Util/Coding.cs` | 256 | util/coding.h | line-by-line (#1) |
| `Util/Comparator.cs` | 211 | include/rocksdb/comparator.h, util/comparator.cc | line-by-line (#1) |
| `Util/Comparators.cs` | 132 | util/comparator.cc | line-by-line (#1) |
| `Util/Crc32C.cs` | 108 | util/crc32c.h, util/crc32c.cc | line-by-line (#1) |
| `Util/Hash.cs` | 225 | util/hash.h, util/hash.cc, util/fastrange.h | line-by-line (#1) |
| `Util/HashXxph3.cs` | 274 | util/xxph3.h | line-by-line (#1) |
| `Util/PooledBufferWriter.cs` | 123 | (std::string buffers) | managed-only, design-reviewed (#3): std::string buffers |
| `Utilities/Backup/BackupEngine.cs` | 383 | utilities/backup/backup_engine.cc, include/rocksdb/utilities/backup_engine.h | line-by-line (feature audit: errors/listeners/backups) |
| `Utilities/Backup/BackupEngineOptions.cs` | 215 | include/rocksdb/utilities/backup_engine.h | line-by-line (feature audit: errors/listeners/backups (+ #3 defaults)) |
| `Utilities/Backup/BackupEngineReadOnly.cs` | 798 | include/rocksdb/utilities/backup_engine.h, utilities/backup/backup_engine.cc | line-by-line (feature audit: errors/listeners/backups) |
| `Utilities/Backup/BackupMeta.cs` | 406 | utilities/backup/backup_engine.cc | line-by-line (feature audit: errors/listeners/backups) |
| `Utilities/Transactions/ByteKeyComparer.cs` | 31 | (managed helper) | managed-only, design-reviewed (#3): helper |
| `Utilities/Transactions/IndexedWriteBatch.cs` | 126 | utilities/write_batch_with_index (overwrite_key adapter) | managed-only, design-reviewed (#3): adapter over WBWI |
| `Utilities/Transactions/LockTracker.cs` | 194 | utilities/transactions/lock/point/point_lock_tracker.h, utilities/transactions/lock/point/point_lock_tracker.cc | line-by-line (feature audit: transactions) |
| `Utilities/Transactions/OptimisticTransactionDatabase.cs` | 257 | utilities/transactions/optimistic_transaction_db_impl.cc, utilities/transactions/optimistic_transaction.cc | line-by-line (feature audit: transactions) |
| `Utilities/Transactions/PessimisticTransaction.cs` | 671 | utilities/transactions/pessimistic_transaction.cc | line-by-line (feature audit: transactions) |
| `Utilities/Transactions/PointLockManager.cs` | 465 | utilities/transactions/lock/point/point_lock_manager.cc | line-by-line (feature audit: transactions) |
| `Utilities/Transactions/RocksDatabase.Transactions.cs` | 9 | (API glue) | managed-only, design-reviewed (#3): API glue |
| `Utilities/Transactions/Transaction.cs` | 679 | include/rocksdb/utilities/transaction.h, utilities/transactions/transaction_base.cc | line-by-line (feature audit: transactions) |
| `Utilities/Transactions/TransactionDatabase.cs` | 347 | utilities/transactions/pessimistic_transaction_db.cc | line-by-line (feature audit: transactions) |
| `Utilities/Transactions/TransactionIterator.cs` | 74 | utilities/write_batch_with_index (BaseDeltaIterator adapter) | managed-only, design-reviewed (#3): adapter over BaseDeltaIterator |
| `Utilities/Transactions/TransactionOptions.cs` | 139 | include/rocksdb/utilities/transaction.h, include/rocksdb/utilities/transaction_db.h | line-by-line (feature audit: transactions (+ #3 defaults)) |
| `Utilities/WriteBatchWithIndex/BaseDeltaIterator.cs` | 352 | utilities/write_batch_with_index/write_batch_with_index_internal.cc | line-by-line (feature audit: user features (WBWI)) |
| `Utilities/WriteBatchWithIndex/WriteBatchEntryIndex.cs` | 257 | utilities/write_batch_with_index/write_batch_with_index_internal.{h,cc} | line-by-line (feature audit: user features (WBWI)) |
| `Utilities/WriteBatchWithIndex/WriteBatchWithIndex.cs` | 687 | utilities/write_batch_with_index/write_batch_with_index.cc | line-by-line (feature audit: user features (WBWI)) |
| `Utilities/WriteBatchWithIndex/WriteBatchWithIndexIterator.cs` | 194 | utilities/write_batch_with_index/write_batch_with_index_internal.cc (WBWIIteratorImpl) | line-by-line (feature audit: user features (WBWI)) |
| `Wal/FragmentBufferedLogReader.cs` | 570 | db/log_reader.cc (FragmentBufferedReader) | line-by-line (#1) |
| `Wal/LogFormat.cs` | 86 | db/log_format.h | line-by-line (#1) |
| `Wal/LogReader.cs` | 835 | db/log_reader.h, db/log_reader.{h,cc} | line-by-line (#1) |
| `Wal/LogWriter.cs` | 430 | db/log_writer.{h,cc} | line-by-line (#1) |
| `Wal/WalMetadataRecords.cs` | 88 | util/compression.h, util/udt_util.h, db/dbformat.h | line-by-line (#1) |
| `Wal/WalStreamingCompression.cs` | 130 | util/compression.{h,cc}, util/compression.cc | line-by-line (#1) |
