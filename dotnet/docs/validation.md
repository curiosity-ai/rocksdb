# Validation of RocksDb.Managed against the C++ sources

Audit of the managed port (`src/RocksDb.Managed`) against the C++ tree in the repository root (RocksDB 11.14, `main`).
Each managed file was read side by side with the C++ files it cites. The audit checked:

- on-disk constants, magic numbers, tags and enum values
- byte order and field order
- boundary conditions: empty input, truncated input, overflow
- error paths: every C++ `Corruption` / `InvalidArgument` path has a managed equivalent
- option defaults against `include/rocksdb/*options*.h` and `table.h`
- algorithm fidelity
- concurrency

Status values used in the tables:

- **verified**: matches upstream. Any differences are listed and harmless.
- **fixed**: a discrepancy was found and corrected in this audit (regression tests in
  `tests/RocksDb.Managed.Tests/Validation/AuditRegressionTests.cs` unless noted).
- **known deviation**: intentional or accepted difference, with the reason.
- **not ported**: an upstream feature a user could hit that the port lacks.

The existing tests and the native compat tests back many "verified" entries. These include the byte-identical
WriteBatch, SST, MANIFEST and filter comparisons and the WAL read/write tests in both directions. Ports of upstream
test vectors added by this audit:

- `db/db_iter_test.cc`: DBIterator1..14, SeekToLastOccurrenceSeq0, the lower-bound tests and a SeekForPrev
  upper-bound clamp. See `Iterators/DbIterUpstreamTests.cs`.
- `util/coding_test.cc`: fixed-width encodings, varints, overflow and truncation, length-prefixed slices, zigzag.
- `db/dbformat_test.cc`: value-type validation in ParseInternalKey.

## Util

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Util/Coding.cs` | `util/coding.h`, `util/coding_lean.h` | verified. Varint32 5th-byte truncation, overflow and truncation failures, and zigzag all match. Upstream coding_test vectors ported. |
| `Util/Crc32C.cs` | `util/crc32c.{h,cc}` | verified (Mask/Unmask delta `0xa282ead8`, 3-way interleave equals the serial CRC; existing tests cover every length and alignment) |
| `Util/Hash.cs` | `util/hash.{h,cc}`, `util/fastrange.h` | verified. `Hash()` sign-extends the tail bytes, seeds 0xbc9f1d34 / 397, Bijective(Un)Hash2x64 constants match. |
| `Util/HashXxph3.cs` | `util/xxph3.h` | verified by upstream `hash_test.cc` vectors (existing tests) |
| `Util/Comparator.cs`, `Util/Comparators.cs` | `include/rocksdb/comparator.h`, `util/comparator.cc` | verified. Bytewise and reverse separator/successor match line by line, as do IsSameLengthImmediateSuccessor and the u64ts ordering. |
| `Util/ByteCompare.cs` | (memcmp) | verified (sign-equivalent to `SequenceCompareTo`, tested) |
| `Util/PooledBufferWriter.cs` | (std::string buffers) | verified (no format impact) |

## Compression

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Compression/CompressionType.cs` | `include/rocksdb/compression_type.h` | verified (byte values) |
| `Compression/BlockCompression*.cs` | `util/compression.{h,cc}` (BuiltinCompressionManagerV2, StartCompressBlockV2, ExtractUncompressedSize), `block_based_table_builder.cc` (max_compressed_bytes_per_kb capped at 1023) | verified for framing, level sanitization and the LZ4/LZ4HC level mapping. Known deviations: (1) a configured BZip2/Xpress type stores blocks uncompressed, where upstream falls back to the default compressor; (2) LZ4 acceleration > 1 runs with acceleration 1; (3) zlib only compresses blocks of 16 KiB or less (.NET's 32 KiB deflate window); (4) the ZSTD frame checksum option is ignored. All output stays readable by native. |
| `Compression/BlockCompression.Dictionary.cs`, `CompressionDictionary.cs` | `util/compression.{h,cc}` (CompressionDict / UncompressionDict, ZSTD_TrainDictionary / ZSTD_FinalizeDictionary, BuiltinZSTDCompressorV2 / BuiltinLZ4CompressorV2WithDict dictionary paths, Zlib_DecompressBlock) | **ported.** ZSTD with raw, trained (ZDICT_trainFromBuffer) and finalized (ZDICT_finalizeDictionary) dictionaries via ZstdSharp's port of the same zstd: dictionaries and dictionary-compressed blocks are byte-identical to native. Reading LZ4, zlib (dictionary replayed as a stored deflate block) and Snappy (dictionary ignored, as upstream) dictionary tables. Known deviations: LZ4HC with a dictionary compresses with fast LZ4 (same wire format); zlib writes without the dictionary (.NET has no preset-dictionary deflate); both stay readable by native. **Not ported:** parallel compression (`parallel_threads` is recorded only). |

## WAL

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Wal/LogFormat.cs` | `db/log_format.h` | verified (record types 0..11, 130, 131; header sizes 7/11; safe-ignore mask) |
| `Wal/LogWriter.cs` | `db/log_writer.{h,cc}` | verified. The fragmentation loop, the trailer, the legacy vs recyclable header choice (SetCompressionType always uses the legacy header) and the metadata records with MaybeSwitchToNewBlock all match. |
| `Wal/LogReader.cs` | `db/log_reader.{h,cc}` (Reader) | verified. ReadPhysicalRecord, the per-recovery-mode corruption reporting, recycled-log handling, the uncompress loop, UnmarkEOF and predecessor-WAL verification all match. Upstream `log_test.cc` was already ported (47 tests). **Not ported:** XXH3 `record_checksum` output. |
| `Wal/FragmentBufferedLogReader.cs` | `db/log_reader.cc` (FragmentBufferedReader) | verified (record-type switch and recycled-log handling) |
| `Wal/WalMetadataRecords.cs` | `util/compression.h` (CompressionTypeRecord), `util/udt_util.h`, `db/dbformat.h` (PredecessorWALInfo) | verified |
| `Wal/WalStreamingCompression.cs` | `util/compression.cc` (ZSTDStreamingCompress/Uncompress) | verified (`ZSTD_e_end` with the checksum flag on; same output loop condition) |

## Table and filters

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Table/BlockBuilder.cs` | `table/block_based/block_builder.{h,cc}` | verified, including the fv8 common-prefix rewrite and the size estimate. **Not ported:** separated KV storage and the uniformity bit (both off by default). |
| `Table/Block.cs` | `block.cc` (Block ctor), `data_block_footer.cc` | verified (footer bits 28..31, restart validation). A separated-KV block raises NotSupported. |
| `Table/BlockIter.cs` | `block.cc`, `block_util.h` (DecodeEntry / DecodeKeyV4) | verified (3-byte minimum, shared/non-shared bounds, value-delta decoding) |
| `Table/BlockHandle.cs` | `table/format.cc` (BlockHandle, IndexValue incl. fv8 escape) | verified |
| `Table/Footer.cs` | `table/format.{h,cc}` (Footer, FooterBuilder) | verified (fv2..8, extended magic, context checksum, metaindex gap, legacy magic is NotSupported) |
| `Table/BlockChecksum.cs` | `table/format.cc`, `reader_common.cc` | verified (XXH3 last-byte modifier, context modifier) |
| `Table/BlockFetcher.cs` | `table/block_fetcher.cc` | verified (truncated read and checksum mismatch raise Corruption) |
| `Table/DataBlockHashIndex.cs` | `data_block_hash_index.{h,cc}` | verified (sentinels 255/254, odd bucket count, Initialize bounds) |
| `Table/IndexBuilder.cs` | `index_builder.{h,cc}` (ShortenedIndexBuilder, PartitionedIndexBuilder), `flush_block_policy.cc` | verified by the byte-identical index-block compat tests; partitioned indexes (kTwoLevelIndexSearch, metadata_block_size, coupled and decoupled filter partitions) are byte-identical to native SstFileWriter for fv2..8 (partitions, top level, index properties). **Not ported:** hash index. |
| `Table/FilterBlockBuilder.cs`, `PinnedFilterPartitions.cs` | `full_filter_block.cc`, `partitioned_filter_block.cc`, `filter_block_reader_common.cc` | **ported.** Full and partitioned filter builders with prefix extractors (whole key + prefix as "alt", boundary prefixes at partition cuts, keys_per_partition), decouple_partitioned_filters on/off; byte-identical to native. The reader uses partitioned filters (partition index seek, partitions pinned without cache_index_and_filter_blocks, block cache otherwise), RangeMayExist / IsFilterCompatible for prefix seeks. |
| `Table/TableProperties.cs` | `table/table_properties.cc`, `table/meta_blocks.cc` | verified. Property names match, unsorted keys raise Corruption, and malformed u64 values are skipped. Trivial difference: a malformed `rocksdb.deleted.keys` value is not copied to the user properties. |
| `Table/BlockBasedTableBuilder.cs`, `BlockBasedTableReader.cs`, `BlockBasedTableIterator.cs`, `TableFormatOptions.cs`, `BlockCacheStatistics.cs` | `block_based_table_{builder,reader,iterator}.cc` | verified at the format level by the byte-identical SST compat tests (fv2..8 × checksums × compressions). Ported in this round: prefix extractor (`rocksdb.prefix.extractor.name`, prefix filtering property, table extractor recreated from its id when the configured one differs, PrefixExtractorChanged), prefix seek in the table iterator (CheckPrefixMayMatch on Seek/SeekForPrev), point lookups through the prefix filter when whole_key_filtering is off, partitioned index writing and partitioned filter reading/writing, pin_top_level_index_and_filter, dictionary buffering/sampling (kBuffered state, prime-stride sampling, `rocksdb.compression_dict`; like upstream, `num_data_blocks` does not count blocks replayed from the buffer). **Not ported:** user-defined timestamps, property collectors, super_block_alignment, pin_l0_filter_and_index_blocks_in_cache and partition pinning tiers, memtable prefix bloom (`memtable_prefix_bloom_size_ratio`, `memtable_whole_key_filtering` are recorded only), hash index. |
| `Table/Filters/FilterBitsReaders.cs` | `filter_policy.cc` (GetBuiltinFilterBitsReader) | verified. Every reserved or unknown encoding maps to AlwaysTrue, and an empty filter maps to AlwaysFalse. A Ribbon filter shorter than its metadata raises Corruption (undefined behavior upstream). |
| `Table/Filters/BloomImpl.cs`, `FilterBitsBuilders.cs`, `Standard128Ribbon.cs`, `BuiltinFilterPolicies.cs`, `FilterContracts.cs` | `util/bloom_impl.h`, `filter_policy.cc`, `util/ribbon_*.h` | verified by the byte-identical filter compat tests. `optimize_filters_for_memory` (AllocateMaybeRounding, aggregate_rounding_balance_, GetNumProbes after rounding) is ported with a model of the allocator's `malloc_usable_size` (`Filters/FilterAllocationModel.cs`): glibc on Linux (the NuGet librocksdb.so), none elsewhere (native builds without ROCKSDB_MALLOC_USABLE_SIZE). Known deviation: glibc is not fully deterministic (a free chunk up to 24 bytes larger may be returned unsplit; requests of 128 KiB+ depend on mmap vs heap top), so Ribbon filters (16-byte granularity) can occasionally be one rounding step smaller than native's; Bloom filters below 128 KiB match byte for byte. Readers are unaffected. |

## Db: formats, metadata and the memtable

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Db/DbFormat.cs` | `db/dbformat.{h,cc}` | **fixed.** `TryParseInternalKey` accepted every type below `kTypeMaxValid`, including the WAL-only types (LogData, ColumnFamily*, XID markers), and rejected `kTypeMaxValid`. It now implements `IsExtendedValueType` exactly, and `IsValueType` / `IsExtendedValueType` helpers were added. (low) |
| `Db/InternalKey.cs` (InternalKey, LookupKey, IterKey) | `db/dbformat.h`, `db/lookup_key.h` | verified. `InternalKey.Valid` inherits the fix above. |
| `Db/InternalKeyComparator.cs` | `db/dbformat.{h,cc}`, `index_builder.cc` (FindShortestInternalKeySeparator) | verified |
| `Db/VersionEdit.cs` | `db/version_edit.{h,cc}`, `db/wal_edit.cc` | verified. All tags match, as do the NewFile4 custom-tag order, the forward-compat masks (`1<<13` and custom `1<<6`), the signed path_id, the u64 kNewFile seqno defaults and the error messages. `IsValidInternalKey` now delegates to `DbFormat.IsExtendedValueType`. Known deviation: string fields (db id, comparator, CF name) go through UTF-8, so CF names containing invalid UTF-8 bytes would not round-trip byte for byte. |
| `Db/FileMetaData.cs` (+ BlobFileAddition/Garbage, WalAddition) | `db/version_edit.h`, `db/blob/blob_file_{addition,garbage}.cc`, `db/wal_edit.cc` | verified (blob additions/garbage are now applied to versions, see "Integrated BlobDB") |
| `Db/FileNames.cs` | `file/filename.cc`, `util/string_util.cc` (ConsumeDecimalNumber), `db/manifest_ops.cc` | verified (ParseFileName branch for branch, SetCurrentFile protocol, CURRENT parsing). The deviation for an info-log prefix match is documented in the file. |
| `Db/Manifest.cs`, `Db/ManifestLog.cs` | `db/version_edit_handler.cc`, `db/version_set.cc` (AtomicGroupReadBuffer), `db/version_builder.cc`, `db/wal_edit.cc` (WalSet) | verified (atomic groups, CF add/drop, monotonic per-CF log numbers, max min-log, NewestFirstByEpochNumber and BySmallestKey). Comparator/num_levels validation ported (see "User features"). **Not ported:** UDT boundary padding, point-in-time and best-efforts recovery (already in todo). |
| `Db/WriteBatch.cs`, `Db/WriteBatchReader.cs`, `Db/IWriteBatchHandler.cs` | `db/write_batch.cc` | verified. Every tag and error string of ReadRecordFromWriteBatch is covered, along with the count check and TimedPut unpacking. A TimedPut value shorter than 8 bytes raises Corruption (undefined behavior upstream). |
| `Db/MemTable.cs` | `db/memtable.{h,cc}` | **fixed.** Concurrency (medium): the fragmented range-tombstone cache published the list and its version as two separate fields. A reader racing a rebuild could pair the previous list with the new version and miss a tombstone visible to its snapshot (a deleted key could reappear). The list and version are now published together as one immutable object. The regression test is a stress test, so it may not catch the race on every run. Get/SaveValue and ShouldFlushNow were checked and match. |
| `Db/MemTableSupport.cs`, `Db/MemTableRangeTombstones.cs` | `db/column_family.cc` (arena sanitize), `db/range_tombstone_fragmenter.cc` | verified. The fragmenter is a simplified port that keeps every seqno and applies snapshot striping at query time. |
| `Db/GetContext.cs` | `table/get_context.cc` | verified (range-deletion override of value types) |
| `Db/MergeHelper.cs`, `Db/MergeOperator.cs` | `db/merge_helper.{h,cc}`, `db/merge_context.h` | verified. Key choice and type rewrite after a full merge, the PartialMergeMulti fold, compaction-filter skip-until and the range-delete check per operand all match. Wide-column base values ported (TimedFullMergeWithEntity, see "User features"). **Not ported:** OpFailureScope::kMustMerge. Blob base values are fetched through the compaction's blob fetcher (see "Integrated BlobDB"). |
| `Db/MergeHelper.BuiltinOperators.cs` | `utilities/merge_operators/uint64add.cc`, `string_append/stringappend.cc`, `db/merge_operator.cc` | verified (names, malformed values count as 0, ',' delimiter) |
| `Db/RangeDelAggregator.cs` | `db/range_del_aggregator.{h,cc}` (incl. TruncatedRangeDelIterator) | verified (truncation with kTypeMaxValid and seq-1, read and compaction stripes) |
| `Memtable/InlineSkipList.cs` | `memtable/inlineskiplist.h` | verified. Node layout, splice logic and CAS insert match, with acquire/release on links. The random height uses 32-bit xorshift; this only affects performance. |
| `Memory/Arena.cs`, `ConcurrentArena.cs`, `BlockBufferPool.cs` | `memory/arena.{h,cc}`, `memory/concurrent_arena.{h,cc}` | verified (OptimizeBlockSize clamp and alignment) |

## Db: iterators

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Db/Iterators/DbIter.cs` | `db/db_iter.cc` | verified. FindNextUserEntry with the reseek, MergeValuesNewToOld, ReverseToForward / ReverseToBackward, PrevInternal, FindValueForCurrentKey(+UsingSeek), FindUserKeyBeforeSavedKey and bounds clamping were compared line by line, and 14 upstream tests were added. Known deviation: range tombstones use the pre-7.x ShouldDelete-per-entry scheme instead of tombstones in the merging heap; results are the same, but long deleted ranges are slower. Prefix seek ported: prefix_same_as_start (PrefixCheck / ShouldSetPrefix), expect_total_order_inner_iter_ re-seeks on direction changes, max_skip_ disabled for SeekToFirst/SeekToLast in prefix mode, total_order_seek / auto_prefix_mode; `LevelIterator` invalidates past the seek prefix after skipping a file (LevelIterator::Seek). Identical results to native on native- and managed-written DBs (compat tests). Wide-column entities ported (IsEntity / Columns, merges onto entities). Blob values ported (see "Integrated BlobDB"). **Not ported:** UDT, read callbacks, tombstone-run conversion. |

| `Db/Iterators/MergingIterator.cs` | `table/merging_iterator.cc`, `util/heap.h` | verified (SwitchToForward / SwitchToBackward protocol) |
| `Db/Iterators/LevelIterator.cs` | `db/version_set.cc` (LevelIterator) | verified. SeekForPrev before the first file reads file 0 instead of returning early; the result is the same. |
| `Db/Iterators/Iterator.cs`, `DbIteratorFactory.cs`, `InternalIterator.cs`, `EmptyIterator.cs`, `MemTableInternalIterator.cs`, `TableInternalIterator.cs` | `db/arena_wrapped_db_iter.cc`, `table/internal_iterator.h` | verified (wrappers) |

## Options

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Options/ColumnFamilyOptions.cs` | `include/rocksdb/{options,advanced_options}.h`, `db/column_family.cc` | **fixed.** `ArenaBlockSize` defaulted to 1 MiB, while upstream defaults to 0 and derives min(1 MiB, write_buffer_size/8), 4 KiB aligned. With a small `WriteBufferSize` a fresh memtable already asked for a flush: this is the todo item "small WriteBufferSize with default ArenaBlockSize trips Debug.Assert", and in release builds it caused flush churn. The default is now 0, which `MemTableOptions.GetEffectiveArenaBlockSize` already maps to the upstream derivation. (medium) Known deviation: OPTIONS files now carry `arena_block_size=0` (native writes the sanitized value); both sides load it. Other defaults checked: LZ4 compression, level0 20/36 triggers, 64 MiB target file size, 256 MiB level base, dynamic level bytes, max_sequential_skip 8. `ttl` stores the sanitized 30 days instead of the `0xfffffffffffffffe` sentinel. |
| `Options/BlockBasedTableOptions.cs` | `include/rocksdb/table.h` | verified (fv7, XXH3, 4 KiB blocks, restart 16/1, decouple_partitioned_filters, optimize_filters_for_memory, readahead sizes) |
| `Options/DbOptions.cs`, `CompressionOptions.cs`, `OperationOptions.cs`, `IngestExternalFileOptions.cs`, `OptionEnums.cs`, `OptionsBase.cs` | `include/rocksdb/options.h`, `compression_type.h` | verified (spot-checked defaults; enum values) |
| `Options/OptionsFile.cs`, `OptionTypeInfo.cs` | `options/options_parser.cc`, `util/string_util.cc` (Escape/UnescapeOptionString) | verified (comment and escape handling, section rules, duplicate statements keep the first) |

## Cache and Env

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Cache/LruCache.cs` | `cache/lru_cache.{h,cc}`, `cache/sharded_cache.cc` | verified (Release/Erase ref-count transitions, per-shard capacity rounding) |
| `Cache/HyperClockCache.cs` | `cache/clock_cache.{h,cc}` (fixed table) | spot-checked: meta-word layout and the acquire/release counters. Covered by existing tests, not audited line by line. |
| `Cache/CacheKey.cs`, `Cache/BlockCache.cs` | `cache/cache_key.cc`, `include/rocksdb/cache.h` | verified (existing tests) |
| `Env/*` | `env/`, `file/` | reviewed for protocol (rename plus directory fsync, OFD LOCK interop is compat-tested). Not a persisted format. |

## Db: database layer (audit #2)

Audit of the DB layer against `db/db_impl/*.cc`, `db/version_set.cc`, `db/version_builder.cc`, `db/column_family.cc`,
`db/write_thread.cc`, `db/flush_job.cc`, `db/memtable_list.cc`, `db/table_cache.cc`, `db/compaction/*`,
`db/external_sst_file_ingestion_job.cc`, `db/transaction_log_impl.cc`, `db/wal_manager.cc`, `utilities/checkpoint` and
`db/db_impl/db_impl_secondary.cc`. The focus was durability ordering, crash consistency, concurrency and semantics
rather than formats (audit #1). Regression tests are in `tests/RocksDb.Managed.Tests/Db/DbLayerAuditTests.cs` and
`Db/DbCrashConsistencyTests.cs` unless noted.

New test infrastructure:

- `Db/TestHooks.cs` (src): a lite TEST_SYNC_POINT. Named callbacks, invoked only in DEBUG builds
  (`[Conditional("DEBUG")]`), at `DBImpl::RunCompaction:Start` / `:BeforeLogAndApply`,
  `DBImpl::FlushMemTables:Start` / `:BeforeLogAndApply`, `VersionSet::LogAndApply:BeforeManifestWrite` and
  `DBImpl::GetImpl:AfterSuperVersion`.
- `tests/.../Db/FaultInjectionFileSystem.cs`: wraps `MemoryFileSystem`. It fails the N-th mutating operation (and
  every later one, or only that one), optionally with a torn append, can fail the next append to a named file, and
  counts open file handles and locks for leak checks.
- `DbCrashConsistencyTests`: a deterministic workload (puts, deletes, range deletes, sync writes, manual flushes,
  background flushes and compactions, two column families) crashed at about 300 operation indices. It covers power
  loss (unsynced data and directory entries dropped) and process kill with torn writes. After each crash the test
  checks that the DB reopens, that synced (or, for a kill, all acknowledged) writes survive, that the state equals
  some prefix of the issued writes in both CFs, that Get and the iterator agree, and that the DB keeps working
  afterwards. It also asserts that no file handle or LOCK leaks after a failed close or a failed open.
- `ConcurrentStressWithInvariants` runs 2 x 2.5 s. Writers (async and batched) overwrite keys while readers do Get,
  snapshot re-reads and full scans, a thread runs flushes and CompactRange, and another creates, flushes and drops
  column families while one of their iterators is open. It checks that a key is never missing or mis-valued, that
  a snapshot's view does not change, that iterator counts are right, that no background error occurs, and that
  everything survives a reopen.

| Area (managed) | C++ source(s) | Status |
|---|---|---|
| `VersionSet.LogAndApplyAsync`: edits for dropped column families | `version_set.cc` ProcessManifestWrites / LogAndApply (`IsDropped` -> ColumnFamilyDropped) | **fixed (high).** A flush or compaction that checked `Dropped` before calling LogAndApply could append its edit after the drop record. The next open then failed with "MANIFEST record referencing unknown column family", making the DB unopenable. LogAndApply now rejects the edit under the MANIFEST lock, which also serializes drops. |
| `VersionSet.LogAndApplyAsync`: failed MANIFEST write | ProcessManifestWrites (`descriptor_log_.reset()`, keep the new MANIFEST when only the CURRENT update failed) | **fixed (high).** After a failed, possibly torn, append, later edits were appended to the same MANIFEST behind the torn record, and recovery dropped them (a column family created afterwards was lost). The next edit now rolls to a new MANIFEST. If writing a new MANIFEST succeeded but the CURRENT update failed, that MANIFEST is no longer deleted, since CURRENT may already name it. |
| `VersionSet` last_sequence in edits | LogAndApplyHelper (`max_last_sequence` / `descriptor_last_sequence_`) | **fixed (medium, with ingestion below).** The recorded last sequence never decreases and covers the largest seqno of the added files. |
| `DbImpl.Ingest` sequence publication | `DBImpl::IngestExternalFiles` (SetLastSequence after LogAndApply) | **fixed (medium).** The last sequence was published before the MANIFEST write, so a snapshot taken during that window had the ingested seqno but did not see the files, and its view changed once they were installed. It is now published together with the super version install. |
| `DbImpl.Read` Get / GetAsync / NewIterator | `DBImpl::GetImpl`, `NewIteratorImpl` (reference SV, then read the sequence) | **fixed (medium).** The read sequence was taken before the super version. A flush followed by a (non-bottommost) compaction in between could drop the version visible at that sequence, so a Get returned an older, overwritten value. The deterministic test reproduces this: it returned `v0` after `v1` and `v2` were written. Snapshot validation now also happens before the reference, so a bad snapshot no longer leaks a super version reference. |
| Drop CF during flush | `DBImpl::DropColumnFamilyImpl`, MemTableList refs, FlushJob | **fixed (medium).** Drop released the CF's immutable memtables while a flush still read them (arena freed under the reader). It also disposed the CF's table cache under readers, iterators and compactions that still held its versions. The flush also unreferenced the memtables a second time, which tripped an assert in debug builds. Now the flush holds its own memtable references, a drop that commits right after the flush's MANIFEST write is detected, and dropped CFs keep their table cache until close (their files are evicted as they become obsolete). |
| `DbImpl.Open` WAL replay, point-in-time recovery | `RecoverLogFiles` / `MaybeReviseStopReplayForCorruption` / `MaybeHandleStopReplayForCorruptionForInconsistency` | **fixed (high).** After a corrupted record, PIT recovery stopped reading every later WAL. Upstream keeps reading and resumes when a record continues the sequence, which is the case for writes made after an earlier PIT recovery with `avoid_flush_during_recovery`. Those synced writes were lost on the next restart. An IO error is now fatal in PIT mode, and the "SST file contains data beyond the point of corruption" consistency check was added. Read-only open follows the same rules. |
| `DbImpl.Write` group commit: cancellation | WriteImpl (no cancellation) | **fixed (medium).** The leader passed its own CancellationToken to the WAL append and sync. Cancelling the leader's call could abandon a half-written group record in the middle of the WAL (subsequent records then follow a torn one) and failed followers that never asked to be cancelled. WAL I/O is no longer cancellable once the group is committed, and FlushWAL is likewise not cancellable. |
| `DbImpl.Write` SwitchMemtable failure | `DBImpl::SwitchMemtable` (SetBGError on failure) | **fixed (medium).** A failure creating the new WAL or syncing the old one leaked the new WAL's file handle and left writes enabled. The handle is now closed, the file deleted, and a background error set. Found by the crash sweep's handle-leak check. |
| Close vs queued writers / unbatched operations | `DBImpl::CloseHelper`, WriteThread | **fixed (medium).** A writer or flush queued behind close became leader after close. Writers failed with a NullReferenceException wrapped as IOError (and set a background error); a queued flush could create a new WAL after the LOCK was released. Both now fail with ObjectDisposedException. |
| `DbImpl.Open` column family options | `SanitizeCfOptions` (db/column_family.cc) | **fixed (medium).** Out-of-range options were used as given. They are now sanitized like upstream: write_buffer_size clipped to [64 KiB, 64 GiB], max_write_buffer_number >= 2, min_write_buffer_number_to_merge, num_levels (>= 2 for leveled, >= 3 for universal with ingest-behind), level0 trigger ordering (stop >= slowdown >= compaction trigger), FIFO L0 triggers, max_bytes_for_level_multiplier, soft/hard pending-compaction limits, dynamic level bytes only for leveled with one path, and max_compaction_bytes = 25 x target. The OPTIONS file records the sanitized values, like native. Two existing stall tests relied on unsanitized triggers and now block compaction with a test hook instead. Not ported: memtable factory and ttl/periodic adjustments (the managed defaults are already the sanitized values). |
| WAL sync, MANIFEST + CURRENT ordering | `DBImpl::WriteToWAL`, `SyncWAL`, `SetCurrentFile`, `ProcessManifestWrites` | verified. The WAL is synced (plus a directory fsync for a new WAL) before a sync write is acknowledged. Unlike upstream, the managed SwitchMemtable syncs the old WAL eagerly instead of on the next sync write (stricter). A MANIFEST edit is synced before its version is installed. A new MANIFEST is fully written and synced, then CURRENT is written to a temp file, synced, renamed, and the directory fsynced. Table files are synced, and their directory fsynced, before the MANIFEST edit that adds them. |
| Obsolete file deletion | `FindObsoleteFiles` / `PurgeObsoleteFiles` | verified. Table files become obsolete only when the last version referencing them is released, which happens after the MANIFEST edit that dropped them is durable and every reader (super version, iterator, compaction input version) is gone. A file moved trivially is referenced before the old version is released. WALs are deleted only below the minimum log number of all live CFs, never the current WAL. MANIFESTs are deleted only after the new one is installed. Deletions are deferred while `DisableFileDeletions` is in effect (checkpoints). |
| `min_log_number_to_keep` (multi-CF, non-2PC) | `PrecomputeMinLogNumberToKeepNon2PC`, SwitchMemtable empty-CF log number bump | verified. Matches upstream, including the in-memory-only log number advance of empty CFs. Covered by the two-CF crash sweep. |
| Sequence publication vs memtable visibility | WriteImpl (non-pipelined), `SetLastSequence` | verified. Sequences are allocated before the WAL write and published only after every memtable insert of the group, so readers never see a partial group. After recovery the last sequence is the maximum of the MANIFEST value and the last replayed record. |
| Torn MANIFEST / WAL tails | `log::Reader`, VersionSet::Recover | verified by the process-kill sweep (torn appends at about 120 points). |
| `WriteThread` | `db/write_thread.cc` | verified. Grouping rules match: no sync write under a non-sync leader, no mixing WAL-disabled or no_slowdown writers, size cap of leader size + 128 KiB. Leader handoff and pooled writers are correct. Known deviation: a leader that is cancelled while delayed by a write stall fails its whole group with OperationCanceledException (upstream has no cancellation). |
| `FlushJob`, recovery flush | `flush_job.cc`, `builder.cc`, `WriteLevel0TableForRecovery` | verified. Known deviation (from audit #1): no entries are dropped at flush time. Recovery writes the L0 file and the new log number in one edit per CF; a crash between CFs is handled because each CF skips WALs below its own log number. |
| `TableCache` | `db/table_cache.cc` | verified. Ref-counted handles: eviction and Evict on deletion close a reader only after the last lease is returned. Failed opens are not cached. |
| `GetFromFile` unclipped tombstones | `TableCache::Get` | verified. Upstream also applies a file's tombstones unclipped for keys inside the file's range. Compaction outputs and the iterator/compaction aggregators truncate (`CompactionJob.CollectTruncated` at user-key granularity, which is equivalent for outputs because the point keys at the boundary are resolved by the seq-1 truncated aggregator). |
| Compaction | `compaction_job.cc`, `compaction_outputs.cc`, `compaction_iterator.cc` | spot-checked: snapshots captured at pick time, input version referenced, outputs pending until installed and deleted on failure, edits refused for dropped CFs (above). Not re-audited line by line: the CompactionIterator rules (covered by existing tests). |
| `ExternalSstFileIngestionJob` | `external_sst_file_ingestion_job.cc` | verified: level and seqno assignment (overlap means a new seqno; L0 for overlapping files), memtable overlap check with blocking flush, writes stopped during Run. Sequence publication was fixed (above). |
| Checkpoint | `checkpoint_impl.cc`, `db_filesnapshot.cc` | verified: file deletions are disabled during the copy, the MANIFEST is trimmed to the installed size, CURRENT is replacement contents, the last WAL is trimmed, and the tmp directory is renamed with directory fsyncs. |

Remaining risks:

- The read-sequence race is fixed in DbImpl.Read.cs, which is currently the only place that computes a read
  sequence. New read paths (for example a MultiGet batch, or prefix-seek iterators) must keep the same order:
  super version first, then the sequence.
- Close does not fsync the WAL (like upstream). Unsynced writes acknowledged before a close can be lost on power loss.
- No automatic recovery from background errors (ErrorHandler auto-resume). After a failed flush or MANIFEST write
  the DB stays read-only until it is reopened.
- Secondary instances re-read the MANIFEST in full on every catch-up.

## Transactions (utilities/transactions)

Ported in `src/RocksDb.Managed/Utilities/Transactions/` with small hooks in `Db/` (`TransactionWriteContext.cs`,
`DbImpl.Transactions.cs`, the write group leader and `MemTableInserter` in `DbImpl.Write.cs`, WAL recovery in
`DbImpl.Open.cs`, the 2PC minimum WAL in `DbImpl.Flush.cs` / `DbImpl.cs`). Tests:
`tests/RocksDb.Managed.Tests/Transactions/` (ports of optimistic_transaction_test.cc and transaction_test.cc, 49 cases)
and `tests/RocksDb.Managed.CompatTests/Api/TransactionCompatTests.cs` (native TransactionDB through the C API).

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Db/TransactionWriteContext.cs`, leader in `DbImpl.Write.cs` | `DBImpl::WriteImpl` (callback, disable_memtable, log_ref, log_used, pre-release callback), `WriteThread::EnterAsBatchGroupLeader`, `WriteCommittedTxn::CommitInternal` (WAL termination point) | verified. Write callbacks run after PreprocessWrite and before sequence allocation; a failed callback excludes its writer from the WAL record and the memtables. Writers whose callback disallows batching are not grouped behind another leader. A prepare writes the WAL only and consumes no sequence numbers; the WAL that received it is marked as holding a prepared section before the write is released (MarkLogCallback). The commit of a prepared transaction writes `[Commit(xid)]` to the WAL and inserts the prepared batch into the memtables at the same sequence numbers (modelled as a separate memtable batch instead of a WAL termination point; the WAL bytes are identical). |
| `MemTableInserter` 2PC markers | `MemTableInserter::MarkBeginPrepare/MarkEndPrepare/MarkCommit/MarkRollback/MarkNoop` (db/write_batch.cc) | verified (write-after-commit only). Live writes ignore the markers. Recovery rebuilds each prepared section (no memtable insert, no sequence numbers), applies it at its commit marker with `log_number_ref` = prepare WAL, drops it at a rollback marker, and fails with NotSupported ("Open with TransactionDB::Open()") without allow_2pc. Unprepared (WriteUnprepared) sections are refused. |
| `DbImpl.Open` WAL replay with 2PC | `RecoverLogFiles`, `SetupLogFilesRecovery`, `InsertLogRecordToMemtable` (next_sequence), `SanitizeOptions` | **fixed (high; reachable once 2PC markers are replayed).** The recovered last sequence came from the record header (seq + count - 1). A prepare record counts its entries but consumes no sequence numbers and a commit record has count 0 but consumes the prepared batch's, so the header under-estimated the last sequence after a commit and the next writes would have reused sequence numbers. It now comes from the inserter (next_sequence), also for point-in-time resumption and secondary catch-up. With allow_2pc replay starts at the persisted min_log_number_to_keep (WALs below the CF log numbers may hold prepared sections) and the recovered memtables are always flushed (avoid_flush_during_recovery is ignored, like upstream). |
| `DbImpl.Transactions.cs` prep tracking | `LogsWithPrepTracker`, `FindMinPrepLogReferencedByMemTable`, `PrecomputeMinLogNumberToKeep2PC`, `MemTable::RefLogContainingPrepSection` | verified. WAL deletion (purge, full scan, checkpoint WAL set) keeps WALs with outstanding prepared sections and the prepare WALs of committed data still in memtables; 2PC flushes always persist min_log_number_to_keep. Known deviation: deletion uses the live minimum, which can be newer than the persisted value; this is safe because a WAL is only dropped once no prepared section and no unflushed data needs it. |
| Memtable history | `MemTableList` history, `TrimHistory`, max_write_buffer_size_to_maintain | partial. Flushed memtables are kept (oldest first, referenced) while mutable + immutable + history memory stays below max_write_buffer_size_to_maintain; trimming runs at flush completion and memtable switch (upstream also trims on inserts through the trim-history scheduler). Only conflict checks read the history (as upstream). |
| `DbImpl.Transactions.cs` conflict checks | `TransactionUtil::CheckKey/CheckKeysForConflicts`, `DBImpl::GetEarliestMemTableSequenceNumber`, `GetLatestSequenceForKey` | verified. Busy on a write after the tracked sequence, TryAgain when cache_only and the memtables (with history) do not reach back to it. The table-file lookup records the newest visible entry or covering tombstone (the max of the two is used, which can only report more conflicts). |
| `OptimisticTransactionDatabase.cs` | `optimistic_transaction.cc`, `optimistic_transaction_db_impl.cc` | verified. kValidateSerial runs the check as a non-batching write callback in the group; kValidateParallel (default) takes striped bucket locks in ascending order (async waits) and checks before writing. History is enabled at open. |
| `PessimisticTransaction.cs`, `TransactionDatabase.cs` | `pessimistic_transaction.cc` (WriteCommittedTxn), `pessimistic_transaction_db.cc`, `transaction_base.cc` | verified: TryLock (lock upgrade, snapshot validation after locking, unlock on failed validation), state machine incl. expiration / lock stealing, SetName rules, Prepare / Commit / Rollback (prepared, started, committed), savepoints (unlocking keys locked since the save point), UndoGetForUpdate, direct DB writes wrapped in internal transactions with default_lock_timeout, `Write` locking sorted keys, recovered prepared transactions rebuilt with skip_concurrency_control. Transaction names are written as UTF-8 bytes. Not ported: WritePrepared / WriteUnprepared policies, commit-time batches, user-defined timestamps, range locks, commit_bypass_memtable, secondary indices. |
| `PointLockManager.cs` | `lock/point/point_lock_manager.cc` (PointLockManager, not PerKeyPointLockManager) | verified: striped lock maps, shared/exclusive locks, lock limits, expiration and stealing, NotifyAll wake-up on unlock, deadlock detection with the same BFS and path order, bounded deadlock buffer. Waiting is asynchronous (TaskCompletionSource per wait) for async callers and blocks only synchronous callers. |
| `IndexedWriteBatch.cs`, `TransactionIterator.cs` | `write_batch_with_index.cc` (overwrite_key), `BaseDeltaIterator` | consolidated: thin adapters over the public `WriteBatchWithIndex` (overwrite_key mode, 2PC Noop placeholder) and `BaseDeltaIterator` (see "User features"). |

Compat (native librocksdb 11.12 via the C API): native prepared / committed / rolled back 2PC transactions (including
one left prepared at close) recover in the managed TransactionDB and vice versa; the prepare, commit and rollback WAL
records written by both are byte-identical; managed optimistic commits are readable natively.

Remaining risks: a crashed test instance keeps running its background jobs against `MemoryFileSystem` after
`SimulateCrash()` (tests must quiesce compactions first, see `TransactionTests.Crash`); lock-table entries allocate per
locked key (about 150 B) and the parallel validator creates its bucket semaphores lazily.

## User features (WriteBatchWithIndex, dynamic options, comparators, properties, statistics, LOG, wide columns)

Tests: `tests/RocksDb.Managed.Tests/Features/` (unit) and `tests/RocksDb.Managed.CompatTests/Api/{CustomComparator,
PropertiesStatistics,SetOptions,WideColumn,WriteBatchWithIndex}CompatTests.cs` (native librocksdb 11.12).

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Utilities/WriteBatchWithIndex/*` (`WriteBatchWithIndex`, `WriteBatchEntryIndex`, `WriteBatchWithIndexIterator`, `BaseDeltaIterator`) | `utilities/write_batch_with_index/write_batch_with_index{,_internal}.{h,cc}` | **ported.** Skip list over batch offsets ordered by (cf, key by the CF comparator, offset descending) in reusable struct-of-arrays nodes; overwrite_key (Put/Delete/SingleDelete/PutEntity update the newest entry, merges always add); GetFromBatch (MergeInProgress when only merges), GetEntityFromBatch, GetFromBatchAndDB (MergeAcrossBatchAndDB with plain or entity base), WBWIIterator with iterate bounds (Seek/SeekForPrev/SeekToFirst/SeekToLast clamping, NextKey/PrevKey, FindLatestUpdate) and BaseDeltaIterator (direction changes, deletes, merges onto base / batch Put / PutEntity / Delete, entities, columns) line by line; savepoint rollback rebuilds the index. Compat: the batch rep is byte-identical to native WBWI `rocksdb_writebatch_wi_data` for random Put/Delete/SingleDelete/Merge sequences on two CFs (both overwrite modes, also after RollbackToSavePoint), and GetFromBatchAndDB / NewIteratorWithBase results equal native on the same DB. Transactions now use it. Deviations: DeleteRange throws NotSupported (as upstream); no WBWIMemTable ingestion, MultiGetFromBatchAndDB, timestamps, allow_unprepared_value. |
| `ColumnFamilyOptions.Comparator`, `Db/DbImpl.Comparators.cs`, `TableFactory.ResolveComparator` | `VersionEditHandler` comparator check, `VersionBuilder::CheckConsistencyForNumLevels` | **ported.** The comparator object is primary (`CustomizableOption`, `ComparatorName` derived and persisted; built-ins recreated from OPTIONS files, unknown names kept). Custom comparators work for open/create/recover/flush/compaction/iterators/read-only/secondary (MANIFEST file ordering uses the configured comparators). A MANIFEST comparator name different from the configured one fails with InvalidArgument "X: does not match existing comparator Y"; files on levels >= num_levels fail with "db has more levels than options.num_levels". Compat: native DBs with a custom comparator / a comparator named `rocksdb.ReverseBytewiseComparator` round-trip with the managed port (flush, compaction, WAL tail on both sides); mismatches fail on both sides. |
| `Db/DbImpl.Options.cs`, `ColumnFamily.Options.cs` | `DBImpl::SetOptions` / `SetDBOptions`, `cf_mutable_options_type_info`, `db_mutable_options_type_info` | **ported.** Empty input, unknown names ("Unrecognized option") and immutable options ("Option not changeable") fail with InvalidArgument and change nothing; values are validated on a copy (with the open-time sanitization), applied under the DB mutex, derived state refreshed (memtable size, compaction score, write stall, flush job limit, delayed write rate, stats dump timer), a new super version installed, flushes/compactions rescheduled and a new OPTIONS file written. bytes_per_sync = 0 is stored as 1 MiB like upstream. Typed overloads take `Action<ColumnFamilyOptions>` / `Action<DbOptions>`. Compat: the same changes give the same OPTIONS-file values as native, with the same accept/reject decisions. Known deviations: readers see the live options object (no MutableCFOptions snapshot per super version); table factory options (`block_based_table_factory`) are not mutable; max_total_wal_size and the *_bytes_per_sync options are recorded only. |
| `Db/DbImpl.Properties.cs`, `ColumnFamily.Properties.cs`, `Core/DbProperties.cs`, `Monitoring/InternalStats.cs` | `db/internal_stats.cc`, `DBImpl::KeyMayExist`, `GetApproximateSizes`, `GetApproximateMemTableStats`, `VersionSet::ApproximateSize/OffsetOf`, `BlockBasedTable::ApproximateOffsetOf/Size`, `Version::GetPropertiesOfAllTables`, `VersionStorageInfo::EstimateLiveDataSize`, `Version::DebugString`, `TableProperties::ToString/Add` | **ported.** KeyMayExist (memtables resolved fully, then only table filters: filter blocks may be read where upstream's kBlockCacheTier answers "may exist" on a cache miss); approximate sizes are identical to native on the same files (compat); GetIntProperty / GetMapProperty / GetAggregatedIntProperty; properties estimate-live-data-size, cur/size-all-mem-tables, block-cache-usage/capacity/pinned-usage, num-live-versions, oldest-snapshot-time/sequence, cfstats (upstream level table format and map keys), dbstats, sstables, aggregated-table-properties[-at-level N] (the aggregatable prefix of the native string, compat-checked), options-statistics. Deviations: cfstats lacks the file-read latency histograms and interval-vs-cumulative separation (interval = cumulative); CPU time equals wall time; size-all-mem-tables equals cur-size (flushed memtables are not pinned). |
| `Monitoring/Statistics.cs`, `StatisticsNames.g.cs` | `include/rocksdb/statistics.h`, `monitoring/statistics.cc`, `monitoring/histogram.cc` | **ported.** All upstream tickers/histograms (names generated from TickersNameMap/HistogramsNameMap of the C++ tree), StatsLevel with upstream values, per-processor striped long counters and histogram buckets (HistogramBucketMapper, Percentile, Data, ToString), `ToString` in native format (compat: same line formats, common names in the same order). Recorded: block cache hit/miss/add per type, bloom useful/full positive/prefix (from the table readers), memtable hit/miss, keys/bytes read and written, write done by self/other, WAL bytes/syncs, iterator seek/next/prev (+found, bytes), iterators created/deleted, flush/compaction bytes, compaction key drops, stall micros, DB_GET/DB_WRITE/DB_SEEK/FLUSH_TIME/COMPACTION_TIME/WRITE_STALL/BYTES_PER_READ/WRITE histograms. Off by default (null). Not recorded: per-level GET_HIT_*, file I/O histograms, compression timers, async iterator calls. |
| `Logging/InfoLogger.cs`, `Logging/FileInfoLogger.cs`, `Db/DbImpl.Monitoring.cs` | `env/env.cc` (Logger::Logv), `logging/env_logger.h`, `logging/auto_roll_logger.cc`, `CreateLoggerFromOptions`, `DBImpl::DumpStats`, `db/event_helpers.cc` | **ported.** LOG file in the DB dir (or db_log_dir with the flattened-path prefix) with the native line format, LOG.old.&lt;micros&gt; rotation at open and by max_log_file_size / log_file_time_to_roll (headers replayed), keep_log_file_num trimming, info_log_level filtering with level prefixes, WARN+ flushes; written by a background task (callers never do file I/O). Open summary + options dump, flush/compaction EVENT_LOG_v1 records, shutdown messages, periodic stats dump (stats_dump_period_sec, also adjustable with SetDBOptions). `DbOptions.InfoLog` replaces the file with a user logger. Compat: native and managed rotate each other's LOG. Read-only/secondary instances only log to a user logger. |
| `Db/Wide/*`, `ColumnFamily.Entities.cs`, wide-column paths in `GetContext`, `DbIter`, `MergeHelper`, `MemTableInserter` | `db/wide/wide_column_serialization.cc`, `wide_columns.cc`, `DBImpl::PutEntity/GetEntity`, `GetContext::SaveValue`, `DBIter::SetValueAndColumnsFromEntity`, `MergeWithWideColumnBaseValue`, `MergeHelper` (TimedFullMergeWithEntity) | **ported.** V1 serialization (byte-identical to the C++ test vectors), V1/V2 deserialization with all corruption checks (V2 blob columns: NotSupported), PutEntity (sorted columns, duplicate names rejected), GetEntity (plain values as a default column), Get of an entity returns the default column, iterator `IsEntity` / `Columns` (forward, reverse, seek-based paths), merges onto entities in Get, iterators and compaction (result stays an entity with the merged default column; a missing default column merges as an existing empty value, like FullMergeV3), WAL replay, flush and compaction. Compat: entities written by managed are read, merged and compacted by native (Get returns the default column) and native-stored entities (written through a WriteBatch rep, as the 11.12 C API has no PutEntity) are read by managed across WAL, L0 and compacted files. Not ported: compaction filters on entities (entities are kept unfiltered), blob-backed columns, attribute groups, MultiGetEntity, TTL DBs. |

Fixed while doing this: `FlushColumnFamilyAsync(wait)` could wait forever when the DB closed after the memtable switch
(no flush is scheduled once shutdown starts); it now fails with ObjectDisposedException and close wakes waiters (the
same fix was made independently on the port branch).

## Integrated BlobDB (blob files)

Ported in `src/RocksDb.Managed/Db/Blob/` with small, isolated hooks in the read paths (`GetContext`, `DbImpl.Read.cs`,
`DbImpl.MultiGet.cs`, `DbIter`, `Iterator.ScanAsync`), flush (`FlushJob`, `DbImpl.Flush.cs`, recovery flush in
`DbImpl.Open.cs`), compaction (`CompactionIterator.PrepareOutput` / `InvokeFilterIfNeeded`, `CompactionJob`,
`MergeHelper.MergeUntil`), the version set (`Version.cs`, `VersionSet.cs`) and obsolete-file deletion (`DbImpl.cs`).
Tests: `tests/RocksDb.Managed.Tests/Blob/` (ports of key cases of blob_file_builder_test.cc, blob_file_reader_test.cc,
db_blob_basic_test.cc, db_blob_compaction_test.cc, db_blob_index_test.cc; 28 cases),
`tests/RocksDb.Managed.CompatTests/Api/BlobCompatTests.cs` (native librocksdb 11.12, blob options through the C API) and
two round-trip scenarios (`FeatureRoundTripTests.BlobDb_BothWays`, native first and managed first).

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Db/Blob/BlobLogFormat.cs` | `db/blob/blob_log_format.{h,cc}` | **ported.** Header (30 bytes), record header (32 bytes, masked crc32c of the first 24 bytes and of key + value), footer (32 bytes, masked crc32c), magic 0x00248f37, version 1, IsValidBlobOffset; every Corruption message. |
| `Db/Blob/BlobIndex.cs` | `db/blob/blob_index.h` | **ported.** kBlob / kBlobTTL / kInlinedTTL encode and decode with the upstream error messages. TTL blob references (stacked BlobDB) raise Corruption when read ("Unexpected TTL blob file"); inlined TTL values are returned as values. |
| `Db/Blob/BlobFileBuilder.cs` | `db/blob/blob_file_builder.cc`, `blob_log_writer.cc`, `util/compression.cc` (LegacyForceBuiltinCompression) | **ported.** Lazy open, min_blob_size (empty values never extracted), blob_file_size rollover, per-blob compression with format-2 framing kept even when larger, footer + fsync on close, BlobFileAddition (no file checksum: empty method/value, as without a checksum factory), prepopulate_blob_cache (flush only). Flushed blob files are **byte-identical** to native for none / snappy / lz4 / zstd (compat test, several files per flush). Known deviation: zlib blobs above 16 KiB fail with NotSupported (.NET deflate window, see Compression). |
| `Db/Blob/BlobFileReader.cs` | `db/blob/blob_file_reader.cc` | **ported.** Header/footer validation (magic, version, TTL, column family id, footer CRC, malformed size), offset and compression checks, record + CRC verification when verify_checksums (key, sizes, blob CRC), decompression errors as Corruption, sync and async reads. Not ported: prefetch buffers, multi-blob batched reads (MultiGetBlob reads blob by blob). |
| `Db/Blob/BlobSource.cs` | `db/blob/blob_source.cc`, `blob_file_cache.cc`, `Version::GetBlob` | **ported.** Optional blob cache (`ColumnFamilyOptions.BlobCache`, any `BlockCache`, bottom priority, OffsetableCacheKey(db_id, session, file).WithOffset(offset)), fill_cache, kBlockCacheTier Incomplete, "Invalid blob file number" for files not in the reading version. Known deviation: blob file readers are not bounded by max_open_files (one per live blob file, closed when the file becomes obsolete). |
| `Db/Blob/BlobFileMetaData.cs`, `Version.cs`, `VersionSet.cs`, `Manifest.cs` | `db/blob/blob_file_meta.{h,cc}`, `db/version_builder.cc` (ApplyBlobFileAddition/Garbage, LinkSst, GetMinOldestBlobFileNumber, SaveBlobFilesTo / AddBlobFileIfNeeded), `VersionStorageInfo::GetBlobStats` | **ported.** Blob files are part of every version (linked SSTs from oldest_blob_file_number, garbage counters); blob files older than the oldest one with linked SSTs, and unlinked fully-garbage files, are dropped exactly like upstream; duplicate additions, unknown files and garbage overflow raise Corruption. New MANIFESTs (WriteCurrentStateToManifest) carry the blob files. **Fixed (high):** previously a managed MANIFEST rewrite of a native blob DB dropped all blob file records. |
| Obsolete blob files (`VersionSet`, `DbImpl.PurgeObsoleteFiles`, full scan) | SharedBlobFileMetaData deleter, `VersionSet::AddObsoleteBlobFile`, `FindObsoleteFiles` | **ported.** Reference counted per version like table files: a blob file is deleted (and its reader evicted) only when no live version (super version, iterator, compaction input) references it; pending blob outputs of running jobs are protected from the open-time scan; failed jobs delete their blob files unless their MANIFEST write was attempted. |
| `Db/Blob/CompactionBlobProcessor.cs`, `CompactionIterator`, `CompactionJob` | `compaction_iterator.cc` (ExtractLargeValueIfNeeded, GarbageCollectBlobIfNeeded, ComputeBlobGarbageCollectionCutoffFileNumber, blob values for the compaction filter), `blob_fetcher.cc`, `compaction_job.cc` | **ported.** Extraction for outputs at levels >= blob_file_starting_level, relocation of blobs older than the age cutoff (CompactRangeOptions blob_garbage_collection_policy / age cutoff overrides), compaction filters on blob values (the blob is read; FilterBlobByKey is not ported), merges onto blob base values in compaction. |
| `Db/Blob/BlobGarbageMeter.cs` | `db/blob/blob_garbage_meter.cc`, `blob_counting_iterator.h` | **ported.** Inflow over every positioned input entry, outflow over outputs, BlobFileGarbage per preexisting file; the garbage recorded by managed and native for the same compaction feeds identical metadata (compat test: native GC on managed garbage and vice versa). |
| Forced GC (`VersionStorageInfo.Compaction.cs`, `LevelCompactionPicker`) | `VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC`, `LevelCompactionBuilder` (kForcedBlobGC) | **ported** for leveled compaction. Not ported for universal/FIFO (as upstream: only the leveled picker consumes the marks). |
| Read paths (`GetContext`, `DbImpl.Read`, `DbImpl.MultiGet`, `DbIter`) | `GetContext::SaveValue` / GetBlobValue, `DBIter::SetValueAndColumnsFromBlob` | **ported.** Blob indexes (from tables or the memtable) are resolved while the super version is referenced (the audit rule: super version before sequence still holds; blob files are pinned by the same version). Merges onto blob base values in Get, MultiGet, iterators (both directions, the seek-based reverse path) and compaction. Iterators read the blob lazily on the first `Value` access (allow_unprepared_value semantics; `PrepareValueAsync` and `ScanAsync` read it with async I/O). Known deviation: an iterator positioned on a blob whose read fails throws from `Value` instead of being invalidated during positioning. KeyMayExist does not read blobs (value_found = false). |
| WAL replay, WriteBatch, replication, checkpoint | `MemTableInserter::PutBlobIndexCF`, `DBImpl::GetLiveFiles`, `GetLiveFilesStorageInfo` | **ported.** kTypeBlobIndex WAL records (blob direct write / legacy) are replayed into the memtable and flushed as blob indexes; GetLiveFiles and checkpoints include the blob files of the current versions; GetUpdatesSince returns PutBlobIndex records unchanged. |
| Properties and statistics | `internal_stats.cc` (num-blob-files, blob-stats, total-blob-file-size, live-blob-file-size, live-blob-file-garbage-size, blob-cache-*) | **ported.** Flush and compaction write bytes include blob files (bytes_written_blob). Tickers: BLOB_DB_BLOB_FILE_BYTES_WRITTEN/READ, BLOB_DB_BLOB_FILE_SYNCED, BLOB_DB_CACHE_*. Not ported: blob read/compression histograms, perf context, the blob fields of compaction/flush job info. |
| Options | `options/cf_options.cc` | **ported** (already persisted): enable_blob_files, min_blob_size, blob_file_size, blob_compression_type, enable_blob_garbage_collection, blob_garbage_collection_age_cutoff / force_threshold, blob_compaction_readahead_size (recorded only, no readahead), blob_file_starting_level, prepopulate_blob_cache; `BlobCache` (not serialized, like upstream). Not ported: blob_compression_opts (default level), blob_file_writable_file_max_buffer_size, blob direct write (11.13+ write path; the replayed WAL records are supported). |
| Wide columns | `wide_column_serialization.cc` (V2 blob columns) | **not ported:** entities with blob-backed columns (V2, written only by newer releases with blob direct write) still raise NotSupported; entities are never extracted to blob files. |

Compat (native librocksdb 11.12): a native blob DB (two CFs, merges, deletes, several blob files per flush, compaction
with forced GC, a WAL-only tail) is read by managed (Get, async Get, MultiGet, both iteration directions), extended,
compacted with GC and handed back to native, which verifies and compacts again; a managed blob DB (garbage recorded
without GC) is read and garbage-collected by native and read back; flushed blob files are byte-identical for none,
snappy, lz4 and zstd. Round trip: `BlobDb_BothWays` (generated op streams, blob CFs with and without compression/GC,
compactions, checkpoint, reopen) passes with either engine first.

Remaining risks: blob reads in the async Get/MultiGet/scan paths complete synchronously on a blob-cache hit and use
async file reads otherwise, but iterators resolving blobs through `Value` do synchronous I/O; blob file readers are not
limited by max_open_files.

## Findings in out-of-scope files (for the coordinator)

1. `Db/ColumnFamilyData.cs` passes `ColumnFamilyOptions.ArenaBlockSize` straight into `MemTableOptions`. With the new
   default of 0, the derivation happens in `MemTableOptions.GetEffectiveArenaBlockSize`, so no change is required.
   The todo item "small WriteBufferSize with default ArenaBlockSize trips Debug.Assert" can be closed.
2. `Db/DbImpl.Read.cs` `GetFromFile`: the file's range tombstones are applied without truncating them to the file
   boundaries. This matches upstream TableCache::Get, so no change; it is noted for when range-tombstone truncation
   in iterators is revisited.
3. Callers of `DbFormat.TryParseInternalKey` (ExternalSstFileIngestionJob, TableCache range-del loading) now reject
   WAL-only value types as corruption, as upstream does. No call sites needed changes.
