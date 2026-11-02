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
| `Compression/BlockCompression*.cs` | `util/compression.{h,cc}` (BuiltinCompressionManagerV2, StartCompressBlockV2, ExtractUncompressedSize), `block_based_table_builder.cc` (max_compressed_bytes_per_kb capped at 1023) | verified for framing, level sanitization and the LZ4/LZ4HC level mapping. Known deviations: (1) a configured BZip2/Xpress type stores blocks uncompressed, where upstream falls back to the default compressor; (2) LZ4 acceleration > 1 runs with acceleration 1; (3) zlib only compresses blocks of 16 KiB or less (.NET's 32 KiB deflate window); (4) the ZSTD frame checksum option is ignored. All output stays readable by native. **Not ported:** dictionary compression and parallel compression. |

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
| `Table/IndexBuilder.cs` | `index_builder.{h,cc}` | verified by the byte-identical index-block compat tests. **Not ported:** partitioned index writing and hash index. |
| `Table/TableProperties.cs` | `table/table_properties.cc`, `table/meta_blocks.cc` | verified. Property names match, unsorted keys raise Corruption, and malformed u64 values are skipped. Trivial difference: a malformed `rocksdb.deleted.keys` value is not copied to the user properties. |
| `Table/BlockBasedTableBuilder.cs`, `BlockBasedTableReader.cs`, `BlockBasedTableIterator.cs`, `TableFormatOptions.cs`, `BlockCacheStatistics.cs` | `block_based_table_{builder,reader,iterator}.cc` | verified at the format level by the byte-identical SST compat tests (fv2..8 × checksums × compressions). **Not ported:** prefix extractor and prefix filters, partitioned filters, dictionary compression, user-defined timestamps, property collectors, super_block_alignment. |
| `Table/Filters/FilterBitsReaders.cs` | `filter_policy.cc` (GetBuiltinFilterBitsReader) | verified. Every reserved or unknown encoding maps to AlwaysTrue, and an empty filter maps to AlwaysFalse. A Ribbon filter shorter than its metadata raises Corruption (undefined behavior upstream). |
| `Table/Filters/BloomImpl.cs`, `FilterBitsBuilders.cs`, `Standard128Ribbon.cs`, `BuiltinFilterPolicies.cs`, `FilterContracts.cs` | `util/bloom_impl.h`, `filter_policy.cc`, `util/ribbon_*.h` | verified by the byte-identical filter compat tests. **Not ported:** `optimize_filters_for_memory` rounding on the build side. |

## Db: formats, metadata and the memtable

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Db/DbFormat.cs` | `db/dbformat.{h,cc}` | **fixed.** `TryParseInternalKey` accepted every type below `kTypeMaxValid`, including the WAL-only types (LogData, ColumnFamily*, XID markers), and rejected `kTypeMaxValid`. It now implements `IsExtendedValueType` exactly, and `IsValueType` / `IsExtendedValueType` helpers were added. (low) |
| `Db/InternalKey.cs` (InternalKey, LookupKey, IterKey) | `db/dbformat.h`, `db/lookup_key.h` | verified. `InternalKey.Valid` inherits the fix above. |
| `Db/InternalKeyComparator.cs` | `db/dbformat.{h,cc}`, `index_builder.cc` (FindShortestInternalKeySeparator) | verified |
| `Db/VersionEdit.cs` | `db/version_edit.{h,cc}`, `db/wal_edit.cc` | verified. All tags match, as do the NewFile4 custom-tag order, the forward-compat masks (`1<<13` and custom `1<<6`), the signed path_id, the u64 kNewFile seqno defaults and the error messages. `IsValidInternalKey` now delegates to `DbFormat.IsExtendedValueType`. Known deviation: string fields (db id, comparator, CF name) go through UTF-8, so CF names containing invalid UTF-8 bytes would not round-trip byte for byte. |
| `Db/FileMetaData.cs` (+ BlobFileAddition/Garbage, WalAddition) | `db/version_edit.h`, `db/blob/blob_file_{addition,garbage}.cc`, `db/wal_edit.cc` | verified |
| `Db/FileNames.cs` | `file/filename.cc`, `util/string_util.cc` (ConsumeDecimalNumber), `db/manifest_ops.cc` | verified (ParseFileName branch for branch, SetCurrentFile protocol, CURRENT parsing). The deviation for an info-log prefix match is documented in the file. |
| `Db/Manifest.cs`, `Db/ManifestLog.cs` | `db/version_edit_handler.cc`, `db/version_set.cc` (AtomicGroupReadBuffer), `db/version_builder.cc`, `db/wal_edit.cc` (WalSet) | verified (atomic groups, CF add/drop, monotonic per-CF log numbers, max min-log, NewestFirstByEpochNumber and BySmallestKey). **Not ported:** comparator/num_levels validation, UDT boundary padding, point-in-time and best-efforts recovery (already in todo). |
| `Db/WriteBatch.cs`, `Db/WriteBatchReader.cs`, `Db/IWriteBatchHandler.cs` | `db/write_batch.cc` | verified. Every tag and error string of ReadRecordFromWriteBatch is covered, along with the count check and TimedPut unpacking. A TimedPut value shorter than 8 bytes raises Corruption (undefined behavior upstream). |
| `Db/MemTable.cs` | `db/memtable.{h,cc}` | **fixed.** Concurrency (medium): the fragmented range-tombstone cache published the list and its version as two separate fields. A reader racing a rebuild could pair the previous list with the new version and miss a tombstone visible to its snapshot (a deleted key could reappear). The list and version are now published together as one immutable object. The regression test is a stress test, so it may not catch the race on every run. Get/SaveValue and ShouldFlushNow were checked and match. |
| `Db/MemTableSupport.cs`, `Db/MemTableRangeTombstones.cs` | `db/column_family.cc` (arena sanitize), `db/range_tombstone_fragmenter.cc` | verified. The fragmenter is a simplified port that keeps every seqno and applies snapshot striping at query time. |
| `Db/GetContext.cs` | `table/get_context.cc` | verified (range-deletion override of value types) |
| `Db/MergeHelper.cs`, `Db/MergeOperator.cs` | `db/merge_helper.{h,cc}`, `db/merge_context.h` | verified. Key choice and type rewrite after a full merge, the PartialMergeMulti fold, compaction-filter skip-until and the range-delete check per operand all match. **Not ported:** OpFailureScope::kMustMerge, blob and wide-column base values. |
| `Db/MergeHelper.BuiltinOperators.cs` | `utilities/merge_operators/uint64add.cc`, `string_append/stringappend.cc`, `db/merge_operator.cc` | verified (names, malformed values count as 0, ',' delimiter) |
| `Db/RangeDelAggregator.cs` | `db/range_del_aggregator.{h,cc}` (incl. TruncatedRangeDelIterator) | verified (truncation with kTypeMaxValid and seq-1, read and compaction stripes) |
| `Memtable/InlineSkipList.cs` | `memtable/inlineskiplist.h` | verified. Node layout, splice logic and CAS insert match, with acquire/release on links. The random height uses 32-bit xorshift; this only affects performance. |
| `Memory/Arena.cs`, `ConcurrentArena.cs`, `BlockBufferPool.cs` | `memory/arena.{h,cc}`, `memory/concurrent_arena.{h,cc}` | verified (OptimizeBlockSize clamp and alignment) |

## Db: iterators

| Managed file | C++ source(s) | Status |
|---|---|---|
| `Db/Iterators/DbIter.cs` | `db/db_iter.cc` | verified. FindNextUserEntry with the reseek, MergeValuesNewToOld, ReverseToForward / ReverseToBackward, PrevInternal, FindValueForCurrentKey(+UsingSeek), FindUserKeyBeforeSavedKey and bounds clamping were compared line by line, and 14 upstream tests were added. Known deviation: range tombstones use the pre-7.x ShouldDelete-per-entry scheme instead of tombstones in the merging heap; results are the same, but long deleted ranges are slower. **Not ported:** prefix seek, UDT, blob and wide columns, read callbacks. |
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

## Findings in out-of-scope files (for the coordinator)

1. `Db/ColumnFamilyData.cs` passes `ColumnFamilyOptions.ArenaBlockSize` straight into `MemTableOptions`. With the new
   default of 0, the derivation happens in `MemTableOptions.GetEffectiveArenaBlockSize`, so no change is required.
   The todo item "small WriteBufferSize with default ArenaBlockSize trips Debug.Assert" can be closed.
2. `Db/DbImpl.Read.cs` `GetFromFile`: the file's range tombstones are applied without truncating them to the file
   boundaries. This matches upstream TableCache::Get, so no change; it is noted for when range-tombstone truncation
   in iterators is revisited.
3. Callers of `DbFormat.TryParseInternalKey` (ExternalSstFileIngestionJob, TableCache range-del loading) now reject
   WAL-only value types as corruption, as upstream does. No call sites needed changes.
