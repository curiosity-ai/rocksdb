# RocksDB.Managed — progress tracker

Legend: `[ ]` todo, `[~]` in progress, `[x]` done. Each item names the C++ source it ports.

## Phase 0 — Setup
- [x] Disable upstream CI (`.github/workflows/*.yml` → `.yml.disabled`, `.lgtm.yml.disabled`)
- [x] `dotnet/` skeleton: global.json, Directory.Build.props (net10.0;net11.0), central packages, slnx
- [x] Projects: library, unit tests (MSTest), compat tests (native RocksDB via `RocksDB` NuGet), benchmarks (BenchmarkDotNet)
- [x] Review rocksdb-sharp high-level API → design table in CLAUDE.md
- [x] Foundation: `Core/Status.cs`, `Util/Coding.cs`, `Util/Crc32C.cs`, `Db/DbFormat.cs`, `Util/PooledBufferWriter.cs`
- [x] Verify the native `RocksDB` NuGet package loads in the compat test project (smoke test)

## Phase 1 — Persistent formats (parallel)
### A. WAL + WriteBatch  `[~] agent running`
- [ ] `Wal/LogFormat` — record types, block size 32KiB, header layouts (legacy 7B, recyclable 11B) — `db/log_format.h`
- [ ] `Wal/LogWriter` — async, pooled, fragmenting records across blocks, recyclable mode — `db/log_writer.cc`
- [ ] `Wal/LogReader` — checksum verify, fragment reassembly, corruption reporting, recycled-log handling — `db/log_reader.cc`
- [ ] WAL metadata records: `kSetCompressionType`, user-defined timestamp size, predecessor WAL info
- [ ] `Db/WriteBatch` — rep format (12B header: seq + count), all record tags, CF variants, iterate/handler, save points — `db/write_batch.cc`
- [ ] Compat: managed WriteBatch bytes == native `WriteBatch.ToBytes()`; parse native batches from `GetUpdatesSince`

### B. Block-based table (SST)  `[~] agent running`
- [ ] `Table/BlockBuilder` + `Table/Block` reader/iterator (restart points, shared-prefix delta encoding) — `table/block_based/block_builder.cc`, `block.cc`
- [ ] Data block hash index (`data_block_hash_index.cc`) — reader at least
- [ ] `Table/BlockHandle`, `Footer` (legacy + format_version 1..7, checksum types, magic numbers) — `table/format.cc`
- [ ] Block trailer: compression type + checksum (crc32c/xxhash/xxhash64/xxh3), context checksum (fv 6+)
- [ ] Index blocks: binary search index (+ `index_value_is_delta_encoded`), two-level/partitioned index (reader)
- [ ] Metaindex + properties block (`table_properties.cc`, `meta_blocks.cc`)
- [ ] `BlockBasedTableBuilder` — `block_based_table_builder.cc`
- [ ] `BlockBasedTableReader` + iterator, point lookup using filter — `block_based_table_reader.cc`
- [ ] Range deletion meta block
- [ ] Compat: native `SstFileWriter` → managed reader; managed builder → native `IngestExternalFiles` / read back

### C. Hashing, filters, compression  `[x] merged (cffc808)`
- [x] Hashes: `Hash()`, BloomHash, XXPH3-based `Hash64`/`GetSliceHash64` (RocksDB's frozen pre-release XXH3 — NOT System.IO.Hashing.XxHash3), Hash128/Hash2x64, checksum wrappers
- [x] Filters: FastLocalBloom, legacy Bloom, Standard128Ribbon builder+reader; byte-identical to native for fv4/5/6 + ribbon hybrid
- [ ] `optimize_filters_for_memory` rounding on build side
- [ ] Partitioned filters (reader) — with stream B's table reader
- [x] Compression: Snappy, LZ4/LZ4HC, ZSTD (pooled ctx), Zlib (raw deflate, <=16KiB blocks); BZip2/Xpress NotSupported
- [ ] Verify managed-compressed blocks are readable natively (needs SST builder → native ingest)
- [ ] Dictionary compression
- Note: `FilterPolicy.CreateBuilder(in FilterBuildingContext)` added (level/flush aware, needed for ribbon `bloom_before_level`) — table builder must call it

### D. Version metadata & files  `[~] agent running`
- [ ] `Db/FileNames` — `file/filename.cc` (LOG, MANIFEST-, CURRENT, IDENTITY, .sst, .log, OPTIONS-)
- [ ] `Db/VersionEdit` encode/decode, all tags incl. new-file custom fields, blob files, WAL additions/deletions — `db/version_edit.cc`
- [ ] MANIFEST read/write via log format; `CURRENT` atomic update
- [ ] `Options/OptionsFile` — parse & write `OPTIONS-*` (INI-like) — `options/options_parser.cc`
- [ ] Compat: decode native MANIFEST & OPTIONS files of a natively created DB

### E. Memtable  `[~] agent running`
- [ ] `InternalKeyComparator`, `BytewiseComparator`, `ReverseBytewiseComparator` — `util/comparator.cc`, `db/dbformat.cc`
- [ ] Arena-like pooled slab allocator for memtable entries — `memory/arena.cc`
- [ ] Concurrent `InlineSkipList` (lock-free reads, concurrent inserts) — `memtable/inlineskiplist.h`
- [ ] `MemTable` add/get/iterator, range tombstones, memtable entry encoding — `db/memtable.cc`

### F. Infrastructure  `[~] agent running`
- [ ] `Env/` file abstraction: async sequential writer (`WritableFileWriter`), random-access reader (`RandomAccessFileReader`), `LOCK` file
- [ ] Block cache (sharded LRU first, HyperClock later) with ref-counted pooled blocks — `cache/`
- [ ] Benchmark harness: managed vs native for each component + db_bench-like workloads (fillseq, fillrandom, readrandom, seekrandom)

## Phase 2 — DB core
- [ ] `RocksDb.OpenAsync`: create/recover DB (CURRENT→MANIFEST→VersionSet, replay WALs into memtables)
- [ ] Column families (create/drop/list, per-CF options)
- [ ] Write path: async group commit (leader/follower via channel), WAL append + sync, memtable insert, sequence numbers
- [ ] Read path: memtable → immutable memtables → L0..Ln with filter + block cache; snapshots
- [ ] Iterators: merging iterator, DBIter (visibility, deletions, merges, bounds, prefix)
- [ ] Merge operator support (full/partial merge, span-based)
- [ ] Flush: memtable → L0 SST, MANIFEST update, WAL obsoletion
- [ ] Compaction: leveled (picker + job), universal later; compaction filter; background scheduling
- [ ] Range deletions (DeleteRange) end-to-end
- [ ] Single delete, TTL
- [ ] Checkpoint, GetUpdatesSince (WAL tailing), GetLiveFilesMetadata, IngestExternalFiles, SstFileWriter public API
- [ ] Read-only and secondary open
- [ ] GetProperty (subset), statistics

## Phase 3 — Compat + performance
- [ ] Full-DB compat: native writes DB → managed opens/reads/writes → native reopens & verifies (and reverse)
- [ ] Randomized differential test vs native (same op stream, compare results)
- [ ] Benchmarks vs native: fillseq, fillrandom, readrandom, readseq, seekrandom, multiget; allocation report
- [ ] Profile & optimize hot paths (block decoding, skiplist, crc, compaction loop)

## Notes / decisions
- Native reference: `RocksDB` NuGet 11.12.0.7385 (rocksdb-sharp). C++ tree is 11.14.
- .NET SDKs: 10.0.403 and 11.0.100-rc.3 in `/root/.dotnet` (cloud container).
