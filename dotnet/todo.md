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
### A. WAL + WriteBatch  `[x] merged (b001a9c)`
- [x] `Wal/LogFormat`, `LogWriter` (sync/async, legacy+recyclable, zstd WAL compression), `LogReader` (recovery modes, predecessor WAL info, UnmarkEof tailing)
- [x] WAL metadata records: compression type, user-defined timestamp size, predecessor WAL info
- [x] `Db/WriteBatch` (byte-identical rep, pooled `Rent()`, save points, max bytes), `WriteBatchReader` ref struct, `IWriteBatchHandler` (struct, no boxing)
- [x] Compat: rep bytes == native; parse `GetUpdatesSince`; read native WALs (plain/recyclable/zstd); native replays managed-written WALs
- [ ] `FragmentBufferedReader` (tolerant tailing reader for replication)
- [ ] XXH3 per-record checksum, WAL termination point, protection info, timestamps, structured `PutEntity`
- [ ] Public API: `ColumnFamily` object instead of `uint cfId`

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

### D. Version metadata & files  `[x] merged (2d9abdd)`
- [x] `Db/FileNames` (builders, TryParseFileName, CURRENT/IDENTITY helpers)
- [x] `VersionEdit` encode/decode, all tags in this tree; byte-identical re-encode of native MANIFEST records
- [x] `ManifestReader`/`ManifestReplayer` (atomic groups, CF add/drop, WALs) → `RecoveredVersionState`; `ManifestWriter` (native opens managed-written MANIFEST)
- [x] Options model (`DbOptions`, `ColumnFamilyOptions`, `BlockBasedTableOptions`, `ReadOptions`, `WriteOptions`, ...) + `OptionsFile` parse/serialize, native loads our OPTIONS
- [ ] Wire MANIFEST seam (`IManifestRecordReader/Writer`) to `Wal.LogReader/LogWriter`; drop test-only `TestLogFormat.cs`
- [ ] Comparator/num_levels validation on replay, UDT boundary padding, point-in-time/best-efforts recovery
- [ ] Type `compaction_options_universal/fifo`, `block_cache` etc. (kept as raw strings)
- [ ] `ReadOptions.Snapshot`; a named static for sync writes (e.g. `WriteOptions.SyncWrites`)

### E. Memtable  `[x] merged (2fb2ccb)`
- [x] Bytewise/ReverseBytewise (+u64ts) comparators with exact separator/successor ports; `ComparatorKind` fast path
- [x] `InternalKeyComparator`, `InternalKey`, `LookupKey` (stackalloc), `IterKey`
- [x] `Arena`/`ConcurrentArena` on pinned-object-heap slabs, pooled (cap 256MB)
- [x] `InlineSkipList<TComparer>` (struct comparer, lock-free reads, CAS concurrent insert)
- [x] `MemTable`: add/concurrent add, get (merge operand collection), iterator, range tombstones + fragmenter, flush triggers
- [ ] Memtable extras: bloom filter, in-place updates, insert hints, protection bytes, wide columns/blob, read callback
- [ ] `MemTableList` (immutable memtables), snapshot-striped tombstone fragmenter
- [ ] Verify index separators against a native SST index block (needs stream B reader)
- [ ] Rename `Db.ValueType` (clashes with `System.ValueType`) once A/B/D are merged — e.g. `EntryType`

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
