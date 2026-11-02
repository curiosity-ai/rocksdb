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

### B. Block-based table (SST)  `[x] merged (082f9f0)`
- [x] `BlockBuilder`, `Block` (ref-counted pooled), `BlockIter` (data/index/meta, SeekForGet via hash index), `DataBlockHashIndex`
- [x] `BlockHandle`, `Footer` (fv 2..7, all checksum types, context checksum), `BlockChecksum`, `BlockFetcher`
- [x] Index: binary search (+delta-encoded values, first-key index); partitioned index read-only
- [x] Metaindex + properties (match native incl. compression name/options), range-del block
- [x] `BlockBasedTableBuilder` (sync/async, external SST mode for ingestion), `BlockBasedTableReader` (+Get visitor for merge chains), `BlockBasedTableIterator`
- [x] Compat: native SstFileWriter/flush → managed read (fv2..7 × 5 checksums, partitioned index); managed → native ingest
- [x] format_version 8 read+write (common key prefix, delta escape, metaindex gap) — byte-identical to native
- [x] Prefix extractor + prefix seek/filters, partitioned index+filters (write/read, byte-identical), optimize_filters_for_memory (bloom byte-identical; ribbon may differ by 16 B with glibc heap state), dictionary compression (zstd byte-identical; zlib/lz4hc dict write deviations)
- [ ] Parallel compression, memtable prefix bloom, hash index, UDT, property collectors
- [x] Integration: InternalKeyComparator/separators, Util.Hash, level-aware filter builders — index & data blocks byte-identical to native SstFileWriter (fv2..8)
- [x] Compat with snappy/lz4/lz4hc/zstd/zlib and bloom/ribbon filters both directions
- [x] `Db/TableFactory`: options model → TableFormatOptions/TableBuilderOptions/reader options, filter policy ids, per-level compression
- [ ] Separated key/value data blocks, uniform block hint, super_block_alignment writing, dictionary compression

### C. Hashing, filters, compression  `[x] merged (cffc808)`
- [x] Hashes: `Hash()`, BloomHash, XXPH3-based `Hash64`/`GetSliceHash64` (RocksDB's frozen pre-release XXH3 — NOT System.IO.Hashing.XxHash3), Hash128/Hash2x64, checksum wrappers
- [x] Filters: FastLocalBloom, legacy Bloom, Standard128Ribbon builder+reader; byte-identical to native for fv4/5/6 + ribbon hybrid
- [ ] `optimize_filters_for_memory` rounding on build side
- [ ] Partitioned filters (reader) — with stream B's table reader
- [x] Compression: Snappy, LZ4/LZ4HC, ZSTD (pooled ctx), Zlib (raw deflate, <=16KiB blocks); BZip2/Xpress NotSupported
- [x] Managed-compressed blocks readable natively (managed SST → native ingest)
- [ ] Dictionary compression
- Note: `FilterPolicy.CreateBuilder(in FilterBuildingContext)` added (level/flush aware, needed for ribbon `bloom_before_level`) — table builder must call it

### D. Version metadata & files  `[x] merged (2d9abdd)`
- [x] `Db/FileNames` (builders, TryParseFileName, CURRENT/IDENTITY helpers)
- [x] `VersionEdit` encode/decode, all tags in this tree; byte-identical re-encode of native MANIFEST records
- [x] `ManifestReader`/`ManifestReplayer` (atomic groups, CF add/drop, WALs) → `RecoveredVersionState`; `ManifestWriter` (native opens managed-written MANIFEST)
- [x] Options model (`DbOptions`, `ColumnFamilyOptions`, `BlockBasedTableOptions`, `ReadOptions`, `WriteOptions`, ...) + `OptionsFile` parse/serialize, native loads our OPTIONS
- [x] `ManifestLogReader/Writer` over Wal.LogReader/LogWriter
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
- [x] Index separators byte-identical to native index blocks
- [x] Renamed `Db.ValueType` → `Db.EntryType` (clashed with `System.ValueType`)

### F. Infrastructure  `[x] merged (8d7a3d0)`
- [x] `LocalFileSystem` hardened: pooled growing write buffer, fdatasync, dir fsync, fallocate, direct I/O (Linux), OFD LOCK interoperating with native RocksDB; `MemoryFileSystem` with crash simulation; `FilePrefetchBuffer`
- [x] Block cache: sharded LRU (+priority pools, strict capacity) and HyperClock; `OffsetableCacheKey`; `CachedBlock` ref-counted handles — recommendation: HyperClock default
- [x] db_bench-style runner (`-- dbbench --engine native ...`) + `IKeyValueEngine`; native baseline recorded below
- [x] Block cache wired into `BlockBasedTableReader` (parsed blocks cached, index/filter high priority, per-file OffsetableCacheKey, stats) — see docs/perf.md
- [ ] Rate limiter implementation; hard links (Checkpoint); non-blocking SyncAsync; auto-resizing HyperClock

Native baseline (200k ops, 16B key / 100B value, shared noisy 4-core box): fillseq 3.9–9.1 µs/op, fillrandom 7.1–8.0, readrandom memtable 4.8, readrandom SST 15.4, readmissing 4.0.

## Phase 2 — DB core
Contracts added: `Db/Iterators/InternalIterator.cs`, `Db/MergeOperator.cs` (+`MergeOperands`), `Db/ITableCache.cs`.
Wave 2a agents: S = DB spine (open/recover, VersionSet, write path, Get, flush, CFs, snapshots, TableCache);
I = iterators + merge (MergingIterator, LevelIterator, DBIter, MergeHelper, range-del aggregation);
P = perf + block cache wiring (Table/, Cache/, Memtable/, Util/).

- [x] `RocksDatabase.Open/OpenAsync`: create (IDENTITY/MANIFEST/CURRENT/OPTIONS/LOCK), recover (MANIFEST + WAL replay, all recovery modes, flush on recovery), obsolete file deletion
- [x] Column families (create/drop/list, per-CF memtables/versions, CF-aware WAL replay)
- [x] Write path: async group commit (`WriteThread`, pooled ValueTask sources, sync callers lead on own thread), memtable switch, max_write_buffer_number stall
- [x] Read path: memtable → imm → L0 → Ln with snapshots, range tombstones, merges; TryGet/Get callback/GetPooled/MultiGet + async
- [x] Flush: background flush jobs, L0 build with tombstones + unique ids, MANIFEST rollover; `FlushAsync`
- [x] Snapshots, GetProperty subset, GetLiveFilesMetadata, NewIterator via DbIteratorFactory
- [x] Compat: native→managed→native and managed→native→managed round trips with CFs/merges/range deletes/WAL-only data
- [x] Block cache passed to readers: `BlockBasedTableOptions.BlockCache` or DB-wide default 32 MiB HyperClock shared across CFs
- [ ] Shared bounded table cache across CFs
- [ ] Concurrent memtable insert, pipelined write, `max_total_wal_size`, lazy WAL sync on rotation
- [ ] Read-only open, custom comparators, KeyMayExist, WAL tracking in MANIFEST, keep dropped CF readable while handle alive
- [x] Iterators: MergingIterator, LevelIterator, DBIter, public `Iterator` (+foreach, ScanAsync), `DbIteratorFactory` — matches native exactly on multi-level DB with snapshots/bounds
- [ ] Iterators: range tombstones in heap (skip deleted ranges), prefix seek, async reverse movement, blob/wide columns
- [x] Merge: `MergeHelper` (TimedFullMerge, MergeUntil), `UInt64AddOperator`, `StringAppendOperator`; `RangeDelAggregator` (read + compaction w/ snapshot stripes); `CompactionFilter` contract
- [x] Compaction: leveled (dynamic level bytes, all compaction pris, trivial move, intra-L0, TTL/periodic), CompactionIterator, CompactionJob, scheduling, write stalls + delayed-write token bucket, CompactRange/WaitForCompact, compaction filters (+TTL), universal (size-amp/ratio/count), FIFO (TTL/size/intra-L0)
- [x] Compat: managed-compacted DB valid & readable natively, native multi-level DB compacted by managed, randomized differential vs native
- [ ] Compaction gaps: subcompactions, async input reads, compensated size (deletion boost), bottommost files marked for compaction, persist round-robin cursors, universal periodic/delete-triggered/incremental, FIFO temperature/kv-ratio, persist universal/fifo options to OPTIONS, CompactFiles, drop CF waits for running compaction
- [x] Range deletions (DeleteRange) end-to-end (memtable, flush, SST, Get, iterators)
- [x] Single delete (read path)
- [x] TTL value format + `TtlDatabase` (compatible with native OpenWithTtl)
- [ ] Wire `TtlCompactionFilter` into compaction; TTL iterator wrapper
- [x] Bug: small WriteBufferSize with default ArenaBlockSize (fixed: default 0 → derived like upstream)
- [x] GetLiveFilesMetadata
- [x] GetUpdatesSince (`TransactionLogIterator`, `TailUpdatesAsync` signalled by write path, WAL archival, FragmentBufferedLogReader) — identical sequences/bytes to native both ways
- [x] Checkpoint (hard links via `IFileSystem.LinkFile`), Disable/EnableFileDeletions, GetLiveFiles, GetSortedWalFiles, FlushWal/SyncWal
- [x] SstFileWriter public API + IngestExternalFiles (level picking, global seqno in MANIFEST) — compat both ways
- [x] Replication round trip (checkpoint + tail into managed and native replicas)
- [ ] Ingestion: WriteGlobalSeqno, IngestBehind, overlapping files in one call, running-compaction checks
- [x] Ported rocksdb-sharp Replication/ + Switchover/ + Cluster/ (Raft) into `src/RocksDb.Managed.Replication` (transport-agnostic, no polling, pooled WalBatch, IFileSystem-aware) + `src/RocksDb.Managed.Replication.MagicOnion` (gRPC) + `samples/` — 30 tests incl. gRPC on localhost and a native replica
- [ ] Replication: network-partition cluster test; handoff hold 100–220 ms vs 10–50 ms reported by rocksdb-sharp (investigate); replication benchmarks
- [x] Read-only and secondary open (+TryCatchUpWithPrimary over a live native primary)
- [ ] Secondary: tail MANIFEST incrementally instead of re-reading
- [x] GetProperty (subset)
- [ ] Statistics

First managed vs native db_bench (200k ops, 16B/100B, shared box; managed has no block cache yet):
| benchmark | native µs/op | managed µs/op | managed alloc/op |
|---|---|---|---|
| fillseq | 4.06 | 3.31 | 0 steady (arena warmup) |
| fillrandom | 6.22 | 4.75 | 0 |
| readrandom (memtable) | 4.19 | 2.80 | 0 |
| readrandom (after flush) | 6.90 | 4.92 | ~3 B (table open) |
| flush | 214 ms | 398 ms | |

With block cache (8 MiB both engines, 200k ops, data > cache):
| benchmark | native µs/op | managed µs/op | managed alloc/op |
|---|---|---|---|
| fillrandom | 8.37 | 5.80 | ~0 steady |
| flush | 210 ms | 470 ms | 6 MB/flush |
| readrandom (SST, cache misses) | 7.74 | 6.69 | 44 B (cache-miss path) |

Perf follow-ups: flush is ~2x slower than native and allocates 6 MB; cache-miss read path allocates ~44 B/op;
HyperClock slots as array-of-structs; single-threaded cached table Get +18% vs plain dictionary.

With compaction (1M ops, 16B/100B, 4MB write buffer, no compression, shared box):
| | native µs/op | managed µs/op | managed alloc/op |
|---|---|---|---|
| fillrandom | 7.12 | 4.06 | 35 B (per-file, not per-key) |
| readrandom | 15.08 | 8.26 | 8.8 B |
| overwrite | 6.26 | 3.72 | 75 B |
Write-amp: fill 3.42 native vs 3.24 managed; overwrite 4.12 vs 4.17.

## Phase 3 — Compat + performance
Final completeness wave (agents running): BlobDB integrated blob files; user-defined timestamps; background error recovery + EventListener + BackupEngine; perf #3 flush/compaction throughput.
- [x] Features: WriteBatchWithIndex (+BaseDeltaIterator; transactions now use it), SetOptions/SetDBOptions (+typed), custom comparators e2e (+MANIFEST comparator/num_levels validation), KeyMayExist/GetApproximateSizes/memtable stats/int+map properties/table properties, Statistics (all upstream tickers/histograms, native ToString), info LOG (+rotation), wide columns (PutEntity/GetEntity/iterator columns, merge/compaction) — all compat-tested vs native
- [ ] Features gaps: compaction filters on entities, blob columns/attribute groups/MultiGetEntity, per-SuperVersion options snapshot, mutable block-based table options, KeyMayExist doing filter I/O on cache miss, cfstats interval/file-read histograms, per-level GET_HIT stats, WBWI MultiGetFromBatchAndDB
- [ ] Flaky? `PrefixSeekTests.PrefixSameAsStartMatchesModel` failed once on net11.0 (sorted-run file count precondition); 10/10 reruns pass
- [x] Perf #2: async fast paths (preadv2/pwritev2 RWF_NOWAIT, pooled builders) → async ≈ sync, 0 B/op reads/seeks; batched `MultiGetBatch` (+async, 0 B/op); BlockIter.Prev cache (~30% faster reverse); LevelIterator reuses table iterators (0 B/op seeks in multi-level LSM); managed overwrite under compaction now 3.4 vs C 5.1 µs. Three-way geomean vs C: managed 0.69–1.17×, async 0.64–1.17×, rocksdb-sharp 0.85–1.33× (`bench/results/2026-11-02-vm-perf2*.md`)
- [ ] Flush/compaction per-entry cost still 1.4–2x native; background compaction lags under sustained writes (waitforcompaction 0.6–0.8 s vs 0.02–0.18 s); reverse iteration copies values (pin blocks instead)
- [x] Transactions: `OptimisticTransactionDatabase` (serial + parallel validation), `TransactionDatabase` (WriteCommitted, point locks, timeouts, deadlock detection, expiration, 2PC prepare/commit/rollback + recovery); 2PC WAL records byte-identical to native, prepared txns recover across engines; fixed recovery last-sequence bug for 2PC records; dbbench txnoptimistic/txnpessimistic on all engines
- [ ] Transactions gaps: WritePrepared/WriteUnprepared, range locks, UDT; unify IndexedWriteBatch with public WriteBatchWithIndex; pool lock entries/tracked keys (~1.8–2.3 KB/txn)
- [x] Replaced stringy enums: typed FilterPolicy/MergeOperator/CompactionFilter/SliceTransform options (persisted text preserved byte-for-byte), MemTableFactoryType/TableFactoryType/FlushBlockPolicyType, `DbProperties` constants, `AppendEntriesRejectReason`, bench EngineKind/DbBenchmark/CompressionType flags, round-trip FilterKind
- [x] `ColumnFamilyOptions.Comparator` typed (ComparatorName derived/persisted)
- [x] Fixed intermittent hang (CloseRacingWritersAndFlushes): FlushAsync/WaitForCompact waiters slept forever when close stopped scheduling a queued flush; shutdown now signals waiters, which fail with ObjectDisposed/ShutdownInProgress
Goal: finish implementation, validate vs C++ source, comprehensive tests (unit/integration/round-trip) and benchmarks (managed vs rocksdb-sharp vs pure C native).
- [x] Source audit #1 vs C++ (`docs/validation.md`): ~90 files (formats/util/table/wal/options/memtable/iterators); fixed memtable range-tombstone publish race, ArenaBlockSize default (0 → derived), internal-key type validation; added db_iter_test vectors
- [x] Source audit #2 (DB layer): 11 fixes (CF-drop vs MANIFEST, torn MANIFEST continuation, PIT WAL recovery resume, Get sequence/super-version order, ingestion seqno publish, CF drop during flush, cancellation tearing WAL, WAL handle leak on switch failure, writes racing close, option sanitization, snapshot ref leak); `TestHooks`, `FaultInjectionFileSystem`, ~300-point crash sweep, concurrent stress
- [ ] Background error auto-recovery (DB stays read-only after failed flush/MANIFEST write until reopen)
- [x] Three-way benchmark suite: C benchmark on librocksdb (`bench/native-c`), dbbench runner with full workload list + percentiles + async, `bench/run-all.sh`, `compare`/`report`, `DbHeadToHeadBenchmarks` (results in `bench/results/`)
- [x] Bench follow-up: compaction flags/stats/waitforcompaction/compact on all 3 engines; compaction-heavy config (`results/2026-11-02-vm-compaction.md`): LSM shape & write-amp match native (2.96 vs 3.05)
- [ ] Perf #2 (from 3-way results): async API allocations (150–300 B/write, ~230 B/TryGetAsync, 0.8–2.9 KB/async seek); span-based MultiGet (130–930 B/key); reverse iteration 1.3–4.6x slower than C; iteration/seek with 1 KiB values 2–5x; flush 1.3–3x slower + 2–8 MB allocs; fillseq 1 KiB allocs ~1.1 KB/op; overwrite under compaction 2.2x C (write stalls: background compaction keeps up less well — 0.83 s pending vs ~0); re-run matrix on a quiet machine
- [x] `tests/RocksDb.Managed.RoundTrip`: 35 crash-isolated managed↔native scenarios (self-invoking child; signals/timeouts decoded; failure state preserved): alternations ×6 option variants, managed-first, unclean shutdown both ways, edge keys/values, 200k–1M keys across levels, SST ingest, checkpoints, TTL, OPTIONS files; long mode `ROUNDTRIP_LONG=1` passes (20 alternations × 6,000 ops × 6 variants). Found/fixed: arena default, native struct-form filter policy in OPTIONS
- [x] Full-DB compat: native writes DB → managed opens/reads/writes → native reopens & verifies (and reverse)
- [ ] Randomized differential test vs native (same op stream, compare results)
- [ ] Benchmarks vs native: fillseq, fillrandom, readrandom, readseq, seekrandom, multiget; allocation report
- [ ] Profile & optimize hot paths (block decoding, skiplist, crc, compaction loop)

## Notes / decisions
- Native reference: `RocksDB` NuGet 11.12.0.7385 (rocksdb-sharp). C++ tree is 11.14.
- .NET SDKs: 10.0.403 and 11.0.100-rc.3 in `/root/.dotnet` (cloud container).
