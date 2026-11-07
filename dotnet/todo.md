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
- [x] `FragmentBufferedReader` (tolerant tailing reader for replication)
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
- [x] `optimize_filters_for_memory` rounding on build side
- [x] Partitioned filters (reader) — with stream B's table reader
- [x] Compression: Snappy, LZ4/LZ4HC, ZSTD (pooled ctx), Zlib (raw deflate, <=16KiB blocks); BZip2/Xpress NotSupported
- [x] Managed-compressed blocks readable natively (managed SST → native ingest)
- [x] Dictionary compression
- Note: `FilterPolicy.CreateBuilder(in FilterBuildingContext)` added (level/flush aware, needed for ribbon `bloom_before_level`) — table builder must call it

### D. Version metadata & files  `[x] merged (2d9abdd)`
- [x] `Db/FileNames` (builders, TryParseFileName, CURRENT/IDENTITY helpers)
- [x] `VersionEdit` encode/decode, all tags in this tree; byte-identical re-encode of native MANIFEST records
- [x] `ManifestReader`/`ManifestReplayer` (atomic groups, CF add/drop, WALs) → `RecoveredVersionState`; `ManifestWriter` (native opens managed-written MANIFEST)
- [x] Options model (`DbOptions`, `ColumnFamilyOptions`, `BlockBasedTableOptions`, `ReadOptions`, `WriteOptions`, ...) + `OptionsFile` parse/serialize, native loads our OPTIONS
- [x] `ManifestLogReader/Writer` over Wal.LogReader/LogWriter
- [ ] Comparator/num_levels validation on replay, UDT boundary padding, point-in-time/best-efforts recovery
- [x] Type `compaction_options_universal/fifo`, `block_cache` etc. (kept as raw strings)
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
- [x] Statistics

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
Remaining-gap wave (agents running):
- [x] WritePrepared / WriteUnprepared (commit cache, prepared heap, read callbacks, SnapshotChecker in compaction, recovery; WAL byte-identical and mutual recovery with native 11.12 under both policies). Gaps: two_write_queues, commit-time batches
- [x] Blob direct write + V2 entity blob columns + FilterV3 on entities (compat both ways with native 11.12, which supports both); optimistic txn + UDT = upstream parity (cited); remaining UDT txn overloads done
- [x] Audit #3: `docs/validation-coverage.md` covers all 220 source files (172 line-by-line across audits, 48 managed-only design-reviewed); fixed FIFO TTL newest-key-time, WAL checksum always verified, compaction_options_fifo/universal persisted + SetOptions, blob GC ranges, TTL 2038
- [x] rocksdb-sharp: real linux-x64 glibc CI build with all codecs: exports only rocksdb_* (1772), loader preload not required, all native/managed tests pass (PR #98). arm64/musl/jemalloc not built locally (same flags)
- [x] Audit #3 deviations ported: universal (full picker port: periodic, delete-triggered/marked files, max_read_amp, incremental, compression_size_percent), FIFO (max_data_files_size, kv-ratio, temperature in MANIFEST), output cutting (TTL, SstPartitioner + fixed-prefix, round-robin, upper bound), memtable first-insert time, HyperClock eviction cap/clamp, LRU charge, WAL iterator NotFound on gaps. Universal + FIFO lock-step parity with native (write-amp 3.201 vs 3.202, 4.408 vs 4.408)
- [ ] Left: read-triggered compaction (needs read sampling), reduce_file_locking, preclude_last_level_data_seconds; partition-straddle preselection in CompactRange
- [x] Final benchmarks (`bench/results/2026-11-02-final-warm-r5.md`, `-final-bdn.md`): REPEATS=5 warm, medians + 95% bootstrap CIs.
  Geomean time vs C native over 112 matrix benchmarks: managed 0.66x [0.64-0.67], managed-async 0.68x, rocksdb-sharp 1.03x;
  compaction-heavy managed 0.58x. Known slow spots: 4-thread sync fillrandom/overwrite (1.04-1.81x; async 0.75-1.03x), 4-thread
  readmissing with 100 B values (1.4-3.0x). BDN vs rocksdb-sharp: 0.21-0.60x, hot paths allocation-free.
- [x] Fixed close/background-flush race found by the benchmark warm-up (purge after a flush could run after close and delete a
  new DB's MANIFEST in the same directory).
- [x] Perf: 4-thread sync random writes and 4-thread readmissing (see Remaining gaps)
Final completeness wave (all merged):
- [x] Integrated BlobDB: blob file format/builder/reader/cache/GC, versions & MANIFEST, Get/MultiGet/iterators/merges, checkpoint/GetLiveFiles; blob files byte-identical to native; native↔managed both ways; blob dbbench (`results/2026-11-02-vm-blob.md`: managed 0.55x C)
- [x] Blob: zlib >16 KiB (16 KiB independent chunks; readable by native), blob readers bounded by max_open_files; forced GC universal/FIFO = parity (upstream leveled-only)
- [x] Blob gaps: V2 entity blob columns (native 11.12 never writes them), blob direct write
- [x] Background errors: ErrorHandler (severity maps, soft/hard/fatal, auto-resume, retryable IO errors, new MANIFEST/WAL on recovery), `Resume()`, `EventListener`; BackupEngine + BackupEngineReadOnly (native-identical layout & meta; incremental backups across engines)
- [ ] Backup/error gaps: SstFileManager (no-space auto recovery), atomic WAL-error recovery edits, exclude_files_callback, parallel copies; native-under-.NET-11 empty backup meta FIXED: librocksdb.so (NuGet) statically links libstdc++ but exports std:: symbols; the .NET 11 apphost doesn't load libstdc++.so.6 globally → std::bad_cast on numeric stream writes inside native. Tests/bench preload libstdc++ (`tests/Shared/NativeStdCxxPreload.cs`). NOTE: affects any rocksdb-sharp app hosted by a .NET 11 apphost
- [x] User-defined timestamps (persisted mode): timestamped writes/WriteBatch (byte-identical), Get/MultiGet/iterators (+IterStartTs), filters on key w/o ts, ts min/max properties, full_history_ts_low GC + MANIFEST; compat both ways incl. blob values with timestamps; round-trip scenario
- [x] UDT range deletions (native timestamped range tombstones read correctly; flush/compaction/full_history_ts_low; fixed snapshot×timestamp point lookup), SstFileWriter with timestamps + ingestion
- [x] UDT completion: persist_user_defined_timestamps=false (strip/pad, flush postponement, MANIFEST boundaries, toggling on reopen; compat both ways), UDT in WBWI + WriteCommitted transactions (commit ts, validation, 2PC commit-with-ts recovery; compat both ways), truly async timestamped Get/MultiGet
- [x] UDT leftovers: optimistic txn with UDT (upstream refuses too), do_validate=false GetForUpdate overloads, write_batch_track_timestamp_size
- [x] Fixed flaky `PrefixSameAsStartMatchesModel` (forced bottommost compaction for a deterministic LSM shape)
- [x] Perf #3: flush WAL-on 2.31x → 1.19x native (WAL writeback during switch), no large per-file allocations (59.6 → 5.5 B/put, no gen2 GCs), first-run AggressiveOptimization on per-entry paths; steady-state flush/compaction ≤ native; compaction-heavy config managed 0.62x native geomean (`results/2026-11-02-vm-perf3*.md`)
- [x] Compaction picking parity: root causes were flush keeping hidden versions (now applies upstream rule A per snapshot stripe) and max output size cap (2x target above bottommost). Lock-step harness: all 60 LSM shapes and compaction sequence identical to native; write-amp 4.321 vs 4.322. Compat test `CompactionPickerParityTests`
- [ ] Under free-running load the faster managed writer piles L0 to ~10 files with 1 compaction thread (native 5–6) → more pending work at end; consider write-controller pacing parity
- [x] Fixed secondary/WAL tailing lag at block-aligned EOF (also likely cause of an intermittent replication-test failure: 15/15 clean runs after the fix) and missing-dir open error (InvalidArgument)
- [x] Final verification (both TFMs): unit 886, compat 227, replication 30, round-trip 40 (+1 long), QuickStart 26/26, C bench builds. Fixed last flaky test (EventListenerTests: recorder used two unsynchronized queues; 20/20 after fix)
- [x] Bench warm-up: `--warmup` (default on, all 3 legs) runs every workload + 64 flush/compaction cycles on a throwaway DB, waits for JIT quiescence; per-benchmark JIT counts in reports (managed: median 161 → 0 methods during measurement). Warm three-way geomean vs C: managed 0.50–0.81x, managed-async 0.48–0.73x, rocksdb-sharp 0.89–1.19x (`bench/results/2026-11-02-vm-warm*.md`)
- [ ] Compaction-heavy fills still JIT 40–630 methods while timed (LSM shapes not reached by warm-up); LZ4 1 KiB flush 1.04–1.21x C (K4os encoder)
- [ ] First flush/compaction in a fresh process 1.4–1.64x native (JIT warm-up; affects cold-start only); LZ4 flushes 2.5x (K4os JIT/unoptimized) — consider ReadyToRun guidance or a faster LZ4 path
- [x] Features: WriteBatchWithIndex (+BaseDeltaIterator; transactions now use it), SetOptions/SetDBOptions (+typed), custom comparators e2e (+MANIFEST comparator/num_levels validation), KeyMayExist/GetApproximateSizes/memtable stats/int+map properties/table properties, Statistics (all upstream tickers/histograms, native ToString), info LOG (+rotation), wide columns (PutEntity/GetEntity/iterator columns, merge/compaction) — all compat-tested vs native
- [x] (closed, see Remaining gaps) Features gaps: compaction filters on entities, blob columns/attribute groups/MultiGetEntity, per-SuperVersion options snapshot, mutable block-based table options, KeyMayExist doing filter I/O on cache miss, cfstats interval/file-read histograms, per-level GET_HIT stats, WBWI MultiGetFromBatchAndDB
- [x] Flaky? `PrefixSeekTests.PrefixSameAsStartMatchesModel` failed once on net11.0 (sorted-run file count precondition); 10/10 reruns pass
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
- [x] Background error auto-recovery (DB stays read-only after failed flush/MANIFEST write until reopen)
- [x] Three-way benchmark suite: C benchmark on librocksdb (`bench/native-c`), dbbench runner with full workload list + percentiles + async, `bench/run-all.sh`, `compare`/`report`, `DbHeadToHeadBenchmarks` (results in `bench/results/`)
- [x] Bench follow-up: compaction flags/stats/waitforcompaction/compact on all 3 engines; compaction-heavy config (`results/2026-11-02-vm-compaction.md`): LSM shape & write-amp match native (2.96 vs 3.05)
- [ ] Perf #2 (from 3-way results): async API allocations (150–300 B/write, ~230 B/TryGetAsync, 0.8–2.9 KB/async seek); span-based MultiGet (130–930 B/key); reverse iteration 1.3–4.6x slower than C; iteration/seek with 1 KiB values 2–5x; flush 1.3–3x slower + 2–8 MB allocs; fillseq 1 KiB allocs ~1.1 KB/op; overwrite under compaction 2.2x C (write stalls: background compaction keeps up less well — 0.83 s pending vs ~0); re-run matrix on a quiet machine
- [x] `tests/RocksDb.Managed.RoundTrip`: 35 crash-isolated managed↔native scenarios (self-invoking child; signals/timeouts decoded; failure state preserved): alternations ×6 option variants, managed-first, unclean shutdown both ways, edge keys/values, 200k–1M keys across levels, SST ingest, checkpoints, TTL, OPTIONS files; long mode `ROUNDTRIP_LONG=1` passes (20 alternations × 6,000 ops × 6 variants). Found/fixed: arena default, native struct-form filter policy in OPTIONS
- [x] Full-DB compat: native writes DB → managed opens/reads/writes → native reopens & verifies (and reverse)
- [x] Randomized differential test vs native (same op stream, compare results)
- [x] Benchmarks vs native: fillseq, fillrandom, readrandom, readseq, seekrandom, multiget; allocation report
- [x] Profile & optimize hot paths (block decoding, skiplist, crc, compaction loop)

## Notes / decisions
- Native reference: `RocksDB` NuGet 11.12.0.7385 (rocksdb-sharp). C++ tree is 11.14.
- .NET SDKs: 10.0.403 and 11.0.100-rc.3 in `/root/.dotnet` (cloud container).

## Remaining gaps (triage 2026-11-02, evidence-based; supersedes the unchecked items above)

Legend: S/M/L = rough size. [~] = agent working on it.

- Compaction
  - [x] read-triggered compaction (read sampling), periodic compaction trigger, reduce_file_locking (bottom-priority pool), preclude_last_level_data_seconds (seqno-to-time mapping, per-key placement), CompactRange partition-straddle; lock-step/compat vs native
  - [x] read-sampling overhead checked on the quiet final run (one thread-local random step per Get; no slowdown beyond noise)
  - [x] subcompactions (index-anchor boundaries, ClippingIterator), compensated size with deletion boost, bottommost files marked for compaction + bottommost_file_compaction_delay, round-robin cursors persisted in MANIFEST (byte-identical to native), CompactFiles API; lock-step parity vs native
  - [x] write-controller: L0 peak re-measured warm = native (7-8); three write-controller deviations fixed
  - [x] async compaction input readahead; subcompactions with UDT; CompactFiles disk-space check + CompactionJobInfo
- Transactions
  - [x] two_write_queues (second WAL-only write queue, published vs allocated seqno) + commit-time write batches, all policies; compat vs native both ways
  - [x] steady-state transactions allocate only what the equivalent WriteBatch does (557 B for 4 puts, was 717-1517 B)
  - [x] flaky under load `WriteSlowdownDelaysWrites`, `AutoFlushPreservesGenerationOrderAndOldestBlobFile`, `CommitsRacingReaders` (root-caused, see below)
  - [x] range locks (RangeLockManager, escalation, deadlock detection; range_locking_test ported); WAL termination point
- Wide columns / blob
  - [x] compaction filters on entities (FilterV4 + lazy WideColumnBlobResolver), MultiGetEntity, attribute groups (Put/Get/MultiGet), coalescing + attribute-group iterators, lazy entity reads (GetEntityLazy/MultiGetEntityLazy, ranged blob column reads); compat-tested vs native
  - [x] WBWI MultiGetFromBatchAndDB; backup exclude_files_callback + parallel copies
  - [x] WBWI GetEntityFromBatchAndDB / MultiGetEntityFromBatchAndDB, transaction entity APIs + cross-CF iterators, lazy MultiResolve
- Table format / options / API
  - [x] hash index (kHashSearch) read+write, separated key/value data blocks, uniform index hint, block_align, super_block_alignment (byte-identical vs native SstFileWriter)
  - [x] user table-properties collectors (+ CompactOnDeletionCollector, NeedCompact), memtable prefix bloom, insert hints, in-place updates (+ callback)
  - [x] parallel compression (byte-identical to serial), per-key protection info (WriteBatch/memtable/block; db_kv_checksum_test ported; serial + parallel inserts), managed LZ4 fast encoder (byte-identical to liblz4, 1.4-2.2x faster than K4os)
  - [x] index_block_search_type (interpolation / auto on is_uniform), CompactForTieringCollector
  - [x] WriteBatch ColumnFamily overloads, WriteOptions.SyncWrites, MarkWalTerminationPoint, max_total_wal_size, lazy WAL sync on rotation (single-CF, non-2PC; else eager = stronger), reads on dropped CF handles
  - [x] shared per-DB table cache, best_efforts_recovery (matches native on 12 damage cases), WAL tracking in MANIFEST, per-SuperVersion options snapshot, mutable block-based table options
  - [x] auto-resizing HyperClock (growable table, writers pause briefly; upstream is lock-free linear hashing), GenericRateLimiter for flush/compaction writes
  - [x] rate limiter on reads + tickers; all block-based table options via SetOptions; damaged SST fails at open (same status as native, directory untouched)
- Write path / iterators perf
  - [x] 4-thread readmissing (per-processor SuperVersion cache, pinned table readers: 0.41->0.31 us sync, 0.62->0.33 async, C 0.43) and sync group-commit wait (AwaitState port), parallel memtable inserts for groups >= 64 entries (loaded-machine numbers; re-measure quietly)
  - [x] reverse iteration pins blocks (readreverse 1 KiB 0.70->0.45 us), async reverse movement, range-deleted runs skipped (scan over 99% deleted 13.8 ms -> 0.16 ms, native 0.66), async compaction input readahead (cold compact 3.05 s -> 1.05 s, native 2.3-2.5)
  - [x] reverse merge operands pinned; async NextAsync direction switch; async index partition loads; user-iterator readahead (auto + ReadaheadSize)
  - [x] forward merge operands pinned; readahead prefetch allocation-free
  - [x] pipelined write (4-thread sync writes 15-20% faster; native WAL compat both ways), parallel memtable inserts for every group (no 64-entry minimum; faster than serial on a non-oversubscribed box), LZ4 encoder / cold start (managed fast encoder)
  - [x] unordered_write (native WAL compat both ways; 4-8 threads 35-60% faster), offloaded async fdatasync (opt-in `LocalFileSystem.OffloadAsyncSync`: slower on this VM), secondary catch-up re-reads the MANIFEST when the replayed state references a deleted file, `MergeOperatorAcrossMemtableAndTables` / blob auto-flush flakes root-caused
  - [x] `WriteSlowdownDelaysWrites` root-caused: version install and write-stall recalculation were in separate mutex sections (upstream: one); spurious recalculations cut the delayed rate; continued delay now keeps write-controller debt
- Ingestion / TTL / stats
  - [x] ingestion: write_global_seqno (legacy, + reader checksum fallback), ingest_behind, overlapping files in one call, allow_db_generated_files, link_files; TTL iterator; GET_HIT_L*, file-read histograms, cfstats intervals; KeyMayExist without I/O
  - [x] file checksums (FileChecksumGenCrc32c in MANIFEST, VerifyFileChecksums, ingestion verify), ingestion temperatures; GET_HIT for MultiGet, FILE_READ_FLUSH/DB_OPEN/VERIFY histograms, per-level file-read histograms
  - [x] ingestion atomic_replace_range (incl. universal partial overlap via range-tombstone SST, power-loss sweep), blob direct-write checksums, backups verify against the DB's crc32c checksums (identical shared_checksum lines to native), blob I/O histograms
- Replication / secondary
  - [x] secondary tails the MANIFEST incrementally (native<->managed secondary compat), Raft network-partition tests, switchover hold trimmed (probe promote, 3 fewer fsyncs), replication benchmarks (bench/RocksDb.Managed.Replication.Benchmarks)
  - [x] secondary verifies new files before installing a version
  - [x] switchover hold re-measured quietly (in-process p50 4.6-6.1 ms; multi-process median 27 ms)
  - [x] replica keeps up at max rate: large chunks inserted into the memtable in parallel ranges (WriteOptions.MemtableInsertParallelism, replica default min(cores,4)); lag p50 39-268 ms -> 5-8 ms
- Backup / error handling
  - [x] SstFileManager (space limit, trash deletion, compaction space pre-check) + no-space auto recovery; atomic WAL-error recovery flush
- [x] Flaky-under-load hunt: LOCK release race (OFD lock shared with vforked children; unlock before close, as upstream), compaction zeroing seqnos of versions committed mid-compaction (current_key_committed_), WriteUnprepared rollback with two_write_queues visible before its commit (rollback batch now AddPrepared, deviation worth reporting upstream), compaction snapshot list taken at pick time (now with the job snapshot under the mutex); test-timing fixes for prefix-seek compat, blob auto-flush, write slowdown
- [x] Final-run regressions fixed: flush (closed WAL writers freed by the background purge, like logs_to_free_: 1 KiB flush 75.9 -> 39.0 ms, C 43.7), readahead via OS prefetch like BlockPrefetcher (seekrandom 1 KiB 16.6 -> 10.2 us, C 14.9), compaction allocations (74 -> 0.45 MB), subcompactions 4x: 294 ms vs C 354 (warm-up now JIT-quiesces compaction)
- [x] readrandom ~86 B/op / overwrite ~80 B/op explained: one-time block-cache fill and second-memtable arena growth on a fresh DB; steady-state Gets with block-cache misses allocate 0 B (unit test + BDN SstUncached); runner no longer counts its own histograms
- [x] Definitive benchmark (bench/results/2026-11-03-definitive-r5.*, same CPU as the previous run): managed 0.72x of C [0.71-0.74], async 0.73x, rocksdb-sharp 1.05x; compaction-heavy 0.58x; 2 of 232 managed cells >1.1x (noisy fillsync / async readmissing); hot paths allocate nothing (BDN)
- [x] 4-thread 100 B readmissing / fillrandom rechecked (7-rep interleaved A/B): noise, no regression; flush allocations: table-builder buffers pooled (TableBuilderBuffers), no longer bimodal
- [x] table readers pin index/filter blocks from the table buffer pool (flush 171 KB -> 40 KB); `BlobReadersBoundedByMaxOpenFiles` failure root-caused: bounded table cache sorted readers by a LastUse field mutated concurrently (List.Sort 'inconsistent comparer'), now sorts a snapshot
- [x] WAL write-error recovery with recycling on (deliberate deviation, user-accepted): 13 unit + 4 native-comparison tests incl. crash sweeps; fixed GetUpdatesSince returning the failed write (recovery-gap fence), tailing iterator missing records of a reused WAL, SstFileManager/flush deadlock
- [x] tailing reused WALs (secondary catch-up, GetUpdatesSince, replication): fixed three reader bugs copied from upstream's FragmentBufferedReader (spin on old record, bad-length block skip, buffer dropped at bad checksum); managed secondary follows a live recycling native primary (native's own secondary hangs there); RateLimiter statistics test made deterministic with a manual clock
- [ ] note: a replication tailer that falls a whole recycle cycle behind gets NotFound 'Gap in sequence numbers' and must resync (recycling keeps no WALs for tailers, as upstream) (by design)
- Out of scope by request: rocksdb-sharp arm64 / musl / jemalloc native builds (only linux-x64 built)

## Missing features wave (2026-11-03, requested)

- [x] PlainTable + CuckooTable (read/write, byte-identical regions vs native SstFileWriter, OPTIONS sections), adaptive reading by magic, user-defined index API
- [x] trie_index user-defined index (byte-identical trie blocks vs native 11.12, each side reads the other's through the trie, 0-alloc Gets); ingestion opens any table format and applies upstream's checks (plain/cuckoo files are rejected by the comparator/version checks, exactly as native rejects its own)
- [x] OPTIONS `user_defined_index_factory=trie_index` resolved to TrieIndexFactory; `index_mode`, `use_udi_as_primary_index`, `fail_if_no_udi_on_open` typed and written like native 11.12 (compat both ways)
- [x] Cache layers: SecondaryCache + adapter, CompressedSecondaryCache, TieredCache, persistent cache (volatile + file tier), WriteBufferManager (shared across DBs, cache charging, stalls; behaves like native's in compat tests), SimCache, CacheReservationManager; block-cache lookups unchanged when unused
- [x] cache layers: batched MultiGet secondary lookups, compressed admission to non-volatile (three-queue) tiers, async persistent-cache reads; index/filter blocks at table open now come from the cache (incl. secondary tiers) like upstream
- [x] Column family export/import, remote compaction (CompactionService + OpenAndCompact; job/result format interoperates with native both ways), GetMergeOperands, PromoteL0, SuggestCompactRange, DeleteFilesInRange(s), LockWAL, Disable/EnableManualCompaction, GetCreationTimeOfOldestFile, ClipColumnFamily, GetPropertiesOfTablesInRange, OpenAndTrimHistory (compat vs native), MultiScan
- [-] remote compaction leftovers (per-subcompaction jobs, blob compactions on the worker, paranoid_hash): out of scope (user decision)
- [x] Query tracing + Replayer, block cache and IO tracing (trace files interoperate with native both ways), RepairDB (compat both ways), tools/RocksDb.Managed.Tools with ldb and sst_dump commands
- [-] tools leftovers (TTL/timestamp/blob/transaction ldb commands, sst_dump on PlainTable/Cuckoo, recompress codec benchmark): out of scope (user decision)
- [-] Deviation inventory: not needed (user accepted the documented reasons in docs/validation.md)
- [ ] (upstream NotSupported, kept) timestamps in optimistic transactions and explicit-timestamp WBWI writes

## Gaps found while porting the utilities (2026-11-06)

- [x] Flaky `BlockCacheTierInsertLookupAcrossFilesAndEviction(False)`: test assumed Insert never returns TryAgain; product fixes: buffer wait no longer gives up after 100 ms (pipelined inserts were dropped), writer-thread I/O errors no longer crash the process (100/100 under CPU hogs)
- [ ] async MultiGet with block cache 2-4% slower after the batched secondary-cache lookups (consistent across runs, within one StdDev) (S)

- [ ] `max_successive_merges` is accepted but ignored (upstream folds merge operands into a Put at write time once the limit is hit) (S)
- [ ] `min_write_buffer_number_to_merge` is ignored: immutable memtables are flushed as soon as they are sealed (M)

## Second correctness review (2026-11-06; all items fixed 2026-11-06 with regression tests and native comparisons where observable; remaining deliberate deviations are listed in docs/validation.md (second-review sections) and README "Not ported yet"; 5 reviewers vs C++ 11.14 / native 11.12; evidence in each item's source refs)

P1/P2 confirmed bugs (reproduced):
- [x] avoid_flush_during_recovery: a flush during WAL replay doesn't advance the CF log number (upstream flushes all CFs at the end if any recovery flush happened, db_impl_open.cc MaybeFlushFinalMemtableOrRestoreActiveLogFiles) -> next open replays again, merge operands applied twice (DbImpl.Open.cs RecoverLogFilesAsync) (S)
- [x] rejected SetOptions writes live CF options before validating (DbImpl.Options.cs ValidateMutableCfOptions): bad protection-bytes value poisons later SetOptions/Flush and is persisted to OPTIONS; validate a copy and run ValidateOptions (FIFO ttl, temperature thresholds, max_read_amp) (S)
- [x] TtlDatabase has no TtlMergeOperator wrapper: merges see the 4-byte time suffix (wrong values / Corruption); native TTL DBs with merges misread; TtlDatabase lacks Merge and column families (S)
- [x] SingleDelete+Put dropped under a transaction snapshot: no write-conflict-boundary snapshots (CompactionJob earliestWriteConflict = Max) -> later conflicting txn write succeeds instead of Busy (M)
- [x] ReadTier: BlockCacheTier ignored (Get/MultiGet/iterators do I/O, upstream Incomplete); MemtableTier inverted (Get/MultiGet skip SSTs, iterators don't; Get throws Corruption); PersistedTier iterators should be NotSupported (S)
- [x] SetOptions on struct options (compression_opts, compaction_options_universal/fifo): partial update resets other fields, unknown sub-fields accepted and persisted, dotted names rejected (S)
- [x] can't open a native DB with the `___rocksdb_stats_history___` CF (persist_stats_to_disk); stats persistence options are no-ops (S/M)
- [x] `rocksdb.estimate-num-keys` clamps memtable and table parts separately (native 50, managed 100 after deletes) (S)
- [x] CompactionFilter.IgnoreSnapshots=false accepted (upstream NotSupported) (S)
- [x] ReFitLevel: missing "Moving more than 1 file from non-L0 to L0" Aborted check; files not marked being-compacted while the mutex is released (race with concurrent CompactRange -> Corruption bg error) (S)
- [x] native embedded-blob SSTs (SstFileWriter::OpenWithEmbeddedBlobs, blob file number 0) fail with Corruption: port or NotSupported at open + README (suspected, native C API can't produce one) (M)

Unported / silently ignored (undocumented):
- [x] ~35 options native 11.12 writes are untyped (memtable_max_range_deletions, paranoid_memory_checks, disallow_memtable_writes, strict_max_successive_merges, memtable_op_scan_flush_trigger, compression_opts.auto_skip, max_manifest_space_amp_pct, daily_offpeak_time_utc, background_close_inactive_wals, metadata_cache_options, ...); strict OptionsFile.Load fails on every default native OPTIONS file (M)
- [x] WriteOptions.LowPri (ThrottleLowPriWritesIfNeeded, low_pri_rate_limiter) (S)
- [x] ReadOptions.Tailing, ValueSizeSoftLimit, per-read VerifyChecksums, Deadline/IoTimeout (S each)
- [x] optimize_filters_for_hits; verify_compression, detect_filter_construct_corruption, read_amp_bytes_per_bit parsed but unused (S each)
- [x] VersionBuilder consistency checks + force_consistency_checks; compaction OutputValidator (out-of-order keys) + paranoid_file_checks (S/M)
- [x] oldest_ancestor_time of compaction outputs too old (not narrowed per output) -> early TTL recompaction (S)
- [x] MANIFEST size auto-tuning (max_manifest_space_amp_pct, kLastCompactedManifestFileSize record) (S)
- [x] db_paths / CompactRangeOptions.TargetPathId placement; periodic obsolete-file full scan (delete_obsolete_files_period_micros); trivial copy for temperature change (S each)
- [x] trivial moves don't advance the round-robin compact cursor (upstream now does; validation.md claim is wrong) (S)
- [x] MergeOperator OpFailureScope (kMustMerge) and FullMergeV3 (S/M)
- [x] Transaction: MultiGet / MultiGetForUpdate, public SetLockTimeout, assume_tracked overloads, Disable/EnableIndexing, GetNum* counters, GetElapsedTime (S)
- [x] DestroyDB, public CancelAllBackgroundWork, OnDBShutdownBegin; SstFileReader; row_cache; PerfContext / IOStatsContext (S-M each)
- [x] ~15 properties missing (compression-ratio-at-level, *-write-stall-stats, num-running-bottom-compactions, min-log-number-to-keep, ...); num-files-at-level out of range returns "0"; total-sst-files-size ignores pinned versions (S)
- [x] statistics: seeks inflate BLOOM_FILTER_PREFIX_USEFUL, SEEK_* tickers always 0, *_TRUE_POSITIVE never recorded (S)
- [x] EVENT_LOG: table_file_deletion, blob_file_*, trivial_move, recovery_*, ingest_finished not written (S)

Minor divergences:
- [x] write group stops at the first incompatible writer (upstream keeps scanning with an r_list): throughput only (S)
- [x] SetDBOptions: no immediate SwitchWAL on lower max_total_wal_size / wal_bytes_per_sync, rewrites OPTIONS on no-op, no per-CF validation (S)
- [x] max_open_files not clipped to [20, OS max]; delayed_write_rate=0 with a rate limiter uses 16 MiB, not the limiter rate (S)
- [x] Raft follower commit index uses min(leaderCommit, myLatest) instead of prevLogSeq (inherited from rocksdb-sharp; CommitSeq is status-only today) (S)
- [x] docs: validation.md claims readers never see a partial write group; false on memtable insert failure (published, no fatal bg error) -- fix behaviour (HandleMemTableInsertFailure) or the doc (S)

Missing tests (all added): ReadTier matrix; TTL + merge; avoid_flush with full memtable during recovery; SingleDelete under txn snapshot through compaction; IgnoreSnapshots=false; ReFitLevel guards; MANIFEST with overlapping L1 files; oldest_ancestor_time on multi-output compactions; SEEK_* tickers; txn/WBWI merges over a DB merge chain spanning memtable+SST.
Follow-up review: IsInSnapshot, RangeLockTree and the multi-CF iterator checked consistent; WritePrepared AddCommitted now caps max_evicted_seq by the last published seq (two_write_queues). Not reviewed: the rest of WritePrepared/WriteUnprepared recovery.

Still open from this round:
- [ ] `max_successive_merges` (and `strict_max_successive_merges`), `paranoid_memory_checks`, `memtable_op_scan_flush_trigger` and the other typed-only options listed in README "Not ported yet" (S-M each)
- [ ] flush does not run the merge helper / compaction filter (TTL merge results are stamped at the first compaction, flush filtering not applied) (M)
- [ ] paranoid_file_checks read-back of flush outputs; MANIFEST size check on close (S)

## Combinatorial round trips (2026-11-06)

- [x] Pairwise option/pattern matrix (79 cases, long 279), crash matrix (20, long 80), 2PC engine-switch suite; seeded, see docs/testing.md
- [x] Get / MultiGet (sync + async) stopped at the first file of a level when a key's versions span the file boundary (FilePicker::GetNextFile); fixed + `DbMultiGetTests.KeyVersionsSpanningFilesOfALevel`
- [x] TransactionDB::Write accepted SingleDelete (non-default CF), DeleteRange, TimedPut and blob indexes that upstream LockBatch rejects; fixed + `TransactionTests.WriteBatchWithUnsupportedRecordsIsRejected`
- [ ] Multi-CF `Flush(FlushOptions, IReadOnlyList<ColumnFamily>)` overload (rocksdb_flush_cfs equivalent) (S)
- [ ] Native 11.12 bugs found (same code on main; could be reported upstream): unordered_write + manual_wal_flush self-deadlock on wal_write_mutex_; DBIter::MergeWithBlobBaseValue returns freed memory; hash index / hash skiplist call Transform without InDomain (see docs/validation.md "Combinatorial round trips"; ready-to-file reports with C API repros in docs/upstream-issues/)
- [ ] Round-trip matrix not yet crossing: wide columns (no C API PutEntity in 11.12), preclude_last_level_data_seconds, separate_key_value_in_data_block, db_paths / cf_paths, last_level_temperature

## Allocation-free pass (2026-11-07; tool: tools/RocksDb.Managed.AllocationProfile, user guide: docs/allocations.md)

- [x] Profiler tool (Memory.Introspect allocation ticks + exact B/op per scenario, 121+ scenarios, child process per scenario)
- [x] Iterators: pooled iterator stacks per CF (NewIterator+Dispose 2.1-4 KB -> 56 B handle), `Iterator.Refresh` / `Refresh(Snapshot)` / `RefreshAsync`, `SetBounds`, cached ScanAsync enumerator; base-delta and transaction iterators pooled
- [x] Writes / transactions: concurrent async write-group leader without state-machine boxes (OwnedValueTaskMethodBuilder), transaction resource reuse, optimistic commit and 2PC SetName 0 B, `WriteBatchWithIndex.Rent` / `CreateWriteBatchWithIndex`, undisposed `new WriteBatch()` no longer drains the shared pool
- [x] Range tombstones: closure/lambda-free fragmenter (differential tests), flush / compaction / range-del block without per-tombstone keys or list growth; per-reader tombstone cache lookup without a closure
- [x] Background: EVENT_LOG / LOG formatted only when enabled, table-property string interning, pooled file write buffers, long-lived background thread pool (BackgroundThreadPool), pooled dictionary-compression blocks, cached compression-options string, version installation and filter buffers
- [x] Reads / monitoring / open: pooled legacy MultiGet (20 KB -> 2.2 KB per 16 keys), caller-memory `MultiGet` (+async) 0 B, lazy-entity pin reuse, integer property path (`TryGetIntProperty` 0 B), cached `ColumnFamilies`, `VisitLiveFiles` / list-refill `GetLiveFilesMetadata`, span `GetApproximateSizes`, non-allocating `GetHistogramData`, lazy HyperClock slot table (open 2.7 MB -> 0.75 MB)
- [ ] Open still lists the DB directory more than once (sharing one listing across WAL recovery and obsolete-file scans is unsafe; a few KB per open)
- [x] Features (64 scenarios, Scenarios/Features.cs): attribute-group Get / MultiGet 0 B (were 1.8-18.8 KB; reused internal snapshot for multi-CF consistency) + `GetAttributeGroupsAsync` / `MultiGetAttributeGroupsAsync`; MultiScan 1080 -> 104 B; coalescing-iterator direction switch 0 B; block-cache / IO trace records 0 B; `TryGetFullHistoryTsLow(IBufferWriter)` 0 B; pooled async builders for merge operands, TTL writes, MultiScan; TTL, Plain/Cuckoo reads, blob reads, rate limiter, query tracing, UDT and merge reads already 0 B
- [ ] Features left as is: NewCoalescingIterator ~3 KB per iterator (reuse it), rate-limiter wait ~216 B, async Get under block cache tracing 168 B (AsyncLocal), remote-compaction worker open ~1 MB per job
- [x] Replication / WAL tailing (32 scenarios, Scenarios/Replication.cs): pooled `AsyncBroadcastSignal` for sequence / commit / progress waits (tailing waits ~520 -> ~25 B), pooled async builders across WalTailer / consumer / hosts, WAL listing only after rotation (TransactionLogIterator, ReplicationSource retirement), Raft quorum and heartbeat answers 0 B, lockstep replica round trip 2.3 KB -> <200 B, secondary idle catch-up 7.1 -> 1.4 KB; `RocksDatabase.CurrentWalNumber`
- [ ] Replication left as is: GetUpdatesSince per poll ~3 KB (hold one tail instead), idle TryCatchUpWithPrimary ~1.4 KB (directory listing + CURRENT), Raft leader heartbeat per interval, MagicOnion / gRPC DTOs, checkpoints

## Unported utilities (found 2026-11-03)

- [x] Built-in merge operators: max, put / put_v1, sortlist, bytesxor, StringAppendTEST, AggMerge (+ Aggregator API); resolved by name from OPTIONS; native writes / managed reads and compacts and vice versa for 12 ids; string-append delimiter now persisted like native
- [-] Encrypted file system (env_encryption.h): out of scope (user decision)
- [x] WalFilter (recovery replay rewritten as a port of ProcessLogFiles), track_and_verify_wals predecessor-WAL records written + verified (byte-identical with native both ways), UserWriteCallback; WAL recovery now follows upstream for paranoid_checks=false and point-in-time stops
- [x] WAL recycling at DB level (recycle list, in-place reuse, recyclable records, option rules; native recovers managed recycled WALs and vice versa)
- [x] WAL settings x failure-mode matrices: unit 300 scenarios (15 factors pairwise x 15 faults; long 6,299), compat 1,152 managed-vs-native scenarios (long 36,864); 7 managed/native disagreements found and fixed (corruption messages, seqno-set-backwards check, 2PC WAL retention, 2PC after point-in-time stop, blob direct-write crash images, reused-WAL stale tail)
- [x] GetAllKeyVersions (pooled, 0-alloc enumeration), OptionChangeMigration (all of option_change_migration_test.cc; native opens migrated DBs), MemoryUtil (+ fixed `estimate-table-readers-mem` and `size-all-mem-tables`), InfoLogFinder (finds native LOG files)
- [x] Cache dump / load (cache_dump_load.h): dump file byte-identical to upstream's layout, LRU and HyperClock, `BlockCache.ApplyToAllEntries`; no native comparison possible (no C API)
- [x] TransactionDBMutexFactory (`TransactionDbOptions.CustomMutexFactory`; default lock path unchanged within noise, PointLockBenchmarks)
- [x] EnvMirror as `MirrorFileSystem` (env_mirror_test.cc ported + DB workload); StackableDB has no C# equivalent (transactions / TTL are built in)
