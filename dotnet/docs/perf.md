# Performance notes (table / cache / memtable / util hot paths)

This file records how the hot paths of `RocksDb.Managed` were measured and tuned, with before/after numbers.
Update it with every perf-sensitive change.

## Method

- BenchmarkDotNet (`bench/RocksDb.Managed.Benchmarks`), `net10.0`, Release, `--job short --iterationCount 8
  --warmupCount 3`, `[MemoryDiagnoser]` on every class.
- **Machine caveat:** a shared 4-vCPU cloud VM ("Intel Xeon @ 2.80GHz", AVX-512, SSE4.2, PCLMUL) with other agents
  running builds and benchmarks at the same time (load average 3-4). Run-to-run noise is ±10-20% (occasionally
  more). Absolute numbers are therefore inflated and only ratios measured in the same window are meaningful.
- "Before" = the tree at `db74408` (phase 2 contracts), built in a separate directory. Before/after runs were
  **alternated** (base, new, base, new) in the same time window and the tables report the **median of the per-run
  means**. Results within ±10% are noise.
- Profiling: BenchmarkDotNet's EventPipe profiler (`--profiler EP`) produces a speedscope file; self time per frame
  was summarized with a small script. `--disasm` was used to inspect the JIT output of `BlockIter.ParseNextKey`.

Reproduce:

```bash
export PATH=/root/.dotnet:$PATH
cd dotnet
dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- \
  --filter '*.TableBenchmarks.*' '*SkipListBenchmarks*' '*MemTableBenchmarks.Managed*' '*UtilBenchmarks*' \
  --job short --iterationCount 8
```

## Results

Median of 2 alternated runs each (before = `db74408`, after = this change). The "before" cached table benchmarks used
a lock-protected `Dictionary` behind the old `IBlockCache` seam; "after" uses the real HyperClock cache
(`BlockCache.CreateHyperClock`), and the dictionary variant is kept as `legacy IBlockCache seam` for an
apples-to-apples comparison.

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `UtilBenchmarks.Crc32C4K` | 641.5 ns | 279.3 ns | **-56%** |
| `TableBenchmarks` Block: full scan (~50 entries) | 1106.3 ns | 556.1 ns | **-50%** |
| `TableBenchmarks` Block: Seek | 289.4 ns | 220.9 ns | **-24%** |
| `TableBenchmarks` Index: Seek (top-level index, ~3k entries) | ~406 ns¹ | 380.5 ns | ~-6%¹ |
| `TableBenchmarks` Table: Get (cached, dictionary through `IBlockCache`) | 1008.2 ns | 1066.8 ns | +6% (noise) |
| `TableBenchmarks` Table: Get (cached, HyperClock) | 1008.2 ns² | 1188.5 ns | +18%² |
| `TableBenchmarks` Table: Get (block read + checksum per op) | 1510.5 ns | 1417.9 ns | -6% |
| `TableBenchmarks` Table: Seek + 100 x Next (cached) | 7685.1 ns | 8126.2 ns | +6% (noise) |
| `MemTableBenchmarks.ManagedGet` (100k keys) | 1745.5 ns | 1588.0 ns | -9% |
| `MemTableBenchmarks.ManagedAdd` | 1449.0 ns | 1380.5 ns | -5% |
| `SkipListBenchmarks.ContainsRandom` (200k u64 keys)³ | 935.5 ns | 796.2 ns | (noise) |
| `SkipListBenchmarks.InsertRandom`³ | 500.4 ns | 531.0 ns | (noise) |
| `UtilBenchmarks.DecodeVarint32x1000` (1-3 byte varints) | 3691.7 ns | 3775.9 ns | +2% (noise) |
| `UtilBenchmarks` compare 24-byte keys: `SequenceCompareTo` vs `ByteCompare` | 6.5 ns | 5.0 ns | -23% |

¹ The index benchmark was added during this work; "before" is the first measurement with the old `BlockIter`.
² Different caches: the single-threaded benchmark is the best case for an uncontended lock (its cache line stays in
L1), while a HyperClock hit does two atomic adds on the probed slot's cache line (acquire + release) and one more
dependent load (slots are heap objects). Under concurrency HyperClock wins clearly, see below.
³ The skiplist benchmark uses its own `u64` comparer, so none of the changes apply; the differences show the noise
level of this machine.

Concurrent cached `Get` (4 threads on one reader, all blocks resident; `TableConcurrentGetBenchmarks`, wall time per
Get, single run):

| Cache | Per Get |
|---|---:|
| HyperClock (`BlockCache.CreateDefault`) | **370.5 ns** |
| lock + `Dictionary` (legacy seam) | 531.9 ns |
| sharded LRU (`BlockCache.CreateLru`) | 596.7 ns |

Block cache memory: a 4 KiB-`block_size` data block (~4.1 KiB with trailer) is now charged 5 KiB instead of 8 KiB
(`BlockBufferPool`), i.e. the same cache capacity holds ~1.6x as many blocks.

All benchmarked hot paths report `Allocated = -` (zero bytes per operation) before and after.

## What changed and why

### Table point lookup (cached `Get`)

Profile of `Table: Get (cached blocks)` before tuning (self time): data block seek ~35% (`ScanBlockForGet` +
`ParseNextKey`), `SequenceCompareTo` ~13%, block cache lookup ~10%, index seek (inlined into `Get`) most of the rest.
The benchmark's working set is ~3000 cached 4 KiB blocks (24 MiB before the pool change) plus a ~90 KiB index, so a
large part of the remaining ~0.9 µs is cache/TLB misses (index binary search: ~12 dependent probes; data block:
restart array + entries; cache slot). Checksums are **not** re-verified on a hit: cache entries hold the parsed,
verified, decompressed `Block`.

- `BlockIter.ParseNextKey`: entry header (shared / non-shared / value length) decoded without bounds checks into
  registers (the old `out`-parameter decoder forced the three values to the stack and zero-initialized them); key
  delta copies use two overlapping word moves for 4..16 bytes instead of `Buffer.BlockCopy` (type checks +
  `memmove` call). Slow path (multi-byte varints, index blocks with delta-encoded values) is a separate method.
  `Block: Seek` and `Block: full scan` improved ~25-40%.
- `ByteCompare.Compare`: inlined 8-bytes-at-a-time big-endian comparison for keys up to 64 bytes (falls back to
  `SequenceCompareTo`), used by `InternalKeyComparator` (bytewise / reverse bytewise) and the memtable key comparer.
  ~10% faster than `SequenceCompareTo` on 24-byte keys with a 20-byte common prefix (`UtilBenchmarks.Compare*`).
- `[SkipLocalsInit]` on `BlockIter` and `BlockBasedTableReader`.
- Uncached `Get` (block read per op) is ~6% faster: smaller pooled buffers and fewer copies; checksums are still
  verified on every read from the file.
- Block cache: `GetBlockRef` returns a `BlockRef` (cache handle or owned block). A hit costs one HyperClock probe
  plus one atomic add to acquire and one to release; no `Block.AddRef`/`Release` pair on top.

Not changed (investigated): the index binary search itself is a faithful port of
`BlockIter::BinarySeekRestartPointIndex` + `FindKeyAfterBinarySeek` (including decoding the entry before the answer,
as upstream does); remaining cost is memory latency. HyperClock slots are separate heap objects (two dependent loads
per probe vs. one in upstream's inline `ClockHandle` array) — converting the table to a struct array is a possible
follow-up (see gaps).

### Block cache memory efficiency

`ArrayPool<byte>.Shared` rounds to powers of two, so a typical data block (4096+ bytes + 5-byte trailer) occupied an
8 KiB array and was charged 8 KiB: a block cache could only hold about half of its capacity in block bytes.
`Memory/BlockBufferPool` adds four size classes per power of two above 4 KiB (≤25% padding; a 4101-byte block gets a
5 KiB array) and is used for every buffer that becomes a `Block`. Decompression no longer pre-rents
`2 × compressed` and grows (copying) — the decompressors request the exact uncompressed size once.
`BlockCache.EstimateBlockCharge(blockSize)` reports the resulting per-block charge for HyperClock sizing.

### Skiplist / memtable

Profiles of `SkipListBenchmarks.ContainsRandom` (200k random u64 keys) and `MemTableBenchmarks.ManagedGet` (100k
16-byte keys, 100-byte values) show everything inlined into one loop (comparer, `Volatile.Read` — a plain load on
x64/arm64 — and the prefetch); the time is memory latency: ~8 levels × ~1.5 nodes per level of dependent loads over a
randomly laid out 10-30 MiB arena. The node layout, height distribution (xorshift, branching 4) and the
`Prefetch0(next->Next(level))` hint are the upstream ones. Native rocksdb-sharp on the same box: memtable Get ~2.9 µs
(includes P/Invoke + result allocation) vs ~1.6 µs managed. The only change here is the faster user-key comparison.

### Checksums and varints

- `Crc32C.Extend`: for buffers ≥ 768 bytes, three interleaved 256-byte streams (the `crc32` instruction has a latency
  of 3 cycles but a throughput of 1/cycle) combined with a 4 KiB byte-sliced "shift by 256 zero bytes" table, like
  upstream `crc32c_3way`. Applies to block reads with `kCRC32c` and large WAL records. Verified against a bytewise
  reference for all lengths 0..1556 at several alignments.
- XXH3 (the default checksum) uses `System.IO.Hashing.XxHash3`, already vectorized.
- `Coding.DecodeVarint32/64`: unrolled, bounds-check-free decode when the maximal encoding is in range; one-byte
  fast path for varint64 (index block handles).

### Filters

`FastLocalBloom.MayMatch` touches exactly one cache line computed from the hash; a prefetch only pays off when the
probe address is known well before the probe (upstream does this in the batched MultiGet path). There is no batched
lookup API yet, so nothing was changed (single-key `MayMatch` is 15-25 ns on this box).

## JIT settings

The library keeps the .NET defaults: tiered compilation with Dynamic PGO (on by default since .NET 8) gives the
best steady-state code for the interface/virtual call sites (comparators, filter readers, `IBufferWriter<byte>`),
which PGO guarded-devirtualizes. ReadyToRun is a property of the consuming application's publish
(`PublishReadyToRun`), so it is not set in the library project; applications that care about startup can enable it
— R2R code is still re-jitted at tier 1 for hot methods. `TieredCompilation=false` is not recommended (loses PGO).

## Prefix seek, partitioned index/filters, dictionary compression (table features round)

These touch the read hot paths: the table Get filter check (now `InternalKeyMayMatch`: a switch on the filter
kind, optional prefix path), the table iterator Seek (one `CheckFilter` branch), `DbIter`'s forward step (one
`_hasPrefix` branch) and `BlockFetcher` (dictionary argument). Two alternated runs each of `TableBenchmarks` and
`IteratorBenchmarks` (before = `acd00d9`, same window, `--job short --iterationCount 8`) show every difference within
the box's noise (the unchanged `Block:` benchmarks moved by up to ±20% between runs of the same build):

| Benchmark | before (run 1 / run 2) | after (run 1 / run 2) |
|---|---:|---:|
| Table: Get (cached blocks) | 989 / 1774 ns | 1194 / 1144 ns |
| Table: Get (block read+checksum per op) | 1382 / 1961 ns | 1447 / 1487 ns |
| Table: Seek + 100 x Next (cached) | 7282 / 8981 ns | 7879 / 7991 ns |
| Index: Seek | 335 / 391 ns | 385 / 379 ns |
| IteratorBenchmarks ManagedSeekNext100 | 36430 / 32147 ns | 30012 / 32603 ns |
| IteratorBenchmarks ManagedFullScan | 244 / 193 ns | 193 / 224 ns |

All hot paths still report `Allocated = -`. Dictionary compression allocates per table (buffered block copies,
samples) and per data block with LZ4 dictionaries (a native LZ4 stream context); ZSTD dictionaries are digested once
per table (CDict / DDict) and use thread-local contexts.

## Perf pass #2 (async allocations, batched MultiGet, reverse iteration, level iterator, flush/compaction)

Inputs: the three-way results `bench/results/2026-11-02-vm.md` / `-vm-compaction.md` (before) and
`bench/results/2026-11-02-vm-perf2.md` / `-vm-perf2-compaction.md` (after; reduced sizes: num=300k, 60k for 1 KiB
values; same shared 4-vCPU VM, load 4-18 during the runs). "Before" = `60888de`, built in a separate directory;
before/after runs were alternated in the same window (dbbench runner and BenchmarkDotNet `--job short
--iterationCount 8`), medians reported. Allocation-tick sampling (`EventListener` on `GCAllocationTick`) and
`dotnet-trace` (sampled thread time) located the allocations and hot spots.

### 1. Async API: synchronous completion when nothing has to wait

Causes: (a) every async WAL append went through `RandomAccess.WriteAsync`, which on Unix runs a blocking `pwrite` on a
thread-pool thread (one pool hop plus a non-pooled state machine box per write: ~150 B and ~8 µs per `PutAsync`);
(b) every async block read went through `RandomAccess.ReadAsync` (same pool hop) and a non-pooled
`BlockFetcher.ReadBlockAsync` box (~230-380 B per `TryGetAsync` / async seek missing the block cache); (c) the
decompression writer object was allocated per compressed block read.

Fixes (`Env/UnixNative.cs`, `Env/LocalFileSystem.cs`, `Table/BlockFetcher.cs`):
- Async reads first try `preadv2(RWF_NOWAIT)`: a page cache hit completes synchronously (a short read continues
  asynchronously, EAGAIN falls back to the async read). Async writes use `pwritev2(RWF_NOWAIT)` where the file system
  supports buffered NOWAIT writes; otherwise (ext4 here) buffered writes up to 256 KiB (WAL records, write-buffer
  flushes) are written inline, like native RocksDB and like the inline `fdatasync` of `SyncAsync`. Larger writes,
  direct I/O and rate-limited writes keep the thread-pool path.
- `BlockFetcher.ReadBlockAsync` has a synchronous fast path and a `PoolingAsyncValueTaskMethodBuilder` slow path; the
  decompression `RentedBufferWriter` is per thread.

| (1 thread, 100 B values unless noted) | before µs/op | before B/op | after µs/op | after B/op |
|---|---:|---:|---:|---:|
| `PutAsync` (probe, WAL on) | 13.5 | 300 | 6.4 - 8.6 (sync `Put` 4.9 - 6.3) | 147 (memtable arena, same as sync) |
| `TryGetAsync`, block from page cache (1 MiB block cache) | 14.4 | 256 | 4.0 | 0 |
| `SeekAsync` + 10 `NextAsync`, page cache | 13.9 | 376 | 6.2 | 0 |
| matrix fillseq, managed-async | 10.34 | 320.6 | 3.55 | 143.5 |
| matrix readrandom, managed-async | 20.73 | 230.0 | 8.36 | 47.6 (cache fill, same as sync) |
| matrix seekrandom, managed-async | 41.06 | 818.2 | 21.24 | 0.3 |
| matrix readwhilewriting, managed-async | 21.68 | 431.1 | 8.00 | 0.5 |
| matrix seekrandom 1 KiB, managed-async | 95.96 | 2418.7 | 37.38 | 0.3 |
| matrix readseq 1 KiB, managed-async | 5.98 | 130.5 | 2.34 | 0.3 |

Matrix summary (geometric mean of µs/op vs c-native), managed-async: before 1.06x - 1.86x, after 0.64x - 1.17x; async is
within noise of the sync API in every configuration. BenchmarkDotNet `DbAsyncBenchmarks` (after, per op):

| op | BlockCache sync | BlockCache async | PageCache sync | PageCache async | Allocated |
|---|---:|---:|---:|---:|---:|
| Put / PutAsync | 4.19 µs | 4.98 µs | 4.90 µs | 5.10 µs | - |
| TryGet / TryGetAsync | 1.25 µs | 1.52 µs | 3.38 µs | 5.07 µs | - |
| Seek + 10 Next | 1.83 µs | 2.50 µs | 4.70 µs | 4.97 µs | - |
| MultiGet(16), per batch | 23.1 µs | 21.3 µs | 51.6 µs | 52.7 µs | - |

### 2. Batched MultiGet: `MultiGetBatch`, `MultiGet(batch)`, `MultiGetAsync(batch)`

New API (`MultiGetBatch.cs`, `Db/DbImpl.MultiGet.cs`, `Table/BlockBasedTableReader.MultiGet.cs`): a reusable batch owns
the keys (`Add(ReadOnlySpan<byte>)`), the per-key results (`GetStatus` = `Found` / `NotFound` / `Error`, `GetValue`
span, `GetError`) and all scratch, so steady-state calls allocate nothing. Port of `DBImpl::MultiGetImpl` +
`Version::MultiGet` + `BlockBasedTable::MultiGet`: one super version and one sequence number for the batch, keys sorted
with the column family comparator (non-allocating sort), memtables per key, then every table file is visited once with
the sub-batch of keys in its range (L0 newest first; on sorted levels consecutive keys of one file are grouped); the
filter is checked for the whole sub-batch before any data block is read, and consecutive keys in the same data block
share one block lookup. `MultiGetAsync` reads missing data blocks asynchronously (same code, `synchronous: false`).
Per-key errors (corrupt entries, `ReadTier.MemtableTier` with an incomplete merge) are reported per key; I/O errors fail
the call. Statistics: `NUMBER_MULTIGET_*` tickers, `BYTES_PER_MULTIGET` and `DB_MULTIGET` histograms. The old
`byte[]?[] MultiGet(IReadOnlyList<byte[]>)` is kept, implemented on the batch.

| | before | after |
|---|---:|---:|
| `DbMultiGetBenchmarks` MultiGet(16), cached SST (2 runs) | 22.9 / 23.3 µs, 2384 B | 19.6 / 22.1 µs, - |
| matrix multireadrandom 100 B, alloc | 132 B/op | 0.4 B/op |
| matrix multireadrandom 1 KiB, alloc | 929 B/op | 0.5 B/op |

### 3. Reverse iteration

`BlockIter.Prev` caches the entries decoded by its restart-interval scan (port of `DataBlockIter::PrevImpl`'s
`prev_entries_`), so a run of `Prev` decodes each restart interval once instead of once per entry (data and meta blocks;
index blocks keep the plain scan). Fully cached 100-byte values, one `Prev` per op (probe, 3 alternated runs): 0.368 /
0.444 / 0.325 µs before, 0.258 / 0.351 / 0.220 µs after (about -30%); 1 KiB values (4 entries per block): unchanged.

Standalone runs (fresh process on an existing DB, 8 MiB cache) put managed reverse iteration at parity with C: on 9 L0
files `readreverse` 0.67 - 1.08 µs managed vs 1.07 - 1.50 µs c-native. The remaining matrix gap (readreverse 100 B:
1.43x) comes from how the matrix runs: each benchmark runs once, and `readreverse` is the first user of the Prev code
paths, which run as tier-0 / instrumented code for a large part of the short run (same binary, same DB: 1.9 µs/op on the
first pass, 0.6 µs/op on the second). Iteration over 1 KiB values is at parity (readseq 0.87x, readreverse 1.01x,
seekrandom 0.84x). Not done: values are still copied when iterating backwards (`DbIter.FindValueForCurrentKey`, ~18% of
reverse time with 1 KiB values); upstream pins blocks with `PinnedIteratorsManager` instead.

### 4. Level iterator: no allocation per file switch

On a multi-level LSM, every seek landing in another file of a level created a new `TableInternalIterator` +
`BlockBasedTableIterator` + `IndexCursor` + 3 `BlockIter`s (500-1300 B per seek). `LevelIterator` now recycles its file
iterator (`TableInternalIterator.Retarget`, `ITableCache.NewIterator(file, reuse)`); an exhausted level iterator still
releases its file (`Park` drops the lease and the blocks and keeps the objects).

| seekrandom on the compaction-heavy LSM (`0 0 0 0 0 3 14`) | before | after |
|---|---:|---:|
| probe Seek + 10 Next | 13.7 µs, 507 B | 5.5 µs, 0 B |
| dbbench seekrandom (2 alternated runs) | 13.6 / 13.8 µs, 560 B | 12.0 / 12.5 µs, 49 B (cache warm-up) |
| compaction config seekrandom, managed | 25.22 µs, 1099 B | 21.79 µs, 0.2 B |

### 5. Flush and compaction (investigated; nothing kept)

- Flush is 1.4 - 2x slower than c-native (400k x 100 B: 0.83 - 1.0 s vs 0.4 - 0.6 s, alternated runs). Instrumented
  phases: memtable iteration ~120 ms (randomly ordered skiplist nodes, cache misses), block building 160 - 235 ms, file
  appends 37 ms with the WAL disabled but 290 - 370 ms with it on (page cache writes behind ~50 MB of dirty WAL pages),
  finish 40 ms, fsync 60 - 80 ms. The steady-state in-process build is ~0.6 µs/entry, the first build in a process
  ~2 µs/entry (also with `TieredCompilation=0`, so first-touch of pooled buffers and new page cache pages, not JIT). The
  2 - 10 MB the matrix's single flush per process allocates are pool warm-up (later flushes in the same process: 20 KB).
  A write-behind wrapper (page cache copy of chunk N overlapping with building chunk N+1) gave no measurable gain over
  16 alternated flushes and was reverted.
- Compaction: under `overwrite` with a 4 MiB write buffer managed is now faster than native in the foreground
  (`-vm-perf2-compaction.md`: overwrite 3.39 µs vs 5.14 µs c-native; before 12.29 vs 5.49 µs) with the same LSM shape and
  write-amp (2.96 - 3.15), but leaves more background work behind (waitforcompaction 0.6 - 0.8 s vs 0.02 - 0.18 s), i.e.
  background compaction throughput is lower: a full `CompactRange` of 9 L0 files (58 MB) takes 0.9 - 1.0 s managed vs
  0.4 - 0.6 s c-native. Job limits match upstream (`max_background_jobs = 2`: 1 flush + 1 compaction; native runs one
  subcompaction by default, so range-partitioned subcompactions would not change this comparison). Compaction input
  readahead through `FilePrefetchBuffer` (`compaction_readahead_size` = 2 MiB) gave no gain here (inputs are in the page
  cache; the extra copy costs what the saved `pread`s save) and rented 4 MiB per input, so it was reverted. The profile is
  spread over the merging iterator heap, `CompactionIterator`, block checksums and the table builder.

Remaining gaps: flush / compaction CPU per entry 1.4 - 2x native; background compaction throughput under sustained
writes; `fillsync` and `flush` in the matrix carry first-run effects; reverse iteration copies values (no block pinning).

## Perf pass #3 (flush and compaction throughput)

Inputs: alternated runs of the pre-pass build (`ed896fe`, copied aside) / the new build / c-native in the same window,
medians reported; the shared 4-vCPU VM had load 3-14 from other agents throughout, so single runs vary by +-30%. New
three-way results: `bench/results/2026-11-02-vm-perf3.md` (reduced matrix) and `-vm-perf3-compaction.md`.

Profiling notes. EventPipe sampling (`dotnet-trace`, BenchmarkDotNet `--profiler EP`) is badly biased on Linux: threads
are sampled at GC safe points, so native calls (`pread`, `pwrite`, large `memmove`) dominate the profile (pass #2 saw
"page cache writes behind dirty WAL pages" this way; the actual cause was elsewhere, see 1). This pass used `perf record`
with `DOTNET_PerfMapEnabled=1 DOTNET_EnableWriteXorExecute=0` (perf from `linux-tools-6.8.0-*-oem` works in the VM),
windowed with `--control=fifo` to the compaction under test, `getrusage(RUSAGE_THREAD)` around it, and a TraceEvent
reader for allocation-tick stacks and JIT events. In-process probes repeated one flush / one compaction N times so the
first run (what the matrix measures) and the steady state could be separated.

Findings:
- In steady state the managed builder and compaction are already at or below native cost: building a 400k-entry table
  through `SstFileWriter` takes 0.45 - 0.55 µs/entry managed vs 0.46 - 0.8 µs native; a full `CompactRange` of 9 L0
  files (58 MB) repeated in one process takes 230 - 280 ms managed vs 400 - 600 ms for c-native's (only) run.
- The first flush / compaction of a process is 2.5 - 3.5x slower than the following ones: ~1100 methods are JIT
  compiled during the first compaction and the per-entry code runs as tier-0 / instrumented code until tiering catches
  up (the call-counting delay keeps being extended while new methods are jitted). On the compaction thread: user CPU
  785 ms on the first run vs 130 ms on later runs, perf shows 20% libclrjit + 15% libcoreclr. ReadyToRun or
  `TieredCompilation=0` remove only part of it (610 - 750 ms) - type loading, first-touch of pooled buffers and the JIT
  of methods R2R cannot precompile remain.
- The WAL-on flush gap was ours: see 1.
- Under sustained writes, every new output file allocated large arrays (LOH) that `ArrayPool<T>.Shared` failed to
  hand back across the flush / compaction threads: 51 - 60 B per put and 2 gen2 GCs per 500k puts.

### 1. WAL writeback before the rotation sync

`SwitchMemtableAsync` fdatasyncs the closed WAL before writes continue in the new one (the port's stronger-than-upstream
guarantee). With 50 MB of WAL that had never been written back, this sync took ~155 ms on the write path, and the
`flush` benchmark (which includes the switch) paid for it. A WAL now starts writeback every 1 MiB with
`sync_file_range(SYNC_FILE_RANGE_WRITE)` when `wal_bytes_per_sync` is 0 (`DbImpl.CreateWal`; it does not wait, like
upstream's `wal_bytes_per_sync`), so the rotation sync only waits for the tail: 155 ms -> 10 - 66 ms. Durability
semantics are unchanged.

### 2. No large allocations per output file

- `LocalWritableFile.Append`: an append at least as large as the buffer is written through. The table builder stages
  256 KiB of blocks and used to grow the file buffer 64 KiB -> 1 MiB per file (renting each size) and copy the staged
  bytes once more; now its staged chunks go straight to `pwrite` (block -> staging buffer -> page cache, one copy like
  upstream's block -> `WritableFileWriter` buffer).
- `FilterEntryPool` (filter hash entries) and a dedicated `ArrayPool.Create` pool for the builder's staging buffer
  replace `ArrayPool.Shared` for these large arrays; a new filter builder starts with the largest retained array, so it
  normally never grows.

| compaction-heavy probe (4 MiB memtables, 500k random puts after 500k fill) | before | after |
|---|---:|---:|
| allocated per put, steady phase | 59.6 B | 5.5 B |
| gen0/1/2 GCs, steady phase | 2/2/2 | 0/0/0 |
| run-all compaction config, managed alloc fillrandom / overwrite | 50.5 / 18.1 B/op | 26.8 / 6.7 B/op (rest: memtable arena warm-up) |

### 3. First-run code quality: AggressiveOptimization on the per-entry methods

The per-entry methods of flush and compaction (`FlushJob.AddPointEntries`, `CompactionIterator` Next/NextFromInput/...,
`CompactionJob` per-key helpers, `MergingIterator` Next/heap, `TableInternalIterator` / `BlockBasedTableIterator` /
`BlockIter` Next and accessors, `BlockBasedTableBuilder` Add/Flush/WriteBlock, `BlockBuilder` Add/Finish) are
`[MethodImpl(MethodImplOptions.AggressiveOptimization)]`: compiled once, fully optimized, on first call. That gives up
Dynamic PGO for them, whose main benefit there was guarded devirtualization of the child iterator calls, so
`MergingIterator` and `LevelIterator` now call their children through sealed-type checks (`TableInternalIterator`,
`LevelIterator`; other types fall back to the virtual call), the managed equivalent of upstream caching the child key
in `IteratorWrapper`. In-process `CompactRange` of the 9-file DB, 3 alternated runs each:

| | first compaction in the process | steady state (median of runs 3-6) |
|---|---:|---:|
| without AggressiveOptimization | 920 / 1084 / 922 ms | 239 / 270 / 257 ms |
| with (+ sealed-type dispatch) | 673 / 633 / 680 ms | 265 / 256 / 270 ms |

First table build of a process (in-process probe, 400k entries): 455 - 500 ms -> 355 - 386 ms, steady state unchanged.
The skiplist iterator also prefetches the following node (flush walks a randomly ordered list).

### Before / after (alternated, medians)

| | before | after | c-native | after / C |
|---|---:|---:|---:|---:|
| fillrandom 400k + `flush`, WAL on (5 runs) | 1081 ms | 555 ms | 467 ms | 1.19x (was 2.31x) |
| fillrandom 400k + `flush`, WAL off (3 runs) | 601 ms | 533 ms | 378 ms | 1.41x (was 1.59x) |
| `compact`: CompactRange of 9 L0 files (58 MB), fresh process (5 runs) | 1259 ms | 898 ms | 546 ms | 1.64x (was 2.31x) |
| compaction-heavy config: compaction time per input record (3 runs) | 2.62 µs | 2.22 µs | 1.70 µs | 1.31x (was 1.54x) |
| compaction-heavy config: total compaction time (3 runs) | 6087 ms | 4908 ms | 3811 ms | 1.29x |

`run-all.sh` compaction config (`-vm-perf3-compaction.md`): managed / C geometric mean 0.62x (perf #2: 0.63x), with
fillrandom 4.67 vs 5.02 µs, overwrite 4.03 vs 5.84 µs, readrandom 0.70x, seekrandom 0.33x. Reduced matrix
(`-vm-perf3.md`): this run had about twice the background load of the perf #2 run (load 7-10), which shows in the
read benchmarks this pass did not touch: their ratios moved in both directions by up to 2x (readseq 100 B / 1 thread /
lz4 1.37x -> 0.64x, 1 KiB / 4 threads / lz4 1.46x -> 2.76x), so the summary rows of that file mostly measure noise; its
`flush` / `compact` rows (single first-in-process runs) moved from 1.11 - 5.8x / 0.7 - 2.2x to 0.7 - 3.2x / 0.96 - 2.35x.

Remaining gaps:
- First-run JIT: a single flush / compaction per process (the matrix's `flush` and `compact`) still costs 1.5 - 2x
  native; steady-state flush / compaction CPU per entry is at or below native. (Cold-start only: the benchmarks now
  warm up first, see "Benchmark warm-up".)
- LZ4 flushes: K4os LZ4's compressor is JIT compiled (82 ms of JIT for `LZ4_compress_generic` instantiations / OSR
  variants) and runs as tier-0 code during the first flush; fillrandom 300k + flush with LZ4 is 720 - 845 ms vs
  290 - 343 ms native. Addressed later by the managed LZ4 encoder (see "LZ4 encoder and parallel compression").
- Compaction picking under sustained writes: managed lets L5 grow to 9 - 10 files before L5 -> L6 compactions while
  native 11.12 keeps it at 4 - 6, so managed L0 -> L5 compactions read ~1.4x more L5 data (write-amp 3.05 - 3.33 vs
  2.96) and more work is left at the end of the writes (`waitforcompaction` 0.3 - 1.7 s vs 0.02 - 0.25 s). Fixed
  where it was a fidelity problem, see "Compaction-picking parity" below.
- Range-partitioned subcompactions (`max_subcompactions`) are not implemented; native defaults to 1, so they would not
  change the comparison. (Implemented since: "Compaction completeness" below.)

## Compaction-picking parity (compaction-heavy configuration)

Follow-up of the last gap of perf pass #3. Method: a lock-step harness
(`tests/RocksDb.Managed.CompatTests/Compaction/CompactionPickerParityTests.cs`) runs the same seeded random-write stream
on native (rocksdb-sharp) and managed with the compaction-heavy options scaled down 16x (256 KiB memtable = target
file size, 1 MiB level base, dynamic level bytes, no compression, `max_background_jobs = 1`). After every memtable
both engines flush and `WaitForCompact`, so the LSM shapes depend only on flush / picker / output-cutting logic, and
both LOGs ("Compacting N@L + M@L' files to Lx", "Moved #...", flush and output table sizes) give the compaction
sequence and the bytes written. Managed now writes the upstream lines (`OnCompactionStarted`, "Generated table",
"Moved #", "Level-0 flush table"). The test parses these plain-text lines rather than the EVENT_LOG JSON, which the native
library writes without its numbers in a .NET 11 apphost unless libstdc++ is preloaded (`tests/Shared/NativeStdCxxPreload.cs`).

No picker or scoring change separates the native 11.12 from the 11.14 sources (`git log` of `db/compaction/`,
`db/version_set.cc`, `db_impl_compaction_flush.cc` since 2026-09-25, HISTORY.md 11.13); the scores, base level and
level targets, file ordering (kMinOverlappingRatio), L0 -> Lbase vs intra-L0, trivial moves, grandparent cuts and the
compaction limit (2 jobs -> 1 flush + 1 compaction, speed-up only with write pressure) were already faithful. The
harness diverged at step 11 (first L0 -> L5) and found two real differences:

1. **The flush kept every version.** Upstream's flush runs a CompactionIterator, which drops versions hidden by a
   newer version in the same snapshot stripe (rule (A)). The managed L0 files had ~2% more entries for this workload,
   and duplicated user keys at block boundaries also made the index ~45% larger (1820 vs 1256 bytes), so every L0
   file was 2% larger and so was every compaction. `FlushJob` now drops them for Put / Delete / blob index / entity
   (merge operands, single deletes, TimedPut values and CFs with user-defined timestamps keep every version; single
   deletes are never dropped), with the snapshots taken when the flush starts. Flushed files now match native in
   entries, data, index and filter size.
2. **Max output file size.** Upstream lets outputs of non-bottommost compactions with grandparents grow to 2x
   `target_file_size` (`Compaction::max_output_file_size_`; the grandparent-boundary cuts normally end them at 50 - 90%
   of the target). The port capped them at 1x, so L0 -> L5 compactions wrote more, smaller L5 files.

| lock-step parity test (60 steps, 15.3 MB written by the user) | native | managed before | managed after |
|---|---:|---:|---:|
| first differing LSM shape / compaction | - | step 11 | none (all 60 steps and all compactions identical) |
| write amplification | 4.322 | 4.554 (+5.4%) | 4.321 |

Free-running compaction-heavy config (`bench/results/2026-11-02-vm-picker-compaction.md`, num=500k, three engines,
plus three more runs not committed): native 2.96 every run (`3 0 0 0 0 4 15`), rocksdb-sharp 2.96 - 3.05, managed
2.96 / 3.15 / 3.15 / 3.24 (perf #3: 3.24 - 3.33), and the peak L5 file count during the writes (from the LOG of one
run each) dropped from 9 - 12 to 7 (native 7). What remains is pace, not picking: with one compaction thread the LSM shape depends on how
far compactions fall behind the writer, and managed ingests 1.3 - 1.6x faster than native (overwrite 3.4 - 5.3 vs
5.4 - 7.4 µs/op) while its first compactions run cold (JIT, see perf pass #3). More L0 files then pile up during each
L0 -> L5 compaction (up to 10 vs 5 - 6), the L0 score keeps L5 -> L6 compactions waiting, and the final
`waitforcompaction` has more left (0.5 - 1.2 s vs 0 - 0.4 s). Given the same LSM state the managed picker chooses
the same compactions (same inputs in the LOG comparison of the two engines), and the lock-step test shows identical
results. The managed timings of these runs are cold (fresh process, no warm-up pass).

## Benchmark warm-up (measuring warm code)

Until this change every dbbench process measured from a cold start: the managed engines (and rocksdb-sharp's
P/Invoke stubs and wrapper) were JIT compiled during the first benchmarks, ran as tier-0 / OSR code until tiered
compilation caught up, and the background tier-1 compiler competed with the measurement. The runner now warms up
first, in all three legs, with the same workload (design and flags: "Warm-up" in `bench/README.md`): the whole
benchmark list on a throwaway `<db>-warmup` DB (same options, max(50k, num/10) <= 200k keys; repeated benchmarks for
>= 32 repetitions, >= warmup_ops ops and >= 1 s each), then 4 cycles of reopen + 16 small overwrite + flush,
waitforcompaction and compact (64 flushes, so methods that run once per flush / table file / compaction also get the
~30 calls per tiering step), delete the DB, full GC, wait until `JitInfo.GetCompiledMethodCount` is stable for
200 ms; every measured benchmark starts after a settle pause (JIT quiet for 200 ms; the C leg sleeps 200 ms). Each
.NET result line reports the methods compiled while it was timed (`jit N methods X ms`, CSV `jit_methods` / `jit_ms`).

Development notes. A warm-up that ran each benchmark only for its op count (50 - 100 ms for fast reads) or with
`--warmup_min_ms 100` still left 50 - 120 methods / up to 140 ms of JIT in the measured readrandom: the per-op read
path (block fetch, `BlockIter.Seek`, cache insert) is called millions of times but only promoted after the tiering
delay expires, which needs the warm-up to keep running; hence the time floor. The DB open path (options parsing, LOG
header) and the end of each benchmark (report formatting) promote methods when called again; the settle pause moves
that out of the measurement. With 3 cycles of 4 flushes, 35 - 90 methods were still compiled during the measured
`compact` (managed); 64 small flushes bring that to 4 - 21.

Results, alternated cold (`--warmup false`) / warm runs in the same window, after the compaction-picking merge
(`bench/results/2026-11-02-vm-cold.md` / `-vm-warm.md`, reduced matrix, load 1 - 7; `-vm-cold-compaction.md` /
`-vm-warm-compaction.md` = pair 3 of 3 compaction-heavy pairs, load 1 - 2). The first warm runs (15:43 - 16:10 UTC)
overlapped another agent's librocksdb source build and were redone; the committed files are the redone runs.

JIT methods compiled during the measured benchmarks (all threads):

| | rocksdb-sharp total / median / max | managed total / median / max | managed-async total / median / max |
|---|---:|---:|---:|
| matrix, cold (112 benchmarks per engine) | 3904 / 22 / 249 | 30176 / 161 / 1256 | 34690 / 180 / 1529 |
| matrix, warm | 16 / 0 / 1 | 408 / 0 / 51 | 617 / 1 / 75 |
| compaction-heavy, cold (5 benchmarks, 3 runs) | 40 - 250 per fill | fillrandom 1820 - 2060, overwrite 1110 - 1260, readrandom 600 - 1000 | fillrandom 2010 - 2520, readrandom 1460 - 1720 |
| compaction-heavy, warm (3 runs) | 0 - 1 | fillrandom 170 - 610, overwrite 110 - 630, reads 2 - 37 | fillrandom 40 - 600, overwrite 140 - 430, reads 3 - 106 |

What remains: the compaction-heavy fills reach LSM shapes the 50k-key warm-up DB does not (L5 -> L6 compactions
of a 110 MB tree), so a few hundred per-file / per-compaction methods still tier up during them (60 - 220 ms of
background JIT over a 1.6 - 3.5 s benchmark, on another core). `--warmup_ops 250000` did not reliably reduce it.
Everything else is 0 - a few methods.

Cold vs warm, reduced matrix (median over the 8 configurations of each benchmark; warm/cold = same engine, ratios vs
c-native of the same run):

| benchmark | engine | warm / cold | cold / C | warm / C | JIT methods cold -> warm (median) |
|---|---|---:|---:|---:|---|
| fillseq (first op of the process) | rocksdb-sharp | 0.92x | 1.00x | 1.01x | 207 -> 1 |
| | managed | 0.56x | 0.98x | 0.53x | 494 -> 1 |
| | managed-async | 0.48x | 1.06x | 0.60x | 567 -> 1 |
| flush | rocksdb-sharp | 0.94x | 0.99x | 0.85x | 4 -> 0 |
| | managed | 0.94x | 0.76x | 0.56x | 65 -> 2 |
| | managed-async | 0.54x | 1.71x | 0.50x | 44 -> 3 |
| compact | rocksdb-sharp | 0.90x | 1.08x | 0.94x | 6 -> 0 |
| | managed | 0.64x | 1.04x | 0.47x | 346 -> 8 |
| | managed-async | 0.60x | 0.81x | 0.69x | 386 -> 14 |
| readrandom | rocksdb-sharp | 0.92x | 1.15x | 1.20x | 40 -> 0 |
| | managed | 0.58x | 1.22x | 0.70x | 1026 -> 19 |
| | managed-async | 0.58x | 1.37x | 0.75x | 1264 -> 36 |
| seekrandom | managed | 0.92x | 0.68x | 0.66x | 236 -> 1 |
| multireadrandom | managed | 0.85x | 0.90x | 0.72x | 159 -> 0 |
| readseq | managed | 0.63x | 0.85x | 0.51x | 21 -> 0 |

c-native's own warm/cold medians are 0.87x (fillseq), 1.32x (flush), 1.08x (compact), 0.94x (readrandom): the C
leg gains nothing from the warm-up, and its single-shot `flush` / `compact` rows swing by up to 3x between runs
(e.g. 100 B / 1 thread flush 144 -> 381 ms, 1 KiB / 4 threads compact 678 -> 1544 ms), so read the flush / compact
ratios as +-50%. Per configuration (managed, ms; ratio vs c-native of the same run):

| configuration | flush cold | flush warm | compact cold | compact warm |
|---|---:|---:|---:|---:|
| 100 B, 1 thread, none | 111 (0.77x) | 107 (0.28x) | 648 (0.69x) | 650 (0.53x) |
| 100 B, 4 threads, none | 93 (0.64x) | 95 (0.28x) | 523 (0.64x) | 371 (0.41x) |
| 100 B, 1 thread, lz4 | 181 (1.43x) | 163 (0.76x) | 793 (0.97x) | 679 (0.93x) |
| 100 B, 4 threads, lz4 | 160 (0.93x) | 156 (0.97x) | 688 (1.25x) | 494 (0.84x) |
| 1 KiB, 1 thread, none | 233 (0.39x) | 190 (0.34x) | 2150 (1.49x) | 420 (0.32x) |
| 1 KiB, 4 threads, none | 291 (1.95x) | 161 (0.36x) | 1312 (1.94x) | 422 (0.27x) |
| 1 KiB, 1 thread, lz4 | 282 (0.76x) | 258 (1.04x) | 1140 (0.63x) | 657 (0.82x) |
| 1 KiB, 4 threads, lz4 | 243 (0.63x) | 337 (1.21x) | 1247 (1.11x) | 444 (0.36x) |

Summary (geometric mean vs c-native over the 14 benchmarks of each configuration, same window):

| configuration | rocksdb-sharp cold -> warm | managed cold -> warm | managed-async cold -> warm |
|---|---:|---:|---:|
| 100 B, 1 thread, none | 1.13x -> 0.98x | 0.71x -> 0.52x | 0.72x -> 0.66x |
| 100 B, 4 threads, none | 1.11x -> 1.05x | 0.99x -> 0.60x | 1.03x -> 0.63x |
| 100 B, 1 thread, lz4 | 1.26x -> 1.19x | 0.93x -> 0.63x | 1.12x -> 0.68x |
| 100 B, 4 threads, lz4 | 0.98x -> 1.10x | 0.85x -> 0.61x | 0.87x -> 0.59x |
| 1 KiB, 1 thread, none | 1.05x -> 0.90x | 0.78x -> 0.50x | 0.66x -> 0.48x |
| 1 KiB, 4 threads, none | 1.43x -> 0.99x | 1.23x -> 0.73x | 1.11x -> 0.72x |
| 1 KiB, 1 thread, lz4 | 1.10x -> 1.09x | 0.80x -> 0.67x | 0.85x -> 0.73x |
| 1 KiB, 4 threads, lz4 | 1.04x -> 0.89x | 0.94x -> 0.81x | 1.00x -> 0.72x |
| compaction-heavy (median of 3 pairs) | 1.08x -> 1.02x | 0.62x -> 0.57x | 0.65x -> 0.68x |

Compaction-heavy pairs (µs/op, 3 runs each; the first pair still had load 7 - 8):

| benchmark | engine | cold | warm | cold / C (median) | warm / C (median) |
|---|---|---|---|---:|---:|
| fillrandom | c-native | 5.00 / 5.87 / 6.40 | 5.27 / 5.46 / 5.25 | - | - |
| | managed | 4.73 / 5.74 / 4.01 | 3.23 / 3.22 / 3.26 | 0.95x | 0.61x |
| overwrite | c-native | 5.36 / 6.08 / 5.69 | 5.08 / 6.34 / 6.15 | - | - |
| | managed | 3.01 / 3.35 / 2.97 | 6.73 / 3.25 / 3.25 | 0.55x | 0.53x |
| readrandom | c-native | 11.8 / 13.4 / 12.0 | 11.9 / 13.4 / 11.9 | - | - |
| | managed | 7.56 / 9.72 / 7.85 | 9.24 / 7.98 / 7.46 | 0.66x | 0.63x |
| seekrandom | c-native | 47.0 / 56.8 / 49.2 | 44.0 / 51.6 / 45.8 | - | - |
| | managed | 23.9 / 21.6 / 15.2 | 34.2 / 24.9 / 22.8 | 0.38x | 0.50x |

LSM shape / write-amp of these pairs: native 2.96 every run; managed 2.96 / 2.96 / 3.15 cold and 2.96 / 3.05 / 3.05
warm, managed-async 2.87 / 2.87 / 3.05 cold and 2.96 every warm run.

Side effect of the settle pause: background flushes / compactions also progress during the 200 ms before each
benchmark (in every leg), so `waitforcompaction` right after the fills now finds little or nothing left (20 - 60 µs in
all engines vs 15 - 970 ms cold); it is excluded from the summaries anyway. With the warm-up on, the .NET pause can be
longer than C's when the JIT is still busy (bounded at 5 s; typically 200 - 300 ms).

Takeaways: on the fill and read paths the cold runs mostly measured JIT (fillseq and readrandom compile 500 - 1000
methods; warm, managed is 0.53x / 0.70x of C instead of ~1x / 1.22x). Flush and full compaction of a warm process are
at or below native cost except the LZ4 flushes (K4os LZ4 is steady-state slower; 1 KiB / lz4 flush 1.04 - 1.21x)
and reflect what a long-running application sees. rocksdb-sharp changes little (its JIT is the binding layer only).
Noise: shared 4-vCPU VM; even at load 1 - 7 single-shot rows vary by +-50% and benchmark rows by +-10-30%.

## Integrated BlobDB (blob files)

`bench/results/2026-11-02-vm-blob.md`: num=200k, 1 KiB values, `--enable_blob_files 1 --min_blob_size 512`, 16 MiB
write buffer, single thread, shared 4-vCPU VM under load (load average 6-10; noisy). Geometric mean of the µs/op ratio vs
c-native: rocksdb-sharp 0.84x, managed 0.55x, managed-async 0.53x. Per benchmark (managed / C): fillrandom 0.44x,
overwrite 0.50x, flush 0.41x, readrandom 0.69x, multireadrandom 0.66x, seekrandom 0.45x, readseq 0.77x, compact 0.57x.
The LSM shape after the fills is identical for all engines (`2 0 0 0 0 0 1`); write amplification 1.11 (managed) vs
1.08 (native; the managed flush keeps every version, see the validation notes).

Hot-path notes: a Get of a blob value reads the record into an `ArrayPool` buffer (the record header is verified with
verify_checksums, like upstream), decompresses or copies it into the lookup's scratch buffer and then into the value
buffer (one extra copy of the blob compared with native's pinned slice); steady state allocates nothing beyond the
existing Get path (31.5 B/op is the benchmark harness, as in the non-blob runs). Blob cache keys are derived once per
blob file. Iterators read a blob on the first `Value` access (sync I/O; `ScanAsync` / `PrepareValueAsync` use async
reads). Flush and compaction write blobs through the same buffered `IWritableFile` as table files.

Open follow-ups: blob prefetching for compaction (`blob_compaction_readahead_size` is recorded only), batched blob reads
for MultiGet (one read per blob today), avoiding the extra blob copy in Get (swap the scratch and value buffers).

## Compaction features (audit #3 remaining deviations): no leveled regression

The universal / FIFO picker ports, the complete ShouldStopBefore (TTL cuts, partitioner, upper-bound size, round-robin
split; each behind a predictable per-key branch that is off by default), the memtable first-insert time (one relaxed
load per insert once set) and the HyperClock eviction cap do not change the leveled path. Deterministic check: the
lock-step parity test still matches native on all 60 LSM shapes and every compaction (write-amp 4.321 vs native
4.322). Warm compaction-heavy dbbench (managed only, num 500k, 4 MiB memtable / target, 16 MiB level base, three
alternating runs of the previous build and this one on the shared 4-core VM, µs/op):

| | fillrandom | overwrite | readrandom | seekrandom | write-amp |
|---|---|---|---|---|---|
| before (3 runs) | 3.29 / 3.95 / 3.25 | 3.86 / 3.39 / 3.24 | 8.41 / 6.88 / 7.35 | 27.8 / 11.9 / 18.8 | 2.87 / 3.24 / 3.05 |
| after (3 runs) | 2.99 / 2.91 / 3.08 | 3.33 / 3.56 / 2.85 | 7.94 / 7.59 / 7.24 | 18.8 / 24.3 / 15.3 | 3.15 / 3.05 / 3.15 |

The spread between runs of the same build is larger than any difference (the free-running write-amp depends on how far
compactions fall behind the writer, see "Compaction-picking parity").

## Final three-way results: 5 repetitions on a quiet machine

`bench/results/2026-11-02-final-warm-r5.md` (19:50-21:27 UTC): the reduced matrix and the compaction-heavy
configuration, all four engines, `REPEATS=5` (new in `run-all.sh` / `report`: engines interleaved per repetition,
fresh DB and warm-up per run, medians, min-max, CV, 95% bootstrap CIs), with no other agents on the VM (each repetition
started at a 1-minute load average <= 1.5; 0.51-3.36 before the individual runs, median 1.65). Sizes: 300k x 100 B,
100k x 1 KiB (as many reads), 500k for the compaction-heavy configuration. Same tree as "Compaction features" above.

Geometric mean of the µs/op ratio vs c-native, against the previous single warm run on the loaded VM
(`2026-11-02-vm-warm.md`: 1 KiB with 60k entries and 200k reads; `-vm-warm-compaction.md`: median of 3 runs):

| configuration | rocksdb-sharp single run | rocksdb-sharp median of 5 [95% CI] | managed single run | managed median of 5 [95% CI] | managed-async single run | managed-async median of 5 [95% CI] |
|---|---:|---:|---:|---:|---:|---:|
| 100 B, 1 thread, none | 0.98x | 1.06x [1.01-1.13] | 0.52x | 0.59x [0.56-0.63] | 0.66x | 0.64x [0.61-0.68] |
| 100 B, 4 threads, none | 1.05x | 1.01x [0.96-1.09] | 0.60x | 0.78x [0.72-0.82] | 0.63x | 0.79x [0.74-0.83] |
| 100 B, 1 thread, LZ4 | 1.19x | 1.11x [1.07-1.16] | 0.63x | 0.69x [0.66-0.73] | 0.68x | 0.76x [0.72-0.79] |
| 100 B, 4 threads, LZ4 | 1.10x | 1.01x [0.97-1.09] | 0.61x | 0.83x [0.77-0.87] | 0.59x | 0.78x [0.73-0.81] |
| 1 KiB, 1 thread, none | 0.90x | 1.04x [0.96-1.19] | 0.50x | 0.52x [0.49-0.57] | 0.48x | 0.55x [0.53-0.61] |
| 1 KiB, 4 threads, none | 0.99x | 1.03x [0.96-1.08] | 0.73x | 0.63x [0.58-0.66] | 0.72x | 0.60x [0.56-0.63] |
| 1 KiB, 1 thread, LZ4 | 1.09x | 0.99x [0.94-1.07] | 0.67x | 0.58x [0.55-0.62] | 0.73x | 0.64x [0.60-0.68] |
| 1 KiB, 4 threads, LZ4 | 0.89x | 1.02x [0.94-1.11] | 0.81x | 0.70x [0.65-0.75] | 0.72x | 0.74x [0.68-0.79] |
| all 8 matrix configurations | | 1.03x [1.01-1.06] | | 0.66x [0.64-0.67] | | 0.68x [0.67-0.70] |
| compaction-heavy | 1.02x | 1.07x [1.00-1.13] | 0.57x | 0.58x [0.51-0.63] | 0.68x | 0.69x [0.64-0.72] |

What the repetitions show that single runs did not:

- The run-to-run noise floor of the VM is ~10% per cell even when it is quiet (median CV 9.9% c-native, 10.0-10.7% the
  .NET engines; 20% at the 90th percentile). Single-run cells of earlier entries in this log are within that noise.
- The 100 B / 4-thread configurations are worse than the single warm run suggested (0.78-0.83x instead of 0.60x).
  The causes are two real slow spots, consistent over all 5 repetitions: sync `fillrandom` / `overwrite` with 4
  writer threads (1.15-1.31x / 1.48-1.81x vs C; async 0.90-1.03x / 0.94-0.95x, so it is the sync write path under
  contention, not the memtable), and 4-thread `readmissing` with 100 B values (managed 1.42-1.68x, managed-async
  1.93-3.00x; 0.33-0.59x / 0.51-0.73x in the other configurations).
- rocksdb-sharp's binding cost is 3% overall (CI 1-6%), concentrated in scans (`readseq` 1.17x, `readreverse` 1.14x).
- Compaction-heavy write-amp: c-native, rocksdb-sharp and managed-async 2.96 in every repetition, managed 2.96-3.15
  (median 3.05).
- 1 managed process of 45 crashed in its warm-up: `RocksDatabase.Open` on the directory the warm-up had just deleted
  and re-created failed with `MANIFEST-000001` not found (`DbBenchRunner.RunWarmup`, open with destroyExisting before
  a fill benchmark; 1 KiB / LZ4 / 1 thread, repetition 1). The 14 rows were re-measured. **Fixed since:** a background
  flush counted itself finished before its obsolete-file purge, so close could return while the purge of the disposed
  instance was still deleting files (an old MANIFEST number = the new DB's `MANIFEST-000001`). The purge now runs before
  the job is accounted, like compaction (`DbLayerAuditTests.CloseWaitsForTheFlushObsoleteFilePurge`).

BenchmarkDotNet head-to-head, default job, same window (`bench/results/2026-11-02-final-bdn.md`): managed /
rocksdb-sharp is 0.57x for Put, 0.60x for a 100-put WriteBatch, 0.49x / 0.35x / 0.28x for Get from the memtable /
a cached block / a missing key, 0.26x / 0.21x for Seek + 10 / 100 Next and 0.52x for MultiGet(16); nothing allocates.
`DbAsync`: async costs 0-20% over sync when calls complete synchronously (TryGet 1.31 vs 1.60 µs, MultiGet(16)
19.1 vs 19.1 µs).

## Transactions: per-transaction allocations

BenchmarkDotNet `TransactionBenchmarks` (`--anyCategories Txn`, short job, net10.0, shared 4-vCPU container, one run
before and one after; times are noisy, compare allocations). Begin reusing the previous transaction object, 4 puts
(16 B keys, 100 B values), Commit; the baseline is the same 4-put WriteBatch written directly, whose 557 B are the
memtable arena growth for the data.

| Kind | Allocated before | Allocated after | Mean before | Mean after | plain batch (after) |
|---|---:|---:|---:|---:|---:|
| Optimistic | 717 B | 557 B | 32.7 µs | 31.8 µs | 24.0 µs |
| Pessimistic (WriteCommitted) | 1325 B | 557 B | 25.7 µs | 25.5 µs | 23.6 µs |
| WritePrepared | 1325 B | 557 B | 22.8 µs | 25.0 µs | 21.5 µs |
| WritePrepared, two_write_queues | 1325 B | 557 B | 26.5 µs | 25.8 µs | 24.6 µs |
| plain WriteBatch into a WritePrepared DB | 1397 B | 557 B | 22.1 µs | 21.5 µs | |
| plain WriteBatch into a WritePrepared DB, two_write_queues | 1397 B | 557 B | 23.2 µs | 24.6 µs | |

After: the async variant (PutAsync / CommitAsync) and a named 2PC cycle (SetName, Prepare, Commit) also allocate only
the memtable data, for every pessimistic policy. What changed: lock-table entries (LockInfo + key buffer) are recycled
per stripe; tracked keys live in recycled buffers (`PooledKey`, span lookups) instead of one `byte[]` per key; a lambda
in `AssignCommitTimestamp` allocated its closure on every commit (C# creates the display class on entry to the scope
that declares the captured variables, before the early return); the commit / prepare async methods use
`PoolingAsyncValueTaskMethodBuilder`; a reused transaction keeps the UTF-8 encoding of its name; plain writes into a
WritePrepared DB reuse their WriteInternal state and count sub-batches of bytewise column families in a reusable
byte-equality set.

## Read/write contention with 4 threads (readmissing, sync random writes)

Follow-up of the two slow spots of the final three-way run: 4-thread `readmissing` with 100 B values (managed 1.4-1.7x,
managed-async 1.9-3.0x vs C) and 4-thread sync `fillrandom` / `overwrite` (1.04-1.81x, async 0.75-1.03x).

**Method.** The machine was shared with other agents (load average 8-15 on 4 vCPUs) for the whole session, so wall time
alone was useless (c-native moved by 2x between runs). The A/B runs below use `nice -n -15` for every engine process
(the benchmark gets nearly all CPU when runnable), a short warm-up (`--warmup true --warmup_ops 50000
--warmup_min_ms 300`), the before / after binaries (`base` = the commit before, `new` = after) and c-native interleaved
in every repetition, 5 repetitions, medians. The dbbench runner now also prints the process CPU time per operation
(`cpu ... us/op`, all threads): it is much less sensitive to the other load and shows spinning and cache-line
contention directly. `perf record` with `DOTNET_PerfMapEnabled=1 DOTNET_EnableWriteXorExecute=0` (W^X double mapping
hides JIT code from perf otherwise) was used for the profiles.

### 1. readmissing: shared reference counts on every Get

Profile of 4-thread `readmissing` (data in L0 files after a flush): `TableCache.FindTable` 21% and the inlined super
version reference in `ColumnFamily.Lookup` 15%, while the filter probe itself was a few percent. Every Get wrote four
shared cache lines: the super version's reference count (CAS + decrement), the table handle's reference count (CAS +
decrement) and a global `Interlocked.Increment` for the table cache's LRU order, so the lines bounced between the 4
cores on every lookup. Fixes, both ports of what upstream does:

- **Cached super version references** (`Db/SuperVersionCache.cs`, upstream `ColumnFamilyData::local_sv_`,
  `GetThreadLocalSuperVersion` / `ReturnThreadLocalSuperVersion` / `ResetThreadLocalSuperVersions`). Point reads
  (Get, GetAsync, MultiGet) swap `InUse` into a slot, use the super version the slot holds a reference on, and swap it
  back. Installing a super version resets every slot under the DB mutex before the old one's CF reference is released.
  Slots are per processor (128 bytes apart) rather than per thread (.NET has no thread-exit hook to release a thread's
  reference); a reader that finds `InUse` (another thread on the same processor holds the slot) takes a plain
  reference, and a cached super version that is no longer current is released instead of used.
- **Pinned table readers** (`TableCache.GetReaderForVersion`, upstream `fd.table_reader` with `max_open_files = -1`).
  With an unbounded table cache the first lookup through a referenced version pins the reader (one extra reference,
  dropped when the file is evicted as obsolete), so later lookups through a super version take no reference on it. The
  LRU counter is no longer updated by unbounded caches.
- Async Get: the range tombstones came from a second table-cache lookup per file; a filter miss now completes without
  the async state machines of `GetFromFileAsync` and `BlockBasedTableReader.GetAsync`.

| 4 threads, 100 B, after fillrandom + flush (µs/op, median of 10) | base wall | new wall | base cpu | new cpu | c-native wall |
|---|---:|---:|---:|---:|---:|
| readmissing (sync) | 0.410 | 0.314 | 0.724 | 0.398 | 0.427 |
| readmissing (async) | 0.616 | 0.329 | 0.927 | 0.599 | |

1 thread (median of 5): sync 0.285 / 0.290 µs (unchanged), async 0.507 / 0.405 µs. readrandom, 4 threads: cpu 3.83 ->
3.34 µs/op sync, wall within noise. Allocations unchanged (readmissing 0.1 B/op, readrandom 5.9 B/op, multiread 0.2).
After the change the remaining 4-thread profile is the filter probe, the index/L0 file checks and the benchmark's own
key generation; no shared write is left on the path.

### 2. Sync random writes: follower wake-ups

Profile of 4-thread sync `fillrandom`: 41% skip-list insert by the leader, the rest spread over WAL write, wake-ups and
scheduler. The async path performs the same serial inserts, but its followers resume as thread-pool work items (the
pool's workers spin before they sleep), while a sync follower waited on a `ManualResetEventSlim` and needed a futex
wake-up (and a reschedule) for every hand-off: completing a follower and making the next writer leader.

- **AwaitState port** (`WriteThread.Writer.WaitSync`): busy spin (200 pause iterations), then, with
  `enable_write_thread_adaptive_yield` (default), `Thread.Yield` for up to `write_thread_max_yield_usec` (100 µs) as
  long as the yield credit says yielding ends the wait without involuntary context switches (sampled 1 in 256, three
  slow yields fall back to blocking at once), then a blocking wait. The block is upstream's `STATE_LOCKED_WAITING`
  protocol on a monitor: the waker CASes the state and only takes the monitor if the waiter registered as blocked, so a
  waiter that saw the state while spinning is never touched again (a `ManualResetEventSlim.Set` landing after the
  writer re-armed would have woken its next wait spuriously).
- **Parallel memtable inserts** (`allow_concurrent_memtable_write`, upstream `LaunchParallelMemTableWriters` /
  `CompleteParallelMemTableWriter` / `ExitAsBatchGroupFollower`): after the WAL write the leader wakes the followers,
  every writer inserts its own batch with `MemTable.AddConcurrently` (counters applied by `BatchPostProcess`, flushes
  scheduled from there), and the last writer to finish publishes the sequence and completes the others. Measured with
  single-key writes it was clearly slower (each follower needs two hand-offs instead of one, and concurrent skip-list
  inserts are CAS-based): 4 threads, 5 repetitions, fillrandom 8.48 vs 5.97 µs/op, overwrite 10.34 vs 7.53 (cpu 10.1 vs
  6.0 and 13.9 vs 8.7). With 16-key batches it was neutral (fillrandom 3.92 vs 4.03, overwrite 4.27 vs 3.95 µs/key).
  So groups go parallel only from 64 entries (`DbImpl.MinParallelMemtableGroupEntries`; upstream: any group of two or
  more writers). Groups with merge operands, transaction writers or blob direct write rewrites stay serial like
  upstream (merges) or for simplicity. `TestHooks.ForceParallelMemtableWrites` makes tests exercise the parallel path
  with small groups.
  *(Superseded: the 64-entry minimum and the test hook were removed later, see "Pipelined writes and parallel memtable
  inserts".)*

| 4 threads, 100 B (µs/op, median of 5 / 10) | base wall | new wall | base cpu | new cpu | c-native wall |
|---|---:|---:|---:|---:|---:|
| fillrandom (sync) | 6.31 | 5.22 | 6.38 | 6.47 | 8.94 |
| overwrite (sync) | 6.80 | 6.47 | 8.64 | 6.91 | 11.32 |
| fillrandom (async) | 5.79 | 6.27 | 7.40 | 6.06 | |
| overwrite (async) | 6.85 | 6.42 | 8.68 | 7.91 | |

An earlier run of the same comparison (no c-native, 5 repetitions): sync fillrandom 6.98 -> 5.97, overwrite 7.41 ->
7.53 µs/op. c-native is slower than on a quiet machine in these runs (its WAL and flush I/O compete with the other
agents' I/O, which `nice` does not cover), so the ratios vs C are only indicative; the 4-thread rows need a quiet
`bench/run-all.sh` run to confirm the ratios. Allocations unchanged (fillrandom 66.9 B/op = memtable arena growth).

### 3. Not done

- **Reverse iteration pins blocks.** `DbIter.FindValueForCurrentKey` still copies the value of the newest visible entry
  (upstream pins the data block with `PinnedIteratorsManager`). Pinning needs a pin handle through the merging, level
  and table iterators down to the block (a `BlockRef` reference, or ownership of an uncached block buffer); the copy is
  one memcpy of the value per user key, and managed `readreverse` is already 0.41-0.68x of C (geometric mean 0.59x).
- **Async reverse movement.** `PrevAsync` / `SeekForPrevAsync` / `SeekToLastAsync` stay synchronous (a block read on a
  cache miss blocks the caller). Making `PrevInternal` / `FindValueForCurrentKey` resumable needs the step-function
  treatment of the forward path through the merging and level iterators' reverse movement.
- **Range tombstones in the merging-iterator heap** (upstream 7.x `MergingIterator`). It only pays off when seeking or
  scanning over long deleted ranges (whole covered ranges are skipped with one reseek instead of key by key); the
  matrix has no such workload, and the current design (every visible entry checked against the
  `RangeDelAggregator` in `DbIter`) is correct for all workloads. Not ported.
- **Pipelined writes** (`enable_pipelined_write`): done later, see "Pipelined writes and parallel memtable inserts".

## Compaction completeness: L0 pacing and subcompactions

**L0 pile-up under the free-running compaction-heavy config** ("Compaction-picking parity" above reported up to 10 L0
files for managed vs 5 - 6 for native). Re-measured with the warm-up (num = 500k, 4 MiB memtables / target files,
16 MiB level base, 2 background jobs = 1 flush + 1 compaction thread, fillrandom + overwrite), peak L0 count from both
LOGs (native: the "Level summary" / "compacted to" lines; managed: flushes since the last `compaction_finished`
lsm_state), loaded VM (load average 8 - 12 from other agents):

| run | 1 | 2 | 3 | 4 | 5 |
|---|---:|---:|---:|---:|---:|
| c-native | 7 | 7 | 6 | 7 | 8 |
| managed | 8 | 7 | 9 | 7 | 7 |
| managed, cold (`--warmup false`) | 10 | | | | |

The earlier native figure was the number of L0 files each compaction picked (`Compacting N@0`), which is 4 - 5 on both
engines; the actual L0 count peaks at 6 - 8 on native too. Neither engine reaches a write-stall trigger (L0 slowdown 20,
soft pending bytes 64 GiB; native only stops briefly on max_write_buffer_number while its slower flush runs), so the
write controller does not pace these runs, and the scheduling latency is the same (an L0 -> Lbase compaction starts in
the millisecond its 4th file is flushed, in both LOGs). The pile-up follows from how many flushes complete during an
L0 -> Lbase compaction: managed writes 1.2 - 1.9x faster here (overwrite 4.1 - 7.2 vs 6.3 - 11.1 µs/op) with
compactions of similar speed, and cold JIT slows its first compactions. The write-controller code was still compared
with `column_family.cc` and three differences fixed (delay recovery reward, rate carried between delays,
compaction-speedup thresholds relative to the bottommost data; see `docs/validation.md`); none fires in this config.

**Subcompactions** (`max_subcompactions`, new): `compact` of 1M x 100 B random keys written with auto compactions
disabled (16 MiB memtables, 4 MiB target files, 8 background jobs), one run each on the loaded VM, the managed
`compact` includes some JIT (149 - 299 methods):

| | max_subcompactions 1 | 4 |
|---|---:|---:|
| c-native | 1.62 s | 0.96 s |
| managed | 1.57 s | 0.51 s |

The per-key loop of a subcompaction is the unchanged compaction loop (allocation-free); planning reads up to 128 index
anchors per input file once per compaction.
## Auto-resizing HyperClock table and shared table cache (cache micro-benchmarks)

`CacheBenchmarks` (BenchmarkDotNet default job, net10.0) before and after the change, single runs each on the shared
4-vCPU VM with other agents building (load average 4-10, so differences below ~30% are noise; LRU, which did not
change, moved by +10% to +57% between the two runs). The fixed table code path only gained a read of the table
reference and a branch on a readonly flag. Every case stays allocation-free.

| Benchmark | LRU before | LRU after | HyperClock before | HyperClock after | AutoHyperClock (new) |
|---|---:|---:|---:|---:|---:|
| LookupHit | 609 ns | 668 ns | 292 ns | 263 ns | 260 ns |
| LookupMiss | 94 ns | 139 ns | 131 ns | 164 ns | 107 ns |
| InsertWithEviction | 159 ns | 174 ns | 230 ns | 267 ns | 260 ns |
| LookupHitMultiThreaded (per op) | 307 ns | 484 ns | 72 ns | 125 ns | 100 ns |

Allocated: `-` in every cell. The auto table (estimated_entry_charge = 0) performs like the fixed one once grown;
growth itself (64 slots per shard, doubling) only happens while the cache fills.

## Reverse iteration, range-deleted runs, compaction input readahead

Four items of the "Read/write contention with 4 threads" follow-up list. Method as in that section (shared 4-vCPU VM,
load 5-17): dbbench A/B with the before / after binaries interleaved per repetition (`nice -n -5`, warm-up
`--warmup_ops 50000 --warmup_min_ms 300`, medians), BenchmarkDotNet `--job short` with `[MemoryDiagnoser]`.

### 1. Reverse steps pin data blocks (PinnedIteratorsManager)

DBIter::FindValueForCurrentKey walks a key's versions from old to new, so the value it returns lies behind the inner
iterator's position; the port copied every value into a buffer. Upstream pins the blocks instead
(`db/pinned_iterators_manager.h`, `TempPinData` / `ReleaseTempPinnedData`, `IsValuePinned`). Ported: while DbIter pins,
`BlockBasedTableIterator` hands the block it leaves to the `PinnedIteratorsManager` instead of releasing it (one hand-off
per data block crossed, not per key); `InternalIterator.TryGetPinnedValue` (IsValuePinned + value) returns the value as a
`PinnedSlice` (block buffer range, or memtable arena memory, whose address is fixed) through the merging / level /
table / memtable iterators; children that cannot pin fall back to the copy. Pins are released at the next positioning
call; `ReadOptions.PinData` (pin_thru_lifetime) keeps them until the iterator is disposed. The forward path only gained
one predictable branch in `ReleaseBlock` (on block changes) and in `Next`.

### 2. Async reverse movement

`DbIter.PrevAsync` / `SeekForPrevAsync` / `SeekToLastAsync` were synchronous. DBIter's reverse logic
(ReverseToBackward, PrevInternal, FindValueForCurrentKey(+UsingSeek), FindUserKeyBeforeSavedKey) is now one resumable
state machine (`ReverseStep` returns the next move of the inner iterator); the sync driver performs the moves directly,
the async driver awaits them and completes synchronously when they do (the same pattern as the forward step functions),
so both paths share all of the logic. The table, level and merging iterators got async SeekToLast / SeekForPrev / Prev
(blocks and table opens awaited, cache hits synchronous) and the merging iterator's direction switches are async.

| 1 thread, 200k x 1 KiB after compact, block cache 1 GiB (µs/op, median of 3) | before | after | after, async API |
|---|---:|---:|---:|
| readreverse | 0.70 | 0.45 | 0.48 (before: 0.75) |
| seekrandom --reverse_iterator (SeekForPrev + 10 Prev) | 10.2 | 6.0 | 6.5 (before: 10.4) |
| readseq (forward, unchanged path) | 1.25 | 1.21 | |

The state machine itself costs nothing measurable (pinning-only build vs. pinning + state machine: readreverse
0.474 / 0.453, seekrandom reverse 6.45 / 5.82). BenchmarkDotNet (`IteratorBenchmarks`, 1 memtable + 4 L0 files, per
key; per SeekForPrev + 100 Prev):

| ValueSize | ManagedReverseScan | ManagedAsyncReverseScan | NativeReverseScan | SeekForPrevPrev100 | ...Async | Native |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 194 ns | 185 ns | 515 ns | 26.3 µs | 28.5 µs | 66.0 µs |
| 1024 | 361 ns | 375 ns | 903 ns | 57.4 µs | 48.6 µs | 108.4 µs |

Allocated: `-` everywhere except 72 B per call of the `async Task<long>` SeekForPrev benchmark method (its own
`Task<long>`; the per-key async scan shows `-`). New dbbench / native-c flag: `--reverse_iterator` (seekrandom with
SeekForPrev + Prev, upstream db_bench's flag).

### 3. Range-deleted runs: covered-range skipping

The merging iterator only merged point keys and DbIter asked a `RangeDelAggregator` about every visible entry
(pre-7.x scheme), so a scan over a deleted range visited every deleted key. Upstream feeds range tombstones into the
merging iterator's heap and reseeks past covered ranges (`table/merging_iterator.cc`, SkipNextDeleted /
SkipPrevDeleted, with LevelIterator sentinel keys at file boundaries). Benchmark first (`RangeDeletionBenchmarks`,
200k keys compacted to the last level, 100 range tombstones deleting 99% of every 2000-key chunk flushed to L0, same DB
shape in rocksdb-sharp): managed full scan 13.8 ms vs native 0.69 ms, reverse 24.4 vs 0.83 ms, Seek + 10 Next 59 vs
4.8 µs, so a port was worth it.

Implemented as the same reseek, driven from the aggregator rather than from heap entries: when DbIter drops a visible
entry as covered, `RangeDelAggregator.ShouldDeleteWithSkipTarget` also returns the covering tombstone's seqno and reach
(the fragment end / start, clamped so no user key at a truncated file boundary is crossed), and
`InternalIterator.SkipCoveredForward` / `SkipCoveredBackward` move only the child that produced the entry, in one seek,
and only where its file(s) hold nothing newer than the tombstone (`FileMetaData` smallest / largest seqno: an L0 table,
or every file of a level up to the target). Everything skipped is therefore an entry the aggregator reports as covered,
so the results are exactly those of the per-entry check (unlike upstream's level-order rule, this does not rely on the
LSM ordering invariant). Disabled with user-defined timestamps, read callbacks and legacy prefix seek; memtables never
skip.

| `RangeDeletionBenchmarks` | before | after | native |
|---|---:|---:|---:|
| full scan (2000 live of 200k) | 13,753 µs | 159 µs | 658 µs |
| reverse scan | 24,379 µs | 297 µs | 825 µs |
| Seek + 10 Next (per seek) | 59.4 µs | 2.15 µs | 4.78 µs |

Allocated `-`. Correctness: the randomized model tests now give their files seqno ranges (so skipping is active), the
native compat iterator test has a wide-range-delete variant (tombstones over up to 150 keys, 5 phases over L0/L1/L3/L5,
every snapshot, both directions, bounds), and `DeleteRangeSkipsCoveredKeysOfOlderFiles` proves the skip (with
max_skippable_internal_keys = 50 over 2800 covered keys, which fails without it).

### 4. Compaction input readahead with async prefetch

Compaction read its inputs with one pread per data block. Upstream reads them through a FilePrefetchBuffer of
compaction_readahead_size (2 MiB), with async_io prefetching the next chunk. Ported as `Env/AsyncPrefetchBuffer`
(two buffers; a miss reads a chunk synchronously and starts the next with `ReadAsync`, so I/O of the next chunk overlaps
the merge / output work; a block straddling two chunks is stitched through a reserved gap in front of the prefetch, no
re-read). Compaction input table iterators (`CompactionJob.NoFillTableCache`) read uncached data blocks through it with
`DbOptions.CompactionReadaheadSize` (0 disables it). New dbbench / native-c flags: `--compaction_readahead_size`,
`--drop_page_cache` (fsync + POSIX_FADV_DONTNEED of the SSTs before `compact`).

| `fillrandom,compact`, 300k x 1 KiB, inputs evicted from the page cache (same binary, A/B by flag, 4 reps) | wall | CPU |
|---|---:|---:|
| compaction_readahead_size 0 (one pread per block, as before) | 3.05 s | 1.59 s |
| compaction_readahead_size 2 MiB | 1.05 s | 0.69 s |
| c-native (2 MiB, 2 reps) | 2.27-2.50 s | |

With the inputs in the page cache (the default dbbench run) the difference is within the noise of this machine
(median CPU 0.32 vs 0.33 s, wall 0.75 s both): there the reads complete synchronously (RWF_NOWAIT) and are cheap.

## LZ4 encoder and parallel compression

**LZ4 encoder.** K4os' `LZ4Codec.Encode` (1.3.8) allocates and zeroes a 16 KiB `LZ4_stream_t` in unmanaged memory per
call and its generic engine is 2.2 - 3x slower than native liblz4 on data blocks. Options measured (BenchmarkDotNet,
db_bench-like blocks: 16-byte keys + 100-byte values with compression ratio 0.5, warm, 64 rotating blocks):

| Block | K4os Encode | K4os + cached state (`LL64.LZ4_compress_fast_extState` via reflection) | managed `Lz4BlockEncoder` | native `LZ4_compress_fast` (librocksdb) |
|---|---|---|---|---|
| 1 KiB | 2.87 us | 1.97 us (0.69x) | 1.35 us (0.47x) | 0.97 us (0.34x) |
| 4 KiB | 7.99 us | 7.20 us (0.90x) | 4.71 us (0.59x) | 3.43 us (0.43x) |
| 16 KiB | 28.2 us | 29.1 us (1.03x) | 19.5 us (0.69x) | 14.4 us (0.51x) |

Reusing K4os' state only removes the per-call allocation (worth it for 1 KiB only) and needs reflection into its
internals, so it was not kept. `Compression/Lz4BlockEncoder.cs` ports liblz4's LZ4_compress_generic for the byU16
table (inputs below 64 KiB + 11, acceleration 1, limited output, per-thread hash table cleared per call like
LZ4_initStream): its output is byte-identical to native liblz4 (compat test `Lz4EncoderCompatTests`, random inputs of
every size class and limited capacities), it is 1.4 - 1.7x faster than K4os and within 1.35 - 1.4x of native. LZ4HC,
inputs of 64 KiB and more, big-endian hosts and decompression stay on K4os.

**Cold start.** A fresh process compressing 64 then 12 800 4 KiB blocks (tiered compilation, the first flushes of a
process): K4os 14 - 16 ms + 400 - 430 ms; managed encoder (tier 0 / OSR) 5.3 - 5.9 ms + 213 - 248 ms; managed encoder
with `[MethodImpl(AggressiveOptimization)]` on the loop (kept) 3.2 ms + 83 ms (~4.7x less than K4os; steady state is
60 ms with `DOTNET_TieredCompilation=0`). Guidance for applications that flush right after start: ReadyToRun
(`<PublishReadyToRun>`) precompiles the port and K4os (decompression); `DOTNET_TieredPGO=0` alone does not help
(K4os 14 ms + 333 ms), `DOTNET_TieredCompilation=0` removes the warm-up at the cost of slower startup elsewhere.

**Table build A/B** (`TableBuildBenchmarks`: 200k entries, 4 KiB blocks, fv6, bloom; one SST in memory; ShortRun,
one process, LZ4 rows measured with the machine quiet, the ZSTD rows while other agents' builds started (load 15),
so treat them as indicative):

| | parallel_threads 1 | 2 | 4 |
|---|---|---|---|
| LZ4, K4os encoder | 67.5 ms | 68.0 ms | 69.0 ms |
| LZ4, managed encoder | 52.8 ms (0.78x) | 39.8 ms (0.59x) | 65.2 ms (noisy, median 59 ms) |
| ZSTD | 191 ms | 196 ms | 151 ms (median 130 ms, 0.68x) |

Earlier interleaved run of parallel compression alone (K4os encoder, load ~9 on 4 vCPU): LZ4 130 / 128 / 115 ms,
ZSTD 295 / 279 / 234 ms for 1 / 2 / 4 threads. Parallel compression helps ZSTD most (compression dominates the emit
work); with the faster LZ4 encoder the emit thread (block building, index/filter, checksums) is the bottleneck, so 2
threads already take most of the gain. With fewer free cores than threads the workers only add scheduling cost
(the emit thread then compresses the blocks itself when it needs a slot, so throughput never drops below serial by
much). Output is byte-identical in every configuration.

## Pipelined writes and parallel memtable inserts

**Pipelined writes** (`enable_pipelined_write`, upstream `PipelinedWriteImpl`; design in docs/architecture.md "Write
path"): the WAL leader links its group to a memtable writer queue and hands WAL leadership on before the inserts, so the
next group's WAL write overlaps them. `dbbench --enable_pipelined_write` sets it on all engines (managed, rocksdb-sharp,
c-native).

**Parallel memtable inserts for small groups.** The earlier measurement ("Read/write contention", fillrandom 8.48 vs
5.97 µs/op with single-key writes, 4 threads) led to a 64-entry minimum for parallel inserts. Re-measured with the
minimum configurable (`all` = any group of 2+ writers, as upstream; `pw4` / `pw8` = at least 4 / 8 entries per writer;
`g64` = the old rule), 4 threads, 100 B values, 200k keys, warm-up 50k ops, medians of 4 interleaved repetitions, load
average 1.5 - 4:

| 4 threads, µs/op (wall) | g64 | all | pw4 | pw8 | g64 async | all async | pw4 async | pw8 async |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom, 1 key / write | 4.33 | **3.73** | 4.25 | 4.21 | 3.94 | **3.88** | 4.18 | 4.10 |
| overwrite, 1 key / write | 5.55 | **4.66** | 5.61 | 5.87 | 5.55 | **4.52** | 5.48 | 5.84 |
| fillrandom, 4-key batches (per key) | 2.63 | 2.00 | **1.94** | 2.86 | 3.01 | **2.04** | 2.15 | 2.98 |
| overwrite, 4-key batches | 4.01 | 2.74 | **2.43** | 4.28 | 4.80 | **2.79** | 2.82 | 4.80 |
| fillrandom, 16-key batches | 2.59 | **1.41** | 1.51 | 1.54 | 2.37 | **1.64** | 1.84 | 1.72 |
| overwrite, 16-key batches | 3.00 | 2.26 | 2.04 | **1.96** | 3.60 | 2.76 | **2.62** | 2.64 |

Process CPU per op followed the same order (single-key fillrandom 13.6 `all` vs 16.0 `g64` µs/op, all threads). Going
parallel for every group is never worse on a lightly loaded machine, so the minimum is removed (as upstream;
`TestHooks.ForceParallelMemtableWrites` is gone too, tests exercise the parallel path naturally). The old result came
from a heavily oversubscribed VM (load 8 - 15 on 4 vCPUs): parallel inserts need every writer of the group to be
scheduled, the serial path only the leader. The same effect showed in this session's loaded runs (load 12 - 16:
single-key sync fillrandom 6.98 `all` vs 5.00 `g64`, cpu 11.1 vs 8.4, async equal) and is inherent to upstream's design
too; the hand-offs themselves are not the problem (followers spin through both waits when the CPU is available).

**Final A/B** (`base` = the commit before this work: 64-entry minimum, no pipelining; `final` = no minimum;
`pipelined` = final with `--enable_pipelined_write true`; c-native with and without pipelining), 200k keys, 100 B,
medians of 5 interleaved repetitions, load average 1.8 - 2.6:

| µs/op (wall) | base | final | pipelined | base async | final async | pipelined async | c-native | c-native pipelined |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fillrandom, 1 thread | 2.70 | 2.57 | 2.77 | 2.78 | 2.79 | 2.83 | 4.16 | 4.16 |
| overwrite, 1 thread | 4.22 | 4.07 | 4.01 | 4.18 | 4.04 | 4.15 | 5.80 | 5.48 |
| fillrandom, 4 threads | 3.86 | 3.98 | **3.21** | 4.28 | 3.90 | 3.98 | 4.17 | 3.81 |
| overwrite, 4 threads | 5.70 | 4.64 | **3.88** | 5.41 | 4.73 | 4.94 | 5.09 | 4.25 |

CPU per op, 4 threads sync: fillrandom 14.8 / 14.1 / 11.8, overwrite 20.8 / 15.9 / 14.4 (base / final / pipelined).
With one writer there is nothing to overlap (the single writer leads both stages in turn) and the cost of the second
stage is within noise. With 4 sync writers pipelining gains 15 - 20 % wall, about what it gains on native (fillrandom
4.17 -> 3.81, overwrite 5.09 -> 4.25). Async writers gain nothing: their follower wake-ups are thread-pool work items,
so the stage hand-off adds a continuation per write that eats the overlap. Sync WAL writes (`--sync true`, 20k keys,
fsync-bound, ~80 - 115 µs/op) are unchanged by pipelining on both engines (managed 113.7 -> 97.9 and async 86.6 -> 82.0
µs/op within a 70 - 1070 µs/op spread on a loaded VM; native 85.6 -> 116.3). Pipelining stays off by default, as
upstream.

## Allocation-free readahead prefetches, forward pinned merge operands

BenchmarkDotNet `ScanBenchmarks.cs` (`--anyCategories Scan`, short job, 4 vCPU container, one op = one full scan of an
on-disk DB through the public iterator), before -> after:

| Benchmark | Before | After | Allocated before -> after |
|---|---|---|---|
| ColdScan (200k keys, 1 MiB block cache, fill_cache = false, auto readahead) | 14.1 ms | 13.7 ms | 6.4 KB -> - |
| MergeScan, 5 operands of 24 bytes per key (20k keys, 4 L0 files + memtable) | 7.8 ms | 8.4 ms | - -> - |
| MergeScan, 5 operands of 1 KiB per key | 40.0 ms (+-81 ms error) | 35.9 ms | - -> - |

The cold scan allocated one Task per prefetch (`AsTask` / `Task.FromResult`); the buffer now keeps the ValueTask and
waits on a reusable event when the read is still pending. Forward merges reference their operands in the temp-pinned
blocks instead of copying them into the operand buffer: no allocation change (the buffer was pooled), the copy saving
only shows with large operands and is within the noise of the short job.

## unordered_write

`dbbench --unordered_write true` (all engines; upstream semantics in docs/architecture.md "Write path"): the write group
only writes the WAL and publishes its sequence numbers, every writer inserts its own batch afterwards, concurrently and
without waiting for other groups. 200k keys, 100 B values, single-key writes, medians of 3 interleaved repetitions on
the shared 4-vCPU VM:

| µs/op (wall) | ordered | unordered | ordered async | unordered async | c-native ordered | c-native unordered |
|---|---:|---:|---:|---:|---:|---:|
| fillrandom, 4 threads | 3.66 | **2.25** | 3.86 | **2.65** | 4.13 | 2.87 |
| overwrite, 4 threads | 4.28 | **2.35** | 4.38 | **2.89** | 5.47 | 2.97 |
| fillrandom, 8 threads | 6.29 | **2.30** | 3.26 | **2.09** | 4.42 | 2.58 |
| overwrite, 8 threads | 6.30 | **2.32** | 3.76 | **2.33** | 4.77 | 2.95 |

The gain (35 - 63 % managed, 31 - 46 % native) is the memtable insert leaving the group's critical section: the next
WAL group forms while the previous writers insert. The managed engine keeps its lead over native in both modes.

## Async fdatasync on dedicated sync threads

`LocalWritableFile.SyncAsync` used to call fdatasync inline: the async caller (a thread-pool thread, typically the write
group leader) was blocked for the device flush. It now queues the fdatasync to `FileSyncWorkers` (dedicated threads,
pooled `IValueTaskSource`, asynchronous continuations) and awaits it; `LocalFileSystem.OffloadAsyncSync = false` (and
`dbbench --offload_async_sync false`) restores the inline call. `fillsync` (num/1000 = 10k synced puts, 100 B), medians
of 3 interleaved repetitions, same build, `offload_async_sync` false (= previous behaviour) vs true:

| fillsync µs/op (wall) | 1 thread | 4 threads | 16 threads | 64 threads |
|---|---:|---:|---:|---:|
| async API, inline fdatasync (before) | 159 | 79.5 | 33.4 | 14.4 |
| async API, sync threads (after) | 244 | 115 | 40.6 | 16.6 |
| sync API, before / after (unchanged path, noise) | 177 / 157 | 119 / 126 | 49.7 / 51.4 | 36.4 / 38.2 |

fdatasync itself takes ~140 µs here (virtual disk). The hand-off costs two thread wake-ups per sync -- the sync worker,
then the leader's continuation on the thread pool -- which this VM makes expensive: +85 µs per group at one writer,
shrinking to +15 % at 64 writers where groups are large. Variants measured on the way (same harness, 3 interleaved
repetitions each): continuations run inline on the sync thread (t1 208, t4 126, t16 46 µs/op; rejected: user code would
run on, and could block, the sync threads), workers polling the queue 30 / 50 / 200 µs before blocking (50 µs kept: t1
268 -> 229 with asynchronous continuations; 200 µs burned 3x the CPU per op for nothing), and a hybrid that synced inline
when the thread pool had no queued work (t1 168, t4 109: the queue is rarely empty with 4+ writers, and it would still
block pool threads). Allocation per op is unchanged (pooled requests). What the change buys is not throughput on an idle
pool but that synced async writers no longer hold thread-pool threads for the duration of the device flush (on real
disks: milliseconds), which is what starves other async work on a busy server; latency-sensitive single-writer
workloads on fast storage can turn it off.

**Default: off.** Given the measured latency cost and no throughput gain, the shipped default keeps the inline
fdatasync (the previous behaviour, like upstream's synchronous `Fsync` on the write thread); set
`new LocalFileSystem { OffloadAsyncSync = true }` (or `dbbench --offload_async_sync true`) to opt in.

## Final run on a new VM CPU (2026-11-03)

`bench/results/2026-11-03-final-warm-r5.md` (06:11-07:31 UTC): the same matrix, sizes, engines and `REPEATS=5` as
"Final three-way results" above, on `cfb30f9` (everything in this log after that run: super version cache, pinned
table readers, AwaitState, parallel memtable inserts, reverse pinning, range-delete skipping, compaction readahead,
subcompactions, the managed LZ4 encoder, read sampling, ...). Quiet VM (each repetition started at a 1-minute load
average <= 1.5; 0.91-3.10 before the individual runs, median 1.52). **The VM came back with a different CPU**: "Intel
Xeon Processor @ 2.10GHz" (family 6 model 207, AVX-512 + AMX) instead of "@ 2.80GHz".

| configuration | rocksdb-sharp 11-02 | rocksdb-sharp 11-03 | managed 11-02 | managed 11-03 | managed-async 11-02 | managed-async 11-03 |
|---|---:|---:|---:|---:|---:|---:|
| 100 B, 1 thread, none | 1.06x | 1.08x [1.05-1.14] | 0.59x | 0.72x [0.70-0.76] | 0.64x | 0.73x [0.70-0.76] |
| 100 B, 4 threads, none | 1.01x | 1.05x [0.99-1.11] | 0.78x | 0.77x [0.73-0.82] | 0.79x | 0.75x [0.71-0.80] |
| 100 B, 1 thread, LZ4 | 1.11x | 1.07x [1.01-1.12] | 0.69x | 0.71x [0.67-0.75] | 0.76x | 0.74x [0.70-0.78] |
| 100 B, 4 threads, LZ4 | 1.01x | 1.04x [0.99-1.12] | 0.83x | 0.76x [0.73-0.81] | 0.78x | 0.74x [0.71-0.80] |
| 1 KiB, 1 thread, none | 1.04x | 1.05x [0.98-1.10] | 0.52x | 0.80x [0.76-0.85] | 0.55x | 0.81x [0.77-0.86] |
| 1 KiB, 4 threads, none | 1.03x | 1.04x [0.96-1.13] | 0.63x | 0.84x [0.77-0.91] | 0.60x | 0.85x [0.78-0.92] |
| 1 KiB, 1 thread, LZ4 | 0.99x | 1.03x [0.97-1.08] | 0.58x | 0.76x [0.72-0.81] | 0.64x | 0.76x [0.72-0.80] |
| 1 KiB, 4 threads, LZ4 | 1.02x | 1.02x [0.96-1.07] | 0.70x | 0.89x [0.82-0.92] | 0.74x | 0.84x [0.79-0.87] |
| all 8 matrix configurations | 1.03x | 1.05x [1.02-1.07] | 0.66x | 0.78x [0.76-0.80] | 0.68x | 0.78x [0.76-0.79] |
| compaction-heavy | 1.07x | 1.05x [0.94-1.15] | 0.58x | 0.62x [0.53-0.66] | 0.69x | 0.65x [0.58-0.73] |

**CPU or code?** Absolute medians, 11-03 / 11-02, geometric mean over all 116 cells: c-native 0.60x, rocksdb-sharp
0.61x, managed 0.71x, managed-async 0.68x. Every engine is faster on the new CPU, native more so, most of all where
the old VM's native times were I/O-bound (c-native `flush` 0.33x: 1 KiB flushes took 200-460 ms there, 39-56 ms
now; `compact` 0.53x; `readmissing` 0.46x). To separate the two, managed at `0f866b8` (the tree of the 11-02 run,
built from `git archive` in a scratch directory) and at `cfb30f9` ran interleaved with c-native on the new CPU, 3
repetitions, warm-up on (`bench/results/2026-11-03-ab-0f866b8-vs-cfb30f9.csv`; labels `old-*` / `new-*`):

| configuration (14 benchmarks; compaction-heavy 4) | old / C | new / C | new / old |
|---|---:|---:|---:|
| 100 B, 1 thread, none | 0.65x | 0.69x | 1.07x |
| 1 KiB, 1 thread, none | 0.69x | 0.79x | 1.15x |
| 1 KiB, 4 threads, LZ4 | 0.88x | 0.83x | 0.95x |
| compaction-heavy | 0.54x | 0.63x | 1.16x |
| all 46 | 0.71x | 0.76x | 1.06x |

So on the new CPU the old tree would have scored ~0.71x, not 0.66x. Of the 18% move (0.66x -> 0.78x) about 7% is the
CPU, about 6% is code (new / old 1.06x), the rest is the difference between these 4 configurations and the full
matrix, and noise. The code differences, per benchmark (new / old):

- **Better**: 4-thread sync writes (`fillrandom` 0.72x, `overwrite` 0.68x of the old tree with 1 KiB / LZ4; in the
  matrix 0.81-0.93x / 0.86-1.06x of C, previously 1.04-1.31x / 1.28-1.81x), 4-thread `readmissing` (0.54x; matrix
  100 B 0.59-0.61x of C, previously 1.42-1.68x), 4-thread `readrandom` (0.75x).
- **`flush` 1.40-2.40x slower** (new / old). Not the flush itself: since `01ad63b` the background flush runs the
  obsolete-file purge (which deletes the flushed WAL, 64+ MiB with 1 KiB values) before it accounts the job as
  finished, and `FlushAsync` waits for that accounting. Probe (100k x 1 KiB, fillseq + fillrandom + flush, 3 runs per
  side): with the WAL old 73-93 ms, new 111-130 ms; `--disable_wal true` old 64-102 ms, new 67-89 ms. Upstream signals
  the flush waiter when the memtable flush is installed (`WaitForFlushMemTables`), before the purge. A fix would
  release the waiter at install time and keep only `CloseAsync` waiting for the purge.
- **`seekrandom` with 1 KiB values, 1 thread: 1.69x** (new / old; 1.21x of C in the matrix). Seek + 10 Next over
  ~4 entries per 4 KiB block reads 3-4 consecutive data blocks, so the user-iterator auto readahead (`e4d4311`,
  `BlockBasedTableIterator.PrefetchIfNeeded`, after `num_file_reads_for_auto_readahead` = 2 sequential blocks)
  starts on nearly every seek and issues an extra 8 KiB+ async read into the prefetch buffer, which the next seek
  discards. Probe with the auto readahead disabled by an environment switch (not committed): 9.1-11.0 µs/op against
  16.2-17.1 (old tree 9.8-11.3). Upstream's `BlockPrefetcher` first calls `RandomAccessFile::Prefetch` (the OS
  `readahead`), which costs no copy and no buffer, and creates a prefetch buffer only where `Prefetch` is unsupported.
  The same buffers make sync `readseq` allocate 6-55 B/op (old tree 0.2-2.9 B/op; async scans do not).
- **Compaction-heavy reads** `readrandom` 1.22x, `seekrandom` 1.31x (new / old, 3 repetitions, CVs ~10%; in the
  5-repetition matrix run 0.64x / 0.47x of C). Not investigated further; read sampling adds one thread-static RNG
  step per Get / seek (`FileReadSample.ShouldSampleFileRead`), too little to explain 20%.
- **Allocations of compaction**: one full compaction of the 100 B DB 7.4 -> 31 MB, compaction-heavy fills 5-10 ->
  80-140 B/op (they count the background compactions), `compact` with 4 subcompactions 294 MB vs 74 MB with one.
  Not profiled; the compaction input readahead buffers (`09c0552`) are the likely source.

**Extra rows** (`2026-11-03-final-extras-r3.md`, 3 repetitions): 4-thread writes with `--enable_pipelined_write`
0.91x / 0.91x of C sync (async 1.25x / 1.11x), with `--unordered_write` 0.70x / 0.82x (async 0.94x / 0.91x);
`compact` of 1M x 100 B with 4 subcompactions 1.47x of C (C 697 -> 330 ms, managed 586 -> 484 ms: subcompactions
scale 1.2x on managed and 2.1x on native; not investigated); `readreverse` with 1 KiB values 0.27x, reverse
`seekrandom` 0.69x; 4-thread `readrandom` on the compaction-heavy tree 0.58x.

BenchmarkDotNet (`2026-11-03-final-bdn.md`, default job): managed / rocksdb-sharp 0.72x Put, 0.56x 100-put batch,
0.45x / 0.55x / 0.30x Get memtable / cached block / miss, 0.25x / 0.21x Seek + 10 / 100 Next, 0.62x MultiGet(16);
nothing allocates (1 B per Put, 26 B per 100-entry batch of arena growth). The binding gained more from the new CPU
(cached-block Get 3782 -> 2052 ns vs managed 1334 -> 1133 ns). Async over sync 0-11%.

Replication on the quiet machine (`2026-11-03-replication.md`): in-process switchover hold p50 4.6-6.1 ms, max
6.5-11.2 ms; multi-process `SwitchoverTest` 17.9-31.0 ms (median 27 ms; loaded 35-133 ms). With 100-entry batches at
full speed the replica now applies only 75-80% of the primary's 480-560k ops/s and needs 0.5-0.9 s to catch up.

## Replica apply: parallel insert of one large batch (2026-11-03)

A replica fed by 4 primary writers with 100-entry batches applied 75-80% of the primary's rate and needed 0.5-0.9 s to
catch up. The replica's apply loop was 95-98% busy; `perf` of the apply alone: 56% `InlineSkipList.Insert`, the rest
a few percent each (copy, decode, WAL, background flush). The primary inserts its write groups in parallel; the replica
inserted each merged chunk (~2,400 entries) with one thread, peaking at 456-493k entries/s. New
`WriteOptions.MemtableInsertParallelism` (`Db/DbImpl.SplitInsert.cs`) splits a lone writer's batch into ranges inserted
concurrently and publishes after the last one; the replica uses 4 threads: 0.70-0.77M entries/s with 2 threads,
0.95-1.14M with 4, no added allocation (pooled state, one thread-pool work item per extra range). End to end the lag p50
went from 39-268 ms to 5-8 ms and the catch-up from 0.1-0.6 s to 23-68 ms (load 4-6). Details: docs/benchmarks.md
"Replica apply with parallel memtable inserts", `bench/results/2026-11-03-replication-apply.md`.

## Final-run regressions: flush, readahead, compaction allocations, subcompaction scaling

Follow-up of "Final run on a new VM CPU". Interleaved A/B on the quiet 4-vCPU VM, warm-up on, medians (n = runs per
engine): `c` = c-native, `old` = tree `0f866b8`, `base` = `549770a` (final run + the FlushAsync waiter fix), `new` =
this change; `-a` = async API.

**1. `flush` (1.3 - 2x of C).** Stopwatch marks through the flush path showed 38 - 55 ms spent before the flush was
even scheduled, in the memtable switch's `ReleaseObsoleteClosedWals`: with a single column family a closed WAL is not
synced at the switch (lazy closed-WAL sync, as upstream), so its writer stays open until a sync write or the next
switch, which closed the obsolete ones inline. By then the flush's purge had already unlinked the file, so that
`close()` was the real deletion of a 64+ MiB file (ext4 freeing its blocks, `discard` mount; strace: `close` 53 ms).
The old tree synced and closed the WAL at the switch (28 ms of fdatasync, cheaper). Upstream moves these writers to
`logs_to_free_` and deletes them in the background purge; the port now hands them to the next `PurgeObsoleteFiles`
(or closes them at shutdown). Not the cause, checked on the way: WAL tracking (only with
`track_and_verify_wals_in_manifest`), MANIFEST writes (< 1 ms), the post-flush purge (already after the waiter
release). Also aligned: writable files are no longer `ftruncate(0)`d after `O_CREAT` (.NET `FileMode.Create`
does that unconditionally; upstream's `O_TRUNC` only truncates an existing file, and a truncate to 0 arms ext4's
auto_da_alloc flush on close).

| flush, ms | c | old | base | new | base-a | new-a |
|---|---:|---:|---:|---:|---:|---:|
| 100k x 1 KiB after fillseq,fillrandom,overwrite (n = 5) | 43.7 | 38.4 | 75.9 | **39.0** | 78.0 | **31.9** |
| 300k x 100 B, same list (n = 3) | 99.7 | | 112.4 | **83.6** | | |

The rest of a flush is the table write and its fdatasync (~50 ms for 100 MB here), as in C.

**2. User-iterator readahead (`seekrandom` 1 KiB 1.21x of C, `readseq` allocations).** The auto readahead read through
an `AsyncPrefetchBuffer` (8 KiB+ copy into a pooled buffer) on the third sequential block, i.e. on nearly every Seek +
10 Next over ~4 entries per block, and the next seek threw it away. Ported `BlockPrefetcher::PrefetchIfNeeded` as is:
`readahead_limit_` / `readahead_size_`, and with buffered I/O an OS prefetch (`IRandomAccessFile.Prefetch` =
`readahead(2)` on Linux, PosixRandomAccessFile::Prefetch) of the block plus readahead_size_ whenever a block lies past
the prefetched range, doubling to max_auto_readahead_size; blocks are read normally (from the page cache). The buffer is
only the fallback where `Prefetch` is not supported; `ReadOptions.readahead_size` keeps it, as upstream.

| µs/op, 100k x 1 KiB (n = 5) | c | base | new | base-a | new-a |
|---|---:|---:|---:|---:|---:|
| seekrandom | 14.85 | 16.61 | **10.21** | 9.93 | 10.83 |
| readseq (alloc B/op) | 0.96 | 0.69 (7.2) | 0.66 (**0.7**) | 0.68 (0.6) | 0.77 (0.6) |
| readseq 300k x 100 B (n = 3; alloc) | 0.47 | 0.24 (8.1) | 0.27 (**0.2**) | | |

(The old tree: seekrandom 9.38, readseq 0.64 / 0.7 B/op. The async paths never used the readahead.)

**3. Compaction allocations.** An allocation-tick sampler (EventListener on GCAllocationTick, temporary) showed
byte[] of 2 MiB and 4 MiB: every compaction input file (per subcompaction: all L0 files) got an `AsyncPrefetchBuffer`
of compaction_readahead_size (2 MiB) plus a 2 MiB + 64 KiB prefetch half (rented as 4 MiB); with dozens of concurrent
input iterators the shared ArrayPool overflowed and allocated them on the LOH. Upstream's compaction readahead with
buffered I/O is again only an OS prefetch (`fs_prefetch_support`: `Prefetch(offset, len + compaction_readahead_size)`
past `readahead_limit_`); ported the same way, the buffer stays as the fallback.

| compaction allocations | base | new |
|---|---:|---:|
| `compact` 1M x 100 B, 1 subcompaction | 74.0 MB | **0.45 MB** |
| `compact` 1M x 100 B, 4 subcompactions | 297 MB | **3.6 MB** |
| `compact` 300k x 100 B (matrix) | 22.7 MB | **2.1 MB** |
| compaction-heavy fillrandom / overwrite (B/op, n = 6) | 71.7 / 129 | **5.1 / 10.3** (old 4.8 / 10.3) |

**4. Subcompaction scaling (C 697 -> 330 ms, managed 586 -> 484 ms).** Trace marks: the 4 subcompactions start within
20 ms of each other on their own threads (LongRunning, not limited by max_background_jobs) and each processes 1/4 of
the records, but each took ~360 ms. With `DOTNET_TieredCompilation=0` or `DOTNET_TieredPGO=0` 4 subcompactions take
350 - 380 ms, and the measured `compact` showed 180 - 410 methods / 100 - 300 ms of JIT (also in the final extras
run): methods called once per output file / subcompaction (~60 calls to reach tier 1) were tiering up during the
measurement, competing for the 4 cores with the 4 subcompaction threads (background JIT, runtime suspensions for
tier-up: 19% of the subcompaction threads' samples in `PollGC`). The warm-up's fixed cycles create only a few output
files when the warm-up DB is small compared with target_file_size_base. dbbench now adds extra (overwrite + flush) x 4
+ compact cycles until a warm-up compaction compiles fewer than 10 methods (2 - 7 cycles, +2 - 4 s of warm-up here;
bench/README.md "Warm-up"). `fillrandom,compact`, 1M x 100 B, 16 MiB memtables, 4 MiB target files, 8 background jobs
(n = 3):

| compact, ms | c | base | new |
|---|---:|---:|---:|
| max_subcompactions 1 | 683 | 645 | **588** |
| max_subcompactions 4 | 354 | 446 | **294** |

(`new` with 4 subcompactions: jit 40 methods / 64 ms instead of 182 - 414.)

**5. Compaction-heavy reads (readrandom 1.22x, seekrandom 1.31x of the old tree).** Re-measured with 6 interleaved runs
(500k x 100 B, compaction-heavy shape): readrandom c 7.57, old 5.02, base 5.29, new **4.98** µs/op; seekrandom c 27.4,
old 11.9, base 16.4, new **10.6**. The seek regression was the readahead buffer of (2); readrandom is within noise
(CV ~10% in all trees). Read sampling (`FileReadSample`, one thread-static RNG step per Get / seek) was not changed.

## readrandom / overwrite allocations: memory growth, not per-op cost

The matrix reports showed `readrandom` at ~86 B/op with 1 KiB values (~29 B/op with 100 B) in every tree since the
start, and `overwrite` at ~80 B/op in the 300k x 100 B configurations, although `multireadrandom` on the same DB
allocates ~1 B/op. Allocation sources, found with GC `AllocationTick` sampling (an in-process EventListener in the
runner, now `DBBENCH_ALLOC_TYPES=1`, and `dotnet-trace collect --providers Microsoft-Windows-DotNETRuntime:0x1:5` with
the events grouped by call stack through TraceEvent; bench/README.md "Allocations"), `DOTNET_gcServer=0`:

- **`readrandom`: the empty block cache filling up.** Every byte is `BlockBufferPool.Rent` -> new array in
  `BlockFetcher.ReadBlock` on a block cache miss (the sampler attributes part of it to the `Block` object allocated next
  to it). The total scales with `--cache_size`, not with the number of misses: 100k x 1 KiB, fresh DB, first
  `readrandom`: 1 MiB cache 1.2 MB (11.9 B/op), 8 MiB 9.1 MB (91 B/op), 32 MiB 34.6 MB (346 B/op). The 8 MiB default
  over 100k / 300k reads is the 86 / 29 B/op of the matrix. Once the cache is full, a miss reuses the buffer of the
  block its insert evicts (the block's release returns it to `BlockBufferPool`) and a pooled `Block` object: a second
  `readrandom` on the same DB allocates 0.7 B/op, all of it the runner's own histograms (0.0 - 0.1 B/op once they are
  excluded, below), with 86% data-block misses. Same with HyperClock, lz4 / zstd, 4 threads and the async API
  (0.6 - 2 B/op before the runner fix). Upstream allocates the block contents on every miss and frees the evicted one;
  the port only allocates while the cache grows to its capacity, and the cache's own memory is the "cache entry
  itself" that cannot be avoided. Nothing on the Get path was changed.
- **`overwrite` (300k x 100 B): the second memtable's arena.** `fillrandom` leaves ~45 MB in the 64 MiB memtable, so
  `overwrite` switches to a second memtable while the first one is flushed; its arena slabs (`ArenaSlabPool.Rent`,
  20 - 26 MB) cannot come from the pool because the first memtable still holds its slabs, plus the flush's filter hash
  buffer growing past the retained size (`FilterEntryPool`, 3 - 4 MB of `ulong[]`). Repeating `overwrite`: 89.7, then
  0.2, 2.9, 2.6, 0.2 B/op (each later switch reuses a flushed memtable's slabs; the remainder is amortized flush /
  compaction buffers, ~1 - 2 MB per 64 MiB flush).

So neither is a per-operation allocation; the B/op of the first run on a fresh DB includes the growth of the engine's
memory up to the configured sizes (the C leg's malloc/mmap growth is not counted anywhere). Changes:

- dbbench: `alloc B/op` no longer counts the runner's worker histograms (2 x 30 KB per thread), the merged histogram and
  the report strings, which were the whole steady-state 0.2 - 0.7 B/op of reads.
- dbbench: `DBBENCH_ALLOC_TYPES=1|N` prints the sampled allocations by type after every benchmark.
- BenchmarkDotNet: `DbGetBenchmarks` has `Location = SstUncached` (1 MiB block cache, ~13 MB of data blocks: nearly
  every Get misses the cache and reads the block from the page cache). Allocated is `-` for managed (and native).
- Unit test `SteadyStateGetsWithBlockCacheMissesDoNotAllocate` (LRU and HyperClock): 4000 Gets with > 2000 data-block
  cache misses allocate 0 bytes once the cache is full.

Not done: retaining more freed block buffers process-wide (`BlockBufferPool` keeps 64 per size class) would let a new
DB reuse the warm-up DB's 8 MiB and hide the fill in the benchmark, at the cost of memory held after a DB is closed.

Interleaved A/B, base = `954536b`, new = this change (only the runner's accounting differs; the library is
unchanged), `fillseq,fillrandom,overwrite,flush,readrandom,readrandom,multireadrandom`, warm-up on, n = 4, medians:

| benchmark | 300k x 100 B µs/op base / new | B/op base / new | 100k x 1 KiB µs/op base / new | B/op base / new |
|---|---:|---:|---:|---:|
| overwrite | 3.21 / 3.24 | 63.4 / 70.2 | 4.61 / 4.14 | 17.5 / 17.9 |
| readrandom (first) | 2.95 / 2.88 | 28.9 / 28.7 | 3.40 / 3.24 | 86.4 / 85.8 |
| readrandom (second) | 2.95 / 2.88 | 0.2 / **0.0** | 3.13 / 3.18 | 0.7 / **0.1** |
| multireadrandom | 2.69 / 2.93 | 0.3 / 0.1 | 2.98 / 3.24 | 1.0 / 0.4 |

BenchmarkDotNet `DbGetBenchmarks` (default job, ns/op, Allocated): Memtable managed 786 / rocksdb-sharp 1463,
SstCached 1110 / 2020, Miss 116 / 419, **SstUncached 1996 / 3538**, all `-`.

## Definitive run (2026-11-03): after the final-run regression fixes

`bench/results/2026-11-03-definitive-r5.md` (10:44-12:03 UTC) is the current reference run. It uses the same matrix,
sizes, engines, `REPEATS=5` and CPU as "Final run on a new VM CPU" above ("Intel Xeon Processor @ 2.10GHz", family 6
model 207). It ran on `fb97d3d`, which adds the fixes of "Final-run regressions" and the runner's allocation
accounting of "readrandom / overwrite allocations". The VM was quiet: each repetition started at a 1-minute load
average <= 1.5 (0.71-3.21 before the individual runs, median 1.44). Same CPU, so the two runs compare cell by cell.
The native legs ran at the same speed: the geometric mean over the 116 cells of their absolute medians, new / old, is
0.99x for c-native and 0.98x for rocksdb-sharp. The managed engines' is 0.92x (sync) and 0.93x (async).

| configuration | rocksdb-sharp cfb30f9 | rocksdb-sharp fb97d3d | managed cfb30f9 | managed fb97d3d | managed-async cfb30f9 | managed-async fb97d3d |
|---|---:|---:|---:|---:|---:|---:|
| 100 B, 1 thread, none | 1.08x | 1.09x [1.02-1.13] | 0.72x | 0.68x [0.65-0.71] | 0.73x | 0.70x [0.66-0.72] |
| 100 B, 4 threads, none | 1.05x | 1.01x [0.96-1.07] | 0.77x | 0.76x [0.72-0.79] | 0.75x | 0.75x [0.70-0.78] |
| 100 B, 1 thread, LZ4 | 1.07x | 1.10x [1.02-1.14] | 0.71x | 0.68x [0.64-0.72] | 0.74x | 0.70x [0.65-0.73] |
| 100 B, 4 threads, LZ4 | 1.04x | 1.06x [1.01-1.12] | 0.76x | 0.74x [0.70-0.77] | 0.74x | 0.75x [0.70-0.78] |
| 1 KiB, 1 thread, none | 1.05x | 1.01x [0.93-1.09] | 0.80x | 0.69x [0.63-0.75] | 0.81x | 0.69x [0.64-0.74] |
| 1 KiB, 4 threads, none | 1.04x | 1.00x [0.95-1.07] | 0.84x | 0.77x [0.73-0.81] | 0.85x | 0.75x [0.71-0.79] |
| 1 KiB, 1 thread, LZ4 | 1.03x | 1.00x [0.94-1.05] | 0.76x | 0.67x [0.65-0.71] | 0.76x | 0.73x [0.70-0.77] |
| 1 KiB, 4 threads, LZ4 | 1.02x | 1.09x [1.05-1.17] | 0.89x | 0.81x [0.77-0.84] | 0.84x | 0.81x [0.76-0.84] |
| all 8 matrix configurations | 1.05x | 1.05x [1.02-1.06] | 0.78x | **0.72x [0.71-0.74]** | 0.78x | **0.73x [0.72-0.74]** |
| compaction-heavy | 1.05x | 1.06x [1.00-1.12] | 0.62x | 0.58x [0.54-0.62] | 0.65x | 0.59x [0.55-0.62] |

The 1 KiB configurations gained the most (0.80-0.89x -> 0.67-0.81x). Cell by cell (managed ratio vs C, previous ->
now):

- **`flush`**: 1.02-2.06x -> 0.63-0.93x (async 1.02-2.22x -> 0.66-1.02x). The managed flush takes 0.39-0.60x of its
  previous time, while C's is unchanged (0.89-1.06x). This is fix 1 of "Final-run regressions": the WAL writer's
  close, which was the real deletion of the 64+ MiB file, moved to the background purge.
- **`seekrandom`, 1 KiB, 1 thread**: 1.21x -> 0.64x (no compression) and 0.98x -> 0.68x (LZ4), from fix 2 (the OS
  readahead hint instead of a prefetch buffer). The per-benchmark mean went from 0.72x to 0.61x.
- **`overwrite`**: 0.86x -> 0.77x per benchmark, most of it with 1 KiB values (0.82-1.06x -> 0.65-0.90x).
- **Allocations**:
  - sync `readseq` 7-52 -> 0.0-1.0 B/op (fix 2).
  - One full compaction of the 100 B DB 31-39 -> 6-7.6 MB (fix 3).
  - Compaction-heavy `fillrandom` / `overwrite` 77 / 119 -> 4.8 / 10.4 B/op (fix 3).
  - `fillsync` 206-1590 -> 2-71 B/op, from the runner no longer counting its histograms.
  - `flush` 0.04-0.25 -> 0.05-0.55 MB per flush, with the increase in the 100 B configurations. Not investigated;
    small next to the 62-71 ms flush. Explained since (see "Definitive-run follow-ups").
- **Got worse, within the noise**:
  - 4-thread `readmissing` with 100 B values: 0.61x -> 0.85x (no compression) and 0.59x -> 0.80x (LZ4). Its rows
    have CVs of 15-25%, and the ranges of the old and new runs overlap.
  - 4-thread `fillrandom` with 100 B values, no compression: 0.82x -> 0.98x. C was 10% faster on this cell than in
    the previous run, and managed's absolute time rose 7%.
- **Above 1.1x of C**, only 2 of 232 managed cells, both noisy:
  - managed-async `readmissing`, 100 B, 4 threads, LZ4: 1.11x. The cell is 0.21 against 0.19 µs; C was 34% faster
    on this cell than in the previous run, and the CVs are 16-20%.
  - managed `fillsync`, 1 KiB, 4 threads, LZ4: 1.17x (was 1.23x). It does one fdatasync per put, and C's CV is 20%.

Write amplification of the compaction-heavy configuration is unchanged: C 2.96, managed 3.05-3.15 (median 3.15),
managed-async 3.15. JIT during measurements: median 1 method per benchmark. The most is still in the compaction-heavy
fills (up to 635 methods), and the managed total over 585 measurements is 4024, down from 5579. Noise: median CV
7.8-8.7% per engine (was 8.3-10.2%), with 82 of 116 rows flagged (was 93).

**Extras** (`2026-11-03-definitive-extras-r3.md`, 3 repetitions):

- **Subcompactions now scale**, from fix 4 (warm-up until compactions stop compiling methods). One subcompaction
  takes 535 ms against C's 706 (0.76x), and 4 take 269 against 315 ms (0.85x; previously 1.47x). That is a 2.0x
  speedup for managed against 2.2x for C. The JIT during the measured `compact` with 4 subcompactions is down to 27
  methods. Allocations are 0.44 / 3.4 MB (1 / 4 subcompactions), down from 74 / 294 MB.
- **4-thread writes**:
  - Default write path: 0.79x / 0.85x (`fillrandom` / `overwrite`).
  - Pipelined: 0.71x / 0.86x, with async at 0.97x / 1.06x (was 1.25x / 1.11x).
  - Unordered: 0.82x / 0.86x, with the managed times unchanged and C faster in this session (2.79 against 3.31 µs).
- **1 KiB, 1 thread**: `flush` 0.80-0.84x (async 0.67-0.86x), `readreverse` 0.29x, reverse `seekrandom` 0.68x.

BenchmarkDotNet (`2026-11-03-definitive-bdn.md`, default job): managed / rocksdb-sharp is 0.68x for Put, 0.45x for a
100-put batch, and 0.58x / 0.56x / 0.28x for Get from the memtable / a cached block / a missing key. A Get that misses
the block cache is 0.58x, Seek + 10 / 100 Next 0.24x / 0.21x, and MultiGet(16) 0.65x. Nothing allocates (1 B per Put,
26 B per 100-entry batch). Async over sync costs -5% to +17%, with TryGet the highest.

## Definitive-run follow-ups: 4-thread recheck, flush allocations, write-stall install (2026-11-03)

**1. 4-thread `readmissing` / `fillrandom` with 100 B values: noise, not a regression.** The definitive run showed
`readmissing` at 0.85x / 0.80x of C (none / LZ4; previously 0.61x / 0.59x) and `fillrandom` without compression at
0.98x (previously 0.82x). Interleaved A/B on the quiet VM (load 2-4 from the runs themselves), `nice -n -15`, 7
repetitions, each repetition c-native, `old` = `79d3c6b` (the tree of the previous run, before the regression fixes)
and `new` = `29bd2ec`, sync and async in one process each, benchmarks `fillseq,fillrandom,overwrite,flush,readrandom,readmissing`,
300k x 100 B, 4 threads, warm-up on. Medians, ratio to C, min-max:

| µs/op | C | old | new | old async | new async |
|---|---:|---:|---:|---:|---:|
| fillrandom, none | 4.68 | 3.99 (0.85x) | 3.88 (0.83x) | 4.19 (0.89x) | 4.04 (0.86x) |
| fillrandom, LZ4 | 4.85 | 3.75 (0.77x) | 3.85 (0.79x) | 4.00 (0.83x) | 4.28 (0.88x) |
| readmissing, none | 0.194 | 0.188 (0.97x) [0.12-0.23] | 0.160 (0.83x) [0.11-0.17] | 0.197 (1.02x) | 0.178 (0.92x) |
| readmissing, LZ4 | 0.222 | 0.183 (0.83x) [0.15-0.21] | 0.163 (0.73x) [0.09-0.18] | 0.189 (0.85x) | 0.203 (0.91x) |
| flush, none (ms) | 84.8 | 93.6 (1.10x) | 51.2 (0.60x) | 100.7 (1.19x) | 54.9 (0.65x) |
| flush, LZ4 (ms) | 83.2 | 101.3 (1.22x) | 58.3 (0.70x) | 101.3 (1.22x) | 58.9 (0.71x) |

Per repetition, new / old: `fillrandom` 0.97x / 1.02x (sync, none / LZ4) and 0.96x / 1.06x (async), `readmissing`
0.81x / 0.85x (sync) and 0.97x / 1.00x (async), with single repetitions from 0.45x to 1.53x. `readmissing` is ~0.2
µs/op (a memtable miss and one bloom probe per file), so a context switch in a 4-thread run moves it by 30-50%. A
second A/B (4 repetitions, new vs. the final tree of this section) gave `readmissing` 0.73-0.77x (async 0.77-0.89x) and `fillrandom`
0.86-0.90x of C for both trees. None of the code since `79d3c6b` is on these paths (the readahead changes are iterator /
compaction only, `MemtableInsertParallelism` is checked only for a lone writer with the option set), and the
`readmissing` / `fillrandom` cells of the definitive run were inside the noise of their rows.

**2. Flush allocations (0.04-0.25 -> 0.05-0.55 MB per flush).** Bimodal in every tree: the measured `flush` of
300k x 100 B with 4 threads allocated ~40 / ~80 KB or ~260 / ~300 KB (none / LZ4) in `29bd2ec`, 40-97 / 170-186 KB in
`79d3c6b`, with no change of what is allocated. `DBBENCH_ALLOC_TYPES=1` and `dotnet-trace` (GC AllocationTick with
stacks, the window of the last flush): the large mode is `ArrayPool<T>.Shared` misses in the table builder (the index
block buffer of `BlockBuilder` and its restart `uint[]` growing to 128-256 KiB, the full filter output, the
compressed-block buffer), the rest is the new table reader's index and filter blocks (`BlockFetcher.ReadBlock`,
`ReadRawPayload`), i.e. memory the reader keeps. `ArrayPool.Shared` keeps a returned array in the returning thread's
local slot first, and flushes and compactions run on whichever pool / background thread is free, so whether the next
builder finds the arrays depends on the thread (the same effect that made `FilterEntryPool` and the builder's staging
pool necessary). The table builders' growable buffers now come from `TableBuilderBuffers` (lock-based
`ArrayPool.Create` instances, not trimmed): BlockBuilder contents and restarts, the full filter, the compressed-block
buffer and the output staging buffer. n = 4-5 per side:

| | new (`29bd2ec`) | this change |
|---|---:|---:|
| `flush` alloc, 300k x 100 B, 4 threads, none | 39-262 KB | 170-174 KB |
| `flush` alloc, same, LZ4 | 80-302 KB | 211-229 KB |
| `compact` alloc (300k x 100 B) | 1.06-2.16 MB | 0.10-1.15 MB |
| compaction-heavy `fillrandom` / `overwrite` (500k x 100 B, 4 MiB memtables, n = 3) | 4.9-6.0 / 9.9-10.8 B/op | 4.1-4.4 / 8.7-10.1 B/op |

What is left in the flush window is the new table reader's index and filter blocks. Previously they could reuse the
arrays the builder had just returned to `ArrayPool.Shared` on the same thread (the 40 KB mode); now the builder's
arrays go back to their own pool, so the reader allocates its blocks unless an earlier reader's are free. Over
sustained writes (the compaction-heavy fills) the total went down; a single flush is no longer bimodal. Flush and fill
times are unchanged (`flush` 0.68-0.79x of C in the second A/B against 0.65-0.81x).

**3. `WriteSlowdownDelaysWrites` (rate 73399 = 102400 x 0.8^3 x 1.4 where 0 was expected).** A temporary trace of
every write-stall recalculation (thread, condition, L0 files, rate, call stack) reproduced the exact rate in a normal
run: the stall is recalculated at open twice (two super version installs with the same version) and once more when
compactions are enabled (102400 -> 81920 -> 65536), by the memtable switch of CompactRange's flush (52428); the gated
L0 compaction (one file, L0 3 -> 2) releases the delay (x 1.4 = 73399); the flush installs L0 = 3 again (delayed at
73399); the next L0 compaction empties L0. CompactRange waits for that compaction (its L0 pick conflicts), so the test
could only see 73399 if CompactRange returned after the version with the empty L0 was installed but before the stall
was recalculated: compactions, RefitLevel and flushes installed the version inside `LogAndApplyAsync`'s mutex section
and the super version (with the stall recalculation) in a second one after releasing the mutex, and the waiting manual
compaction can be woken in between. Upstream's LogAndApply returns with the DB mutex held and
InstallSuperVersionAndScheduleWork runs before it is released. Fixes (product, `75dbcfa`):

- `VersionSet.LogAndApplyAsync(..., installedUnderMutex, ...)`: compaction, RefitLevel and flush install their super
  version in the critical section that installs the version. New test `CompactionInstallsVersionAndWriteStallTogether`
  (a hook right after the MANIFEST write reads the version's L0 and the stall under the mutex) failed every time before.
- `InstallSuperVersion` recalculates the stall only if the version or the memtables changed
  (`ColumnFamilyData::InstallSuperVersion`: "treats it as further slowing down"), and the extra recalculations (enabling
  compactions, the end of every compaction, bottom-priority jobs) are gone: the DB of this test now starts at exactly
  delayed_write_rate (asserted), not at 0.64x of it.
- A continued delay acquires its new token before releasing the old one, so the WriteController keeps its credit and
  refill time (`GetDelayToken` resets them only when the delayed count goes from 0 to 1); before, every recalculation
  of a delayed CF forgave the write debt (`ContinuedDelayKeepsWriteControllerDebt`).

The test now asserts what upstream guarantees right after CompactRange (L0 empty, no stall), without
`WaitForCompact`. Stress: 60 runs under 6 CPU hogs before the fix (no failure, as in earlier attempts); after it, 30
runs of the whole `DbCompactionTests` class under 6 hogs, 3 x 15 parallel runs of the class (plus 2 hogs), 20 runs each
with `DOTNET_TieredCompilation=0` and `DOTNET_TieredPGO=0 DOTNET_TC_QuickJitForLoops=0` (4 hogs): no failure.

## Table reader blocks from the table buffer pool; table cache eviction race (2026-11-03)

**1. A new table reader's index and filter.** After `TableBuilderBuffers`, the measured `flush` (300k x 100 B, 4
threads) still allocated 170-210 KB. `dotnet-trace` (GC AllocationTick with stacks, grouped by stack for the window of
the measured benchmark) showed what: the reader that `FlushAndInstallAsync` opens reads its full filter into
`ArrayPool<byte>.Shared` (`ReadRawPayload`, a 128 KiB array for the 64 MiB flush; a miss, since the shared pool keeps
returned arrays per thread first) and its index into `BlockBufferPool` (a hit or a miss of one fine size class). In the
compaction-heavy fills, the readers that compactions open for their L0 / L1 inputs missed the same way (4.5 MB over a
1.5M-op `fillrandom` + `overwrite` run, warm-up included), although the readers of deleted files had returned theirs:
filters went to whichever thread closed the reader, index blocks to one of 32 fine size classes per MiB.

Now the index block and the full filter that a reader pins (not those in the block cache with
`cache_index_and_filter_blocks`, which keep the block buffer pool's finer classes because the cache is charged the
buffer length) are rented from `TableBuilderBuffers.Bytes`, and returned there when the reader is closed (the `Block`
records its pool, `BlockBufferSource`; compressed index blocks are decompressed into the same pool). Their sizes match
the builder's index and filter buffers, which the builder has just returned to that pool, so the reader of a new table
takes them, and the blocks of deleted tables go back to where the next builders look. Retention went from 16 to 32
arrays per size class. Cost: a pinned index block is held in a power-of-two array (up to 2x its size, ~1.4x on average)
instead of a fine size class (at most 1.25x); filters were power-of-two arrays before too.

A/B, 5 interleaved repetitions (`f9a895f` vs this change, one process each, warm-up on, `--cache_size 8388608`), medians
[min-max] of the bytes allocated during the benchmark:

| | before | after |
|---|---:|---:|
| `flush`, 300k x 100 B, 4 threads, none | 171 KB [171-174] | 40 KB [39-41] |
| `flush`, same, LZ4 | 212 KB [212-212] | 40 KB [39-40] |
| `compact`, same, none | 0.10 MB [0.10-1.09] | 0.95 MB [0.95-2.00] |
| `compact`, same, LZ4 | 1.09 MB [0.04-1.15] | 0.96 MB [0.95-2.01] |
| compaction-heavy `fillrandom` (500k x 100 B, 4 MiB memtables / files) | 4.7 B/op [4.4-4.9] | 4.0 B/op [3.8-4.1] |
| compaction-heavy `overwrite` | 9.4 B/op [7.5-9.6] | 8.7 B/op [6.3-9.2] |

Over a 1.5M-op compaction-heavy `fillrandom` + `overwrite` process (traced, warm-up included), the reader block
allocations went from 4.5 MB to none and the builder's pool misses from 1.7 to 2.7 MB; the process allocated 78 instead
of 86 MB. What is left in the steady state of those fills is per-file, not per-block: the write buffers of new SST and
WAL files (`LocalWritableFile`), the strings of parsed table properties, `CompactionJob` / `LogWriter` objects and event
log strings.

The measured `compact` is the largest compaction of the run (all of L0 into one 300k-key output), and its builder now
misses the filter and index arrays of that size (512 + 256 KB), which the live L0 readers hold until the compaction
installs its result and closes them (before, the builder pool kept a set of the flushes' arrays for itself, and the
readers had allocated their own copies at each flush). It is the same memory as before, allocated at a different point
of this sequence; flush + compact together went from 0.27 to 1.0 MB (none) and 1.30 to 1.0 MB (LZ4) in these runs.

Times are unchanged within the noise. A second A/B (7 repetitions, after load < 1.5, the same allocation results): `flush`
57.2 -> 58.6 ms (none; ranges 51.6-66.5 / 52.7-75.2) and 60.8 -> 65.0 ms (LZ4; 57.1-76.4 / 55.3-90.2), `compact`
135 -> 147 ms (none; 130-155 / 132-187) and 154 -> 143 ms (LZ4; 135-196 / 139-160); in the first A/B `flush` with LZ4
was 56.5 -> 71.1 ms and `compact` without compression 135 -> 142 ms. Two more A/Bs of `fillrandom,overwrite,flush` with
LZ4 only: 65.8 -> 69.1 ms (15 repetitions, per-repetition ratios 0.90-1.36, median 1.06), then 70.7 -> 62.0 ms with
process CPU time 61.9 -> 47.3 ms per flush (10 repetitions). The fills moved by -4% to +4%.

**2. `DbBlobTests.BlobReadersBoundedByMaxOpenFiles` (failed once in a full run).** Reproduced in 1 of 100 runs of the
`DbBlobTests` class: `ArgumentException: Unable to sort because the IComparer.Compare() method returns inconsistent
results` thrown by `List.Sort` in `TableCacheStore.EvictUnreferenced`, out of a `Get` of the test's concurrent phase
(8 threads, `max_open_files = 25`, 30 SST files). Not a blob problem: the table cache (capacity 20) evicts on every miss
and sorted the unreferenced handles by their `LastUse` field, which lookups of open readers stamp without the lock
(`FindTable`'s fast path). A stamp that changes during the sort makes the introsort's partition scan run off the
range, which .NET reports as an inconsistent comparer; with more than 16 candidates (insertion sort below that) and
concurrent hits this can happen in any DB with `max_open_files != -1` and concurrent reads, failing the read. Fix
(product): the stamps are copied with the handles before sorting. New test
`DbColumnFamilyAndConcurrencyTests.BoundedTableCacheEvictsUnderConcurrentLookups` (capacity 54, 120 files, six threads
hitting a hot set while two read all files, 2 s) failed 4 of 10 runs before the fix and 0 of 20 after; the
`DbBlobTests` class passed 100 of 100 runs after it with 2 CPU hogs (1 of 100 failed before, without hogs).

## Gaps / follow-ups

- HyperClock table as a struct array (one cache line per probe, like upstream `ClockHandle`).
- Data block linear scan could skip comparisons using the shared-prefix length (incremental comparison).
- Batched MultiGet exists (perf pass #2) but does not prefetch filter cache lines or interleave the block reads of a batch.
- Block cache charge does not include the `Block` object (~64 bytes).
- The first read / overwrite benchmark on a fresh DB shows the block cache fill and the second memtable's arena in
  alloc B/op (see "readrandom / overwrite allocations"); a byte-bounded `BlockBufferPool` retention would let a
  reopened DB reuse a closed one's block buffers.
- Confirmed on a quiet machine: the 4-thread `fillrandom` / `readmissing` ratios vs C are noise, not a regression (see
  "Definitive-run follow-ups"). Done since: pipelined writes (see "Pipelined writes and parallel memtable inserts"), reverse-iteration block pinning, async reverse
  movement, covered-range skipping and compaction input readahead (see "Reverse iteration, range-deleted runs,
  compaction input readahead").
- Done: a new table reader's pinned index and filter come from `TableBuilderBuffers` (see "Table reader blocks from
  the table buffer pool"). Left in the compaction-heavy steady state: per-file allocations (the `LocalWritableFile`
  write buffer of every new SST / WAL file, parsed table property strings, event log strings).
- Done since: merge operands are referenced in pinned blocks in both directions, `NextAsync` after reverse movement
  awaits, async table paths load index partitions asynchronously, user iterators use (auto) readahead, and the
  readahead's prefetches no longer allocate a Task each (see "Allocation-free readahead prefetches, forward pinned
  merge operands").
- From the 2026-11-03 final run: done (see "Final-run regressions: flush, readahead, compaction allocations,
  subcompaction scaling"), confirmed by the definitive run (see "Definitive run (2026-11-03)"). Still open: `readrandom` with 1 KiB values allocates ~86 B/op (100 B: ~29 B/op) in every
  tree since `0f866b8` (block cache misses with the 8 MiB cache; not investigated); the dbbench `overwrite` row of the
  300k x 100 B matrix configuration allocates ~80 B/op (not investigated).
