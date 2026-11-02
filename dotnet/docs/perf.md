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
  native; steady-state flush / compaction CPU per entry is at or below native.
- LZ4 flushes: K4os LZ4's compressor is JIT compiled (82 ms of JIT for `LZ4_compress_generic` instantiations / OSR
  variants) and runs as tier-0 code during the first flush; fillrandom 300k + flush with LZ4 is 720 - 845 ms vs
  290 - 343 ms native. Not addressed (third-party code; a warm-up or an in-house LZ4 encoder would be needed).
- Compaction picking under sustained writes: managed lets L5 grow to 9 - 10 files before L5 -> L6 compactions while
  native 11.12 keeps it at 4 - 6, so managed L0 -> L5 compactions read ~1.4x more L5 data (write-amp 3.05 - 3.33 vs
  2.96) and more work is left at the end of the writes (`waitforcompaction` 0.3 - 1.7 s vs 0.02 - 0.25 s). Fixed
  where it was a fidelity problem, see "Compaction-picking parity" below.
- Range-partitioned subcompactions (`max_subcompactions`) are not implemented; native defaults to 1, so they would not
  change the comparison.

## Compaction-picking parity (compaction-heavy configuration)

Follow-up of the last gap of perf pass #3. Method: a lock-step harness
(`tests/RocksDb.Managed.CompatTests/Compaction/CompactionPickerParityTests.cs`) runs the same seeded random-write stream
on native (rocksdb-sharp) and managed with the compaction-heavy options scaled down 16x (256 KiB memtable = target
file size, 1 MiB level base, dynamic level bytes, no compression, `max_background_jobs = 1`). After every memtable
both engines flush and `WaitForCompact`, so the LSM shapes depend only on flush / picker / output-cutting logic, and
both LOGs ("Compacting N@L + M@L' files to Lx", "Moved #...", flush and output table sizes) give the compaction
sequence and the bytes written. Managed now writes the upstream lines (`OnCompactionStarted`, "Generated table",
"Moved #", "Level-0 flush table"). The EVENT_LOG JSON is not usable for the native side under .NET 11 (the native
JSON writer stops after the first field there), so the test parses the plain-text lines.

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

## Gaps / follow-ups

- HyperClock table as a struct array (one cache line per probe, like upstream `ClockHandle`).
- Data block linear scan could skip comparisons using the shared-prefix length (incremental comparison).
- Batched MultiGet exists (perf pass #2) but does not prefetch filter cache lines or interleave the block reads of a batch.
- Block cache charge does not include the `Block` object (~64 bytes).
