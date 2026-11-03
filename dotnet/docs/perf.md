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
- **Pipelined writes** (`enable_pipelined_write`): accepted and persisted, but writes use the non-pipelined path.

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

## Gaps / follow-ups

- HyperClock table as a struct array (one cache line per probe, like upstream `ClockHandle`).
- Data block linear scan could skip comparisons using the shared-prefix length (incremental comparison).
- Batched MultiGet exists (perf pass #2) but does not prefetch filter cache lines or interleave the block reads of a batch.
- Block cache charge does not include the `Block` object (~64 bytes).
- Confirm the 4-thread `fillrandom` / `overwrite` / `readmissing` ratios vs C on a quiet machine (see "Read/write
  contention with 4 threads"); pipelined writes are open. Done since: reverse-iteration block pinning, async reverse
  movement, covered-range skipping and compaction input readahead (see "Reverse iteration, range-deleted runs,
  compaction input readahead").
- Done since: merge operands are referenced in pinned blocks in both directions, `NextAsync` after reverse movement
  awaits, async table paths load index partitions asynchronously, user iterators use (auto) readahead, and the
  readahead's prefetches no longer allocate a Task each (see "Allocation-free readahead prefetches, forward pinned
  merge operands").
