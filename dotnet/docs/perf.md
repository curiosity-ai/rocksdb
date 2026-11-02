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
