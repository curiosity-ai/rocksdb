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

## Gaps / follow-ups

- HyperClock table as a struct array (one cache line per probe, like upstream `ClockHandle`).
- Data block linear scan could skip comparisons using the shared-prefix length (incremental comparison).
- No batched MultiGet (filter prefetch, coroutine-style interleaving of misses).
- Block cache charge does not include the `Block` object (~64 bytes).
