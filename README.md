# RocksDb.Managed

[![NuGet](https://img.shields.io/nuget/v/RocksDb.Managed.svg?label=RocksDb.Managed)](https://www.nuget.org/packages/RocksDb.Managed)
[![NuGet](https://img.shields.io/nuget/v/RocksDb.Managed.Replication.svg?label=RocksDb.Managed.Replication)](https://www.nuget.org/packages/RocksDb.Managed.Replication)
[![NuGet](https://img.shields.io/nuget/v/RocksDb.Managed.Replication.MagicOnion.svg?label=RocksDb.Managed.Replication.MagicOnion)](https://www.nuget.org/packages/RocksDb.Managed.Replication.MagicOnion)

A fully managed, async-first C# port of [RocksDB](https://rocksdb.org) for **.NET 10 and .NET 11**. No native
library, no P/Invoke: the whole storage engine (WAL, memtables, block-based SST tables, flush, compaction, recovery,
transactions, BlobDB, backups) is C# ported file by file from the RocksDB C++ sources.

- **Binary compatible on disk.** A database written by native RocksDB (C++, Java, rocksdb-sharp, ...) opens with
  RocksDb.Managed, and the other way round. Tests compare bytes with the native library and alternate both engines on
  one directory.
- **Faster than the native library from .NET.** On a db_bench-style workload the managed engine takes 0.72x the
  time of the RocksDB C API (see [Benchmarks](#benchmarks)).
- **Allocation-free hot paths.** Reads, seeks, scans and steady-state writes allocate nothing per operation;
  keys and values are spans.
- **Async first.** `ValueTask` APIs for every I/O-bound operation, with group commit for writes and
  `IAsyncEnumerable` scans; cache hits complete synchronously.
- **Replication included.** Checkpoint + WAL streaming, planned switchover and a Raft cluster layer, with an
  optional gRPC transport.

The engine follows RocksDB **11.14**. This repository is a fork of [facebook/rocksdb](https://github.com/facebook/rocksdb):
the port lives in [`dotnet/`](dotnet/), and the C++ tree at the root is the reference implementation it is ported
from and tested against.

## Install

| Package | Contents |
|---|---|
| [`RocksDb.Managed`](https://www.nuget.org/packages/RocksDb.Managed) | the storage engine |
| [`RocksDb.Managed.Replication`](https://www.nuget.org/packages/RocksDb.Managed.Replication) | replication (checkpoint + WAL streaming), planned switchover, Raft cluster; transport-agnostic |
| [`RocksDb.Managed.Replication.MagicOnion`](https://www.nuget.org/packages/RocksDb.Managed.Replication.MagicOnion) | gRPC (MagicOnion) transport and hosting for the replication layer |

```bash
dotnet add package RocksDb.Managed
```

The packages target `net10.0` and `net11.0`, depend only on managed packages (`System.IO.Hashing`, `Snappier`,
`K4os.Compression.LZ4`, `ZstdSharp.Port`) and are AOT-compatible.

## Examples

Keys and values are bytes. The synchronous API takes `ReadOnlySpan<byte>`, so `"key"u8` literals and `stackalloc`
buffers work without allocating; the asynchronous API takes `ReadOnlyMemory<byte>`. Every example below is compiled
and run by [`dotnet/samples/QuickStart`](dotnet/samples/QuickStart).

### Open, write, read

<!-- snippet:open-sync -->
```csharp
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

db.Put("hello"u8, "world"u8);                       // ReadOnlySpan<byte> in, no allocation
byte[]? value = db.Get("hello"u8);                  // simplest read: allocates the result
Console.WriteLine(System.Text.Encoding.UTF8.GetString(value!));   // world

db.Delete("hello"u8);
Console.WriteLine(db.ContainsKey("hello"u8));       // False
```

### Async

<!-- snippet:open-async -->
```csharp
await using RocksDatabase db = await RocksDatabase.OpenAsync(path, new DbOptions { CreateIfMissing = true });

// Async writes join the group commit; they complete when the batch is in the WAL and memtable.
await db.PutAsync("user:1"u8.ToArray(), "alice"u8.ToArray());
byte[]? alice = await db.GetAsync("user:1"u8.ToArray());   // completes synchronously on cache hits

await db.FlushAsync();                                      // memtable -> L0 SST
await db.CompactRangeAsync();                               // full manual compaction
```

### Typed options

Options are plain classes with typed properties and upstream defaults, written to a native-format `OPTIONS` file on
open.

<!-- snippet:typed-options -->
```csharp
var dbOptions = new DbOptions
{
    CreateIfMissing = true,
    CreateMissingColumnFamilies = true,
    MaxBackgroundJobs = 4,
    WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery,   // enums, not strings
};
var cfOptions = new ColumnFamilyOptions
{
    WriteBufferSize = 32UL << 20,
    Compression = CompressionType.LZ4,
    BottommostCompression = CompressionType.Zstd,
    CompactionStyle = CompactionStyle.Level,
    TableOptions = new BlockBasedTableOptions
    {
        FormatVersion = 7,
        BlockSize = 16 * 1024,
        FilterPolicy = new BloomFilterPolicy(10),            // or new RibbonFilterPolicy(10, 0)
        BlockCache = BlockCache.CreateLru(256L << 20),       // or BlockCache.CreateHyperClock(...)
        CacheIndexAndFilterBlocks = true,
    },
};

using RocksDatabase db = RocksDatabase.Open(path, dbOptions, cfOptions);

// Options are plain readable objects; they are persisted to the native-compatible OPTIONS-xxxxxx file.
Console.WriteLine(db.DefaultColumnFamily.Options.Compression);   // LZ4
DbOptions live = db.GetDBOptions();                              // a copy of the DB-wide options

// Mutable options can be changed on a live DB, typed or with native option names.
db.SetOptions(db.DefaultColumnFamily, [new("disable_auto_compactions", "true")]);
db.DefaultColumnFamily.SetOptions(o => o.Level0FileNumCompactionTrigger = 8);
```

### Atomic write batches

<!-- snippet:write-batch -->
```csharp
using (WriteBatch batch = db.CreateWriteBatch())               // pooled; Dispose returns it
{
    batch.Put("k1"u8, "v1"u8)
         .Delete("stale"u8)
         .DeleteRange("tmp:"u8, "tmp;"u8)                      // [begin, end)
         .Put(audit, "k1"u8, "created"u8);                     // another column family
    db.Write(batch);                                           // atomic
}

using (WriteBatch batch = db.CreateWriteBatch())
{
    batch.Put("k2"u8, "v2"u8);
    batch.SetSavePoint();
    batch.Put("k3"u8, "oops"u8);
    batch.RollbackToSavePoint();                               // drops k3
    await db.WriteAsync(WriteOptions.SyncWrites, batch);       // fsync the WAL before completing
    Console.WriteLine(batch.Count);                            // 1
}
using (WriteBatch batch = db.CreateWriteBatch())
{
    batch.Put("logged"u8, "1"u8);
    batch.MarkWalTerminationPoint();                           // later records skip the WAL (memtable only)
    batch.Put("cache-only"u8, "2"u8);
    db.Write(batch);
}
// The batch representation (batch.Data) is byte-identical to native rocksdb::WriteBatch::Data().
```

### Iterators

<!-- snippet:iterators -->
```csharp
// Cursor style: Key/Value are spans into pinned blocks, valid until the iterator moves. No allocation per step.
using (Iterator it = db.NewIterator(new ReadOptions { IterateUpperBound = "d"u8.ToArray() }))
{
    for (it.Seek("b"u8); it.Valid; it.Next())
        Console.WriteLine($"{(char)it.Key[0]} = {(char)it.Value[0]}");      // b, c (d is the bound)
    for (it.SeekToLast(); it.Valid; it.Prev()) { /* reverse: c, b, a */ }
}

// foreach over a range [start, end) with a ref struct enumerator.
using (Iterator it = db.NewIterator())
{
    foreach ((ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) in it.Range("b"u8, "e"u8))
        Console.WriteLine($"{(char)key[0]} = {(char)value[0]}");            // b, c, d
}

// Async scan: block reads that miss the cache are awaited instead of blocking the thread.
using (Iterator it = await db.NewIteratorAsync(null))
{
    await foreach (KeyValue kv in it.ScanAsync("c"u8.ToArray(), null))     // null = unbounded
        Console.WriteLine(kv.KeySpan.Length);   // kv memory is reused: copy (kv.KeyToArray()) to keep it
}
```

### Column families

<!-- snippet:column-families -->
```csharp
var options = new DbOptions { CreateIfMissing = true };
using (RocksDatabase db = RocksDatabase.Open(path, options))
{
    ColumnFamily users = db.CreateColumnFamily("users", new ColumnFamilyOptions());
    users.Put("u1"u8, "alice"u8);                    // the column family is the receiver
    await users.PutAsync("u2"u8.ToArray(), "bob"u8.ToArray());
    db.Put("config"u8, "v1"u8);                      // db.* = default column family
}

// Reopen: like native RocksDB, every existing column family must be listed.
List<string> names = RocksDatabase.ListColumnFamilies(path);            // [default, users]
var descriptors = new List<ColumnFamilyDescriptor>();
foreach (string name in names) descriptors.Add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));
using (RocksDatabase db = RocksDatabase.Open(path, options, descriptors))
{
    ColumnFamily users = db.GetColumnFamily("users");
    Console.WriteLine(users.Get("u1"u8) is not null);                   // True
    db.DropColumnFamily(users);
}
```

### Following the WAL

<!-- snippet:wal-tailing -->
```csharp
ulong start = db.GetLatestSequenceNumber() + 1;
db.Put("a"u8, "1"u8);
using (WriteBatch batch = db.CreateWriteBatch())
{
    batch.Put("b"u8, "2"u8).Delete("a"u8);
    db.Write(batch);
}

// Batches already in the WAL. Each WalBatch is the native WriteBatch representation (feed it to a native
// or managed replica with new WriteBatch(b.Span)); it is valid until the iteration moves on.
using (TransactionLogIterator updates = db.GetUpdatesSince(start))
    await foreach (WalBatch b in updates)
        Console.WriteLine($"seq {b.Sequence}..{b.LastSequence}: {b.Count} ops");

// Follow the WAL forever: awaits new writes (signalled by the write path, no polling) and WAL rotations.
using var cts = new CancellationTokenSource();
Task writer = Task.Run(async () => { await db.PutAsync("c"u8.ToArray(), "3"u8.ToArray()); });
await foreach (WalBatch b in db.TailUpdatesAsync(start, cts.Token))
{
    foreach (WriteBatchEntry e in b.GetReader())
        Console.WriteLine($"  {e.Type} {System.Text.Encoding.UTF8.GetString(e.Key)}");
    if (b.LastSequence >= start + 3) break;          // or cts.Cancel() from elsewhere
}
await writer;
```

Many more examples, covering transactions, snapshots, merge operators, prefix seek, compaction filters, wide columns,
user-defined timestamps, BlobDB, TTL, checkpoints and backups, SST ingestion, read-only and secondary instances,
listeners, statistics and caches, are in the [API tour](dotnet/README.md#api-tour).

## Benchmarks

The same db_bench-style workload runs on three engines, each in its own process on a fresh database:

- **c-native**: a C program on the RocksDB C API, linked to the `librocksdb` 11.12 that the `RocksDB` NuGet package
  ships. It is the baseline.
- **rocksdb-sharp**: the `RocksDB` NuGet binding, using its span APIs.
- **managed**: RocksDb.Managed, sync and async (`ValueTask`) APIs.

Time per operation relative to c-native, lower is better (below 1.00x is faster than C). Geometric mean of the median
ratios over 14 benchmarks, with the 95% bootstrap confidence interval; 5 repetitions per cell:

| configuration | rocksdb-sharp | managed | managed (async) |
|---|---:|---:|---:|
| 100 B values, 1 thread | 1.09x | 0.68x | 0.70x |
| 100 B values, 4 threads | 1.01x | 0.76x | 0.75x |
| 100 B values, 1 thread, LZ4 | 1.10x | 0.68x | 0.70x |
| 100 B values, 4 threads, LZ4 | 1.06x | 0.74x | 0.75x |
| 1 KiB values, 1 thread | 1.01x | 0.69x | 0.69x |
| 1 KiB values, 4 threads | 1.00x | 0.77x | 0.75x |
| 1 KiB values, 1 thread, LZ4 | 1.00x | 0.67x | 0.73x |
| 1 KiB values, 4 threads, LZ4 | 1.09x | 0.81x | 0.81x |
| **all 8 configurations (112 benchmarks)** | **1.05x** [1.02-1.06] | **0.72x** [0.71-0.74] | **0.73x** [0.72-0.74] |
| compaction-heavy (500k x 100 B, multi-level LSM) | 1.06x [1.00-1.12] | 0.58x [0.54-0.62] | 0.59x [0.55-0.62] |

Per benchmark, geometric mean over the 8 configurations:

| benchmark | rocksdb-sharp | managed | managed (async) |
|---|---:|---:|---:|
| fillseq | 1.00x | 0.68x | 0.66x |
| fillrandom | 1.03x | 0.74x | 0.77x |
| overwrite | 1.03x | 0.77x | 0.78x |
| flush | 0.97x | 0.73x | 0.73x |
| readrandom | 1.04x | 0.67x | 0.73x |
| readmissing | 1.11x | 0.60x | 0.70x |
| seekrandom | 1.09x | 0.61x | 0.63x |
| readseq | 1.18x | 0.66x | 0.66x |
| readreverse | 1.14x | 0.69x | 0.68x |
| multireadrandom | 1.04x | 0.71x | 0.73x |
| readwhilewriting | 1.03x | 0.78x | 0.78x |
| compact | 0.99x | 0.84x | 0.82x |
| deleterandom | 1.03x | 0.71x | 0.75x |
| fillsync | 0.97x | 1.02x | 0.90x |

Steady-state reads, seeks and scans allocate 0-1 bytes per operation. `fillsync` measures the disk (one `fdatasync`
per write), so all engines are close there.

Release build on .NET 10.0.14, 4 vCPUs (Intel Xeon @ 2.10 GHz), report
[`dotnet/bench/results/2026-11-03-definitive-r5.md`](dotnet/bench/results/2026-11-03-definitive-r5.md). The method,
how to run it, extra workloads (pipelined and unordered writes, subcompactions, mixed read/write), micro-benchmarks and
replication throughput are in [dotnet/docs/benchmarks.md](dotnet/docs/benchmarks.md).

## Compatibility

Every persisted format is covered, in both directions: WAL (including recyclable records and WAL compression),
`WriteBatch` (also the replication wire format), MANIFEST, `OPTIONS` files, block-based SSTs (`format_version` 2-8,
all index types, Bloom / Ribbon / partitioned filters, Snappy / LZ4 / ZSTD / Zlib compression), BlobDB, wide columns,
user-defined timestamps, transaction WAL markers, backups and checkpoints. Data, index and filter blocks are
byte-identical to native. The full matrix and the known deviations are in [dotnet/README.md](dotnet/README.md).

Correctness is checked by:

- about 2,000 unit tests, including crash-consistency and fault-injection tests;
- about 540 compatibility tests that run the same operations on native RocksDB and compare bytes and results;
- about 150 round trips that alternate native and managed on one database directory, across option and crash matrices;
- a Jepsen-style [soak test](dotnet/docs/soak.md) (process kills, power loss, torn writes, LazyFS) and a
  [cluster soak test](dotnet/docs/cluster-soak.md) for the Raft layer (partitions, kills, clock skew).

## Replication

[`RocksDb.Managed.Replication`](dotnet/src/RocksDb.Managed.Replication/README.md) streams a primary's WAL to replicas
after an initial checkpoint, performs planned switchovers between a primary and a replica, and runs a Raft cluster
with leader election and quorum-committed writes. It is transport-agnostic;
`RocksDb.Managed.Replication.MagicOnion` provides the gRPC transport, and
[`dotnet/samples`](dotnet/samples) has multi-process examples.

## Building from source

```bash
cd dotnet
dotnet build RocksDb.Managed.slnx                     # net10.0 and net11.0
dotnet run --project samples/QuickStart -f net10.0     # runs every README example
```

`dotnet/global.json` pins the .NET 11 SDK. [dotnet/docs/testing.md](dotnet/docs/testing.md) describes the test
suites, [dotnet/docs/architecture.md](dotnet/docs/architecture.md) maps the C++ modules to the C# ones, and
[dotnet/CLAUDE.md](dotnet/CLAUDE.md) holds the porting conventions.

## License

RocksDb.Managed (everything under [`dotnet/`](dotnet/)) is licensed under the
[Apache License, Version 2.0](LICENSE.Apache). Code derived from LevelDB, xxHash, MurmurHash and the .NET runtime is
attributed in [dotnet/THIRD-PARTY-NOTICES.txt](dotnet/THIRD-PARTY-NOTICES.txt).

The RocksDB C++ sources in the rest of this repository keep their upstream license: GPLv2 ([COPYING](COPYING)) or
Apache 2.0 ([LICENSE.Apache](LICENSE.Apache)), at your option, with LevelDB-derived code under
[LICENSE.leveldb](LICENSE.leveldb).
