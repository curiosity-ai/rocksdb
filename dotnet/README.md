# RocksDb.Managed

A fully managed, async-first C# port of [RocksDB](https://rocksdb.org) for **.NET 10 and .NET 11**. No native
library, no P/Invoke: the storage engine (WAL, memtable, block-based SST tables, flush, compaction, recovery) is
C# code ported file by file from the C++ sources in the parent directory.

The port reads and writes the same on-disk formats as native RocksDB. A database directory written by native RocksDB
(C++, Java, rocksdb-sharp, ...) opens with RocksDb.Managed, and the other way round. This is checked by tests that
compare bytes with the native library and by round-trip tests that alternate both engines on one directory.

```csharp
using RocksDb.Managed;

await using RocksDatabase db = await RocksDatabase.OpenAsync("/data/db", new DbOptions { CreateIfMissing = true });
await db.PutAsync("hello"u8.ToArray(), "world"u8.ToArray());
byte[]? value = await db.GetAsync("hello"u8.ToArray());
```

Contents: [Status and compatibility](#status-and-compatibility) |
[Build](#build-and-install) | [Quick start](#quick-start) | [API tour](#api-tour) |
[Error handling](#error-handling) | [Known deviations and gaps](#known-deviations-and-gaps) | [Documentation](#documentation)

## Status and compatibility

The reference implementation is RocksDB **11.14** (`main` in this repository). Native compatibility is tested against
the `RocksDB` NuGet package (rocksdb-sharp), which bundles `librocksdb` **11.12**.

"Byte-identical" means that for the same input and options, the managed port writes exactly the bytes native RocksDB
writes, and the compat tests compare them. "Read + write" means that each engine reads what the other one wrote,
while the bytes may differ (for example, because a compressor makes different choices).

| Format / feature | Managed reads native | Native reads managed | Notes |
|---|:---:|:---:|---|
| WAL (legacy and recyclable records, zstd WAL compression, metadata records) | yes | yes | all recovery modes; `GetUpdatesSince` returns the same sequences and bytes as native |
| WriteBatch representation | yes | yes | byte-identical (also the replication wire format) |
| MANIFEST / VersionEdit, `CURRENT`, `IDENTITY`, `LOCK` | yes | yes | every tag in 11.14; native records re-encode byte-identically; the `LOCK` file lock works across engines |
| `OPTIONS-xxxxxx` files | yes | yes | typed options; unknown or custom ids are kept verbatim |
| Block-based SST, `format_version` 2-8 | yes | yes | data, index and filter blocks byte-identical to native `SstFileWriter`; checksums crc32c, xxHash, xxHash64, XXH3 |
| Index types | yes | yes | binary search, delta encoding, first-key index, partitioned (two-level) index, hash index (`kHashSearch`); data block hash index |
| Optional block layouts | yes | yes | separated key/value data blocks, index `is_uniform` hint (used by `IndexBlockSearchType.Auto` interpolation search), `block_align`, super block alignment; byte-identical to native |
| User table properties collectors | yes | yes | `TablePropertiesCollector` properties and `rocksdb.property.collectors` as native; `NeedCompact` marks files for compaction |
| Filters: FastLocalBloom, legacy Bloom, Ribbon (incl. hybrid), prefix, partitioned | yes | yes | byte-identical; with `optimize_filters_for_memory`, Ribbon may differ by 16 bytes depending on heap state (both readable) |
| Compression: Snappy, LZ4, LZ4HC, ZSTD, Zlib | yes | yes | read + write; ZSTD (with dictionaries) byte-identical; see deviations below |
| Compression: BZip2, Xpress | no | n/a | a configured BZip2/Xpress type stores blocks uncompressed |
| Integrated BlobDB (blob files, blob GC) | yes | yes | blob files byte-identical |
| Wide columns (`PutEntity`, V1 entities) | yes | yes | compaction filters on entities (`FilterV3`, `FilterV4` with a lazy blob resolver, `kChangeWideColumnEntity`); `MultiGetEntity`, attribute groups, coalescing / attribute-group iterators, lazy entity reads (`GetEntityLazy`, batched `MultiResolve`) |
| Wide-column entities with blob-backed columns (V2 entities) | yes | yes | written by flush/compaction value separation and blob direct write; blob GC and garbage accounting of entity columns |
| Blob direct write (`enable_blob_direct_write`, partitions) | yes | yes | needs RocksDB with blob direct write (librocksdb 11.12 has it); off by default; like upstream v1, blob files not registered by a flush before a crash are not recovered |
| User-defined timestamps (persisted) | yes | yes | timestamped writes byte-identical; `full_history_ts_low` GC; timestamped range deletions and `SstFileWriter` files |
| User-defined timestamps in memtable only (`persist_user_defined_timestamps = false`) | yes | yes | tables/MANIFEST without timestamps, WAL with them; flush cutoff and postponement; enabling/disabling UDT on an existing column family |
| Transactions with user-defined timestamps (commit timestamps, read-timestamp validation) | yes | yes | plain and 2PC commits (`kTypeCommitXIDAndTimestamp`) recover across engines; `GetForUpdate` with `doValidate: false`; optimistic transactions cannot commit timestamped writes (as upstream) |
| Transactions: 2PC prepare/commit/rollback WAL markers | yes | yes | byte-identical; prepared transactions recover across engines |
| WritePrepared / WriteUnprepared transactions (`kTypeBeginPersistedPrepareXID`, `kTypeBeginUnprepareXID`, sequence per sub-batch) | yes | yes | WAL records byte-identical; committed, rolled back and prepared (also multi-batch unprepared) transactions recover across engines; other write policies refuse the WAL like upstream |
| `DBWithTTL` value format | yes | yes | |
| BackupEngine directory layout and meta files | yes | yes | incremental backups across engines |
| Checkpoints, external SST ingestion | yes | yes | |
| File checksums in the MANIFEST (`file_checksum_gen_factory`, `FileChecksumGenCrc32cFactory`) | yes | yes | identical checksums for flushed, compacted, blob and ingested files; `VerifyFileChecksums` on either side |
| PlainTable / CuckooTable, AdaptiveTableFactory reading | yes | yes | plain data / index / bloom and cuckoo hash tables byte-identical to native SstFileWriter output; `[TableOptions/PlainTable]` / `[TableOptions/CuckooTable]` OPTIONS sections round-trip; cuckoo reads need `AllowMmapReads` (as upstream) |
| User-defined index (`UserDefinedIndexFactory`, `IndexMode`, `ReadOptions.ReadIndex`), built-in trie index (`TrieIndexFactory`, "trie_index") | yes | yes | custom index meta block and kCustomOnly footer feature as upstream; trie-index blocks are byte-identical to native and each side reads the other's files through the trie |

Supported features: column families, point reads (`Get`, `MultiGet`), iterators (bounds, prefix seek, reverse,
tailing), snapshots, merge operators, single delete, range deletion, leveled / universal / FIFO compaction with
compaction filters and TTL, manual compaction, write stalls, block cache (LRU and HyperClock), statistics, event
listeners, the info `LOG`, dynamic options (`SetOptions` / `SetDBOptions`), optimistic and pessimistic transactions,
`WriteBatchWithIndex`, wide columns, user-defined timestamps, BlobDB, TTL, checkpoints, BackupEngine, `SstFileWriter` /
ingestion, WAL tailing, read-only and secondary instances, background error handling with `Resume`, best-efforts recovery, WAL tracking in
the MANIFEST (`TrackAndVerifyWalsInManifest`) and predecessor-WAL records in each WAL (`TrackAndVerifyWals`), WAL filters
(`DbOptions.WalFilter`), write callbacks (`WriteWithCallback` with a `UserWriteCallback`), an `SstFileManager` (space limit, rate-limited deletion, automatic
recovery from out-of-space errors) and a `GenericRateLimiter` for flush and compaction writes and, in reads-only / all-I/O
mode, compaction input and user reads (`DbOptions.RateLimiter`, `ReadOptions.RateLimiterPriority`).

The [replication layer](src/RocksDb.Managed.Replication/README.md) (checkpoint + WAL streaming, planned switchover,
Raft cluster, optional gRPC transport) is a separate library on top of the engine.

## Build and install

There is no NuGet package yet. Reference the project from your solution:

```xml
<ProjectReference Include="path/to/dotnet/src/RocksDb.Managed/RocksDb.Managed.csproj" />
```

The library depends on `System.IO.Hashing`, `Snappier`, `K4os.Compression.LZ4` and `ZstdSharp.Port` (all managed)
and is AOT-compatible. To build the solution and run the examples:

```bash
cd dotnet
dotnet build RocksDb.Managed.slnx                          # both TFMs, zero warnings expected
dotnet run --project samples/QuickStart -f net10.0          # runs every example in this README
dotnet run --project samples/QuickStart -f net11.0 -- Transactions WalTailing   # a subset
```

`global.json` pins the .NET 11 SDK (`rollForward: latestMajor`), which builds both `net10.0` and `net11.0`. See
[docs/testing.md](docs/testing.md) for the test suites and [docs/benchmarks.md](docs/benchmarks.md) for the
benchmarks.

## Quick start

Every snippet in this README is copied from [`samples/QuickStart`](samples/QuickStart), which builds with the solution
and checks what each example prints. After changing an example, run `python3 samples/QuickStart/sync-readme.py` from
`dotnet/` to update this file.

Keys and values are bytes. The synchronous API takes `ReadOnlySpan<byte>` (so `"key"u8` literals and `stackalloc`
buffers work without allocating). The asynchronous API takes `ReadOnlyMemory<byte>`, because the data must survive an
`await`.

<!-- snippet:open-sync -->
```csharp
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

db.Put("hello"u8, "world"u8);                       // ReadOnlySpan<byte> in, no allocation
byte[]? value = db.Get("hello"u8);                  // simplest read: allocates the result
Console.WriteLine(System.Text.Encoding.UTF8.GetString(value!));   // world

db.Delete("hello"u8);
Console.WriteLine(db.ContainsKey("hello"u8));       // False
```

<!-- snippet:open-async -->
```csharp
await using RocksDatabase db = await RocksDatabase.OpenAsync(path, new DbOptions { CreateIfMissing = true });

// Async writes join the group commit; they complete when the batch is in the WAL and memtable.
await db.PutAsync("user:1"u8.ToArray(), "alice"u8.ToArray());
byte[]? alice = await db.GetAsync("user:1"u8.ToArray());   // completes synchronously on cache hits

await db.FlushAsync();                                      // memtable -> L0 SST
await db.CompactRangeAsync();                               // full manual compaction
```

Async calls complete synchronously when nothing has to wait (memtable and block-cache hits, page-cache hits through
`preadv2(RWF_NOWAIT)` on Linux), so they cost about as much as the sync calls. The sync API never blocks on thread-pool
work: a synchronous writer that becomes the write-group leader does the WAL I/O on its own thread.

## API tour

### Options

Options are plain classes with typed, readable properties and upstream defaults (`DbOptions`,
`ColumnFamilyOptions`, `BlockBasedTableOptions`, `ReadOptions`, `WriteOptions`, `FlushOptions`,
`CompactRangeOptions`). Closed sets of values are enums. Filter policies, comparators, merge operators, compaction
filters and prefix extractors are objects. The DB writes them to a native-format `OPTIONS` file on open.

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

### Column families

The column family is the receiver: `db.Put(...)` writes to the default column family, and `cf.Put(...)` writes to
another one. `ColumnFamily` has the same read/write API as `RocksDatabase`.

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

### Reading values without allocating

`Get(key)` returns a new `byte[]`. The other read methods avoid that allocation:

<!-- snippet:read-variants -->
```csharp
// 1. Copy into a caller-owned span (no allocation). False = not found; valueLength is the full length,
//    so valueLength > buffer.Length means the copy was truncated.
Span<byte> buffer = stackalloc byte[64];
if (db.TryGet("name"u8, buffer, out int valueLength) && valueLength <= buffer.Length)
    Console.WriteLine(System.Text.Encoding.UTF8.GetString(buffer[..valueLength]));

// 2. Append into any IBufferWriter<byte> (ArrayBufferWriter, PooledBufferWriter, a pipe...).
var writer = new ArrayBufferWriter<byte>();
bool found = db.TryGet("name"u8, writer);

// 3. Zero-copy callback: the span points into the memtable or the cached block. Use static lambdas.
long counter = db.Get("counter"u8, 0, static (value, _) => BinaryPrimitives.ReadInt64LittleEndian(value));

// 4. Pooled buffer: dispose returns it to the pool.
using (PooledValue? pooled = db.GetPooled("name"u8))
    if (pooled is not null) Console.WriteLine(pooled.Length);

// 5. byte[] when convenience matters more than allocation.
byte[]? copy = db.Get("name"u8);

// Each variant also takes ReadOptions (snapshot, fill-cache, ...) and has an async twin:
// TryGetAsync(key, writer), GetPooledAsync(key), GetAsync(key).
```

### WriteBatch

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

### MultiGetBatch

`MultiGetBatch` reads all of its keys from one consistent view, like native `DB::MultiGet`. It visits each SST file
once per batch, checks the filter for all keys before reading data blocks, and shares block reads between keys.

<!-- snippet:multiget -->
```csharp
using var batch = new MultiGetBatch();          // reusable: no allocation once warmed up
batch.Add("a"u8).Add("b"u8).Add("c"u8);
db.MultiGet(batch);                             // or: await db.MultiGetAsync(batch)
for (int i = 0; i < batch.Count; i++)
{
    switch (batch.GetStatus(i))
    {
        case MultiGetStatus.Found:
            Console.WriteLine($"{i}: {System.Text.Encoding.UTF8.GetString(batch.GetValue(i))}");
            break;
        case MultiGetStatus.NotFound:
            Console.WriteLine($"{i}: not found");
            break;
        case MultiGetStatus.Error:
            throw batch.GetError(i)!;
    }
}
batch.Clear();                                  // keep the buffers for the next batch
```

### Iterators

An `Iterator` supports cursor-style movement (`Seek`, `SeekForPrev`, `SeekToFirst`, `SeekToLast`, `Next`, `Prev`), each
with an async twin. It also has a `foreach` range enumerator and an `IAsyncEnumerable` scan. Bounds, prefix mode,
snapshots, `PinData` and `Tailing` come from `ReadOptions`.

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

### Snapshots

<!-- snippet:snapshots -->
```csharp
db.Put("k"u8, "old"u8);
using Snapshot snapshot = db.GetSnapshot();          // consistent view; Dispose releases it
db.Put("k"u8, "new"u8);

var atSnapshot = new ReadOptions { Snapshot = snapshot };
Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get(atSnapshot, "k"u8)!));   // old
Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get("k"u8)!));               // new
using Iterator it = db.NewIterator(atSnapshot);       // iterators honour the snapshot too
```

### Merge operators

A merge operator writes its result to an `IBufferWriter<byte>` and reads operands as spans. It must be thread-safe,
because reads, flushes and compactions call it concurrently.

Upstream's built-in operators are included under their native names, and an OPTIONS file naming one
(`merge_operator=max`, `{id=StringAppendOperator;delimiter=,;}`, ...) recreates it: `UInt64AddOperator`,
`StringAppendOperator`, `StringAppendTestOperator`, `MaxOperator`, `PutOperator` / `PutOperatorV2`, `SortListOperator`
(`MergeSortOperator`), `BytesXorOperator` (`BytesXOR`) and the aggregation operator
`Utilities.AggMerge.AggMergeOperator` (`AggMergeOperator.v1`, with `Aggregator`s registered through
`AggMerge.AddAggregator`). `MergeOperator.CreateBuiltin(id)` resolves the same ids.

<!-- snippet:merge-operator-class -->
```csharp
/// <summary>Keeps the maximum of little-endian int64 operands.</summary>
private sealed class Int64MaxOperator : MergeOperator
{
    public override string Name => "Int64MaxOperator";  // persisted in OPTIONS; keep it stable and unique

    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
        in MergeOperands operands, IBufferWriter<byte> result)
    {
        long max = hasExistingValue ? BinaryPrimitives.ReadInt64LittleEndian(existingValue) : long.MinValue;
        for (int i = 0; i < operands.Count; i++)
            max = Math.Max(max, BinaryPrimitives.ReadInt64LittleEndian(operands[i]));
        BinaryPrimitives.WriteInt64LittleEndian(result.GetSpan(8), max);
        result.Advance(8);
        return true;                                      // false = merge failure (Corruption)
    }
}
```

<!-- snippet:merge-operators -->
```csharp
var options = new DbOptions { CreateIfMissing = true, CreateMissingColumnFamilies = true };
using RocksDatabase db = RocksDatabase.Open(path, options,
[
    // Built-ins share the native names (uint64add, stringappend, max, put, bytesxor, ...), so native RocksDB opens the DB too.
    new ColumnFamilyDescriptor("default", new ColumnFamilyOptions { MergeOperator = MergeOperator.UInt64Add }),
    new ColumnFamilyDescriptor("tags", new ColumnFamilyOptions { MergeOperator = MergeOperator.StringAppend(",") }),
    new ColumnFamilyDescriptor("max", new ColumnFamilyOptions { MergeOperator = new Int64MaxOperator() }),
]);

Span<byte> one = stackalloc byte[8];
BinaryPrimitives.WriteUInt64LittleEndian(one, 1);
db.Merge("hits"u8, one);
db.Merge("hits"u8, one);
ulong hits = db.Get("hits"u8, 0, static (v, _) => BinaryPrimitives.ReadUInt64LittleEndian(v));   // 2

ColumnFamily tags = db.GetColumnFamily("tags");
tags.Merge("post:1"u8, "db"u8);
tags.Merge("post:1"u8, "dotnet"u8);
Console.WriteLine(System.Text.Encoding.UTF8.GetString(tags.Get("post:1"u8)!));   // db,dotnet
```

### Prefix seek

<!-- snippet:prefix-seek -->
```csharp
var cfOptions = new ColumnFamilyOptions
{
    PrefixExtractor = SliceTransform.CreateFixedPrefix(5),          // "user:" / "post:"; persisted as rocksdb.FixedPrefix.5
    TableOptions = { FilterPolicy = new RocksDb.Managed.Table.Filters.BloomFilterPolicy(10) },   // prefix blooms
};
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
db.Put("user:1"u8, "a"u8);
db.Put("user:2"u8, "b"u8);
db.Put("post:1"u8, "c"u8);
db.Flush();

// PrefixSameAsStart: the iterator stops when the prefix of the key changes; filters skip files without it.
using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true });
int users = 0;
for (it.Seek("user:"u8); it.Valid; it.Next()) users++;            // 2
// TotalOrderSeek = true ignores the prefix extractor for one iterator.
```

### Table formats: PlainTable, CuckooTable, adaptive reading

<!-- snippet:table-formats -->
```csharp
// PlainTable: for in-memory / tmpfs databases (no blocks, compression or checksums). With a prefix extractor,
// lookups use a prefix hash index and iterators only support prefix seeks.
var plain = new ColumnFamilyOptions
{
    TableFactory = TableFactoryType.PlainTable,                    // table_factory=PlainTable
    PrefixExtractor = SliceTransform.CreateFixedPrefix(5),
    PlainTableOptions = { EncodingType = PlainTableEncodingType.Prefix, StoreIndexInFile = true },
};
using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "plain"), new DbOptions { CreateIfMissing = true }, plain))
{
    db.Put("user:1"u8, "a"u8);
    db.Put("user:2"u8, "b"u8);
    db.Flush();                                                    // a plain table SST, readable by native RocksDB
    using Iterator it = db.NewIterator(new ReadOptions { PrefixSameAsStart = true });
    for (it.Seek("user:"u8); it.Valid; it.Next()) { /* user:1, user:2 */ }
}

// CuckooTable: fixed-size keys and values, one version per key, read memory-mapped (AllowMmapReads).
var cuckoo = new ColumnFamilyOptions { TableFactory = TableFactoryType.CuckooTable, CuckooTableOptions = { HashTableRatio = 0.9 } };
using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "cuckoo"), new DbOptions { CreateIfMissing = true, AllowMmapReads = true }, cuckoo))
{
    db.Put("key00001"u8, "val1"u8);
    db.Flush();
    byte[]? v = db.Get("key00001"u8);                              // "val1"
}

// AdaptiveTableFactory: reads block-based, plain and cuckoo files by their magic number; writes one format.
var adaptive = new ColumnFamilyOptions
{
    TableFactory = TableFactoryType.AdaptiveTable,
    AdaptiveTableWriteFactory = TableFactoryType.BlockBasedTable,
    PrefixExtractor = SliceTransform.CreateFixedPrefix(5),
};
using (RocksDatabase db = RocksDatabase.Open(Path.Combine(path, "plain"), new DbOptions(), adaptive))
{
    byte[]? v = db.Get("user:2"u8);                                // read from the plain table file
}
```

### Flush, compaction and compaction filters

Background flushes and compactions run automatically. They support leveled (dynamic level bytes, all `CompactionPri`
values, trivial moves, intra-L0, TTL and periodic compaction), universal and FIFO compaction, with write stalls and
delayed writes like upstream.

<!-- snippet:compaction-filter-class -->
```csharp
/// <summary>Drops every key starting with "tmp:" when compaction rewrites it.</summary>
private sealed class DropTemporaryKeys : CompactionFilter
{
    public override string Name => "DropTemporaryKeys";

    public override CompactionFilterDecision Filter(int level, ReadOnlySpan<byte> key, CompactionFilterValueType valueType,
        ReadOnlySpan<byte> existingValue, IBufferWriter<byte> newValue, IBufferWriter<byte> skipUntil) =>
        key.StartsWith("tmp:"u8) ? CompactionFilterDecision.Remove : CompactionFilterDecision.Keep;
}
```

<!-- snippet:compaction -->
```csharp
var cfOptions = new ColumnFamilyOptions
{
    CompactionStyle = CompactionStyle.Level,          // also Universal and Fifo
    Level0FileNumCompactionTrigger = 4,
    TargetFileSizeBase = 64UL << 20,
    CompactionFilter = new DropTemporaryKeys(),
};
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
for (int i = 0; i < 1000; i++)
    db.Put(System.Text.Encoding.UTF8.GetBytes((i % 10 == 0 ? "tmp:" : "key:") + i), "value"u8);
db.Flush();

// Compact everything (null, null) or a key range; Force rewrites the bottommost level too.
db.CompactRange(new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.Force }, null, null);
await db.CompactRangeAsync("key:0"u8.ToArray(), "key:5"u8.ToArray());
await db.WaitForCompactAsync();                       // until no flush/compaction is pending

// Compact chosen table files into a level (files a valid compaction needs are added); returns the outputs.
db.Put("key:x"u8, "value"u8);
db.Flush();
List<string> l0 = db.GetLiveFilesMetadata().FindAll(f => f.Level == 0).ConvertAll(f => f.Name);
IReadOnlyList<string> outputs = db.CompactFiles(new CompactFilesOptions { MaxSubcompactions = 2 }, l0, 6);

Console.WriteLine(db.GetProperty(DbProperties.NumFilesAtLevelPrefix + "6"));   // files at L6
string? stats = db.GetProperty(DbProperties.Stats);                             // native-format stats dump
```

### Transactions

`OptimisticTransactionDatabase` validates at commit time, either serially or in parallel (the upstream default).
`TransactionDatabase` takes point (or range) locks and supports lock timeouts, deadlock detection, expiration and two-phase
commit. Both expose the underlying `RocksDatabase` as `Database`. `TransactionDbOptions.WritePolicy` selects
WriteCommitted (default), WritePrepared (data goes to the memtables at prepare; readers use a commit cache) or
WriteUnprepared (large transactions are written to the DB before prepare once their batch exceeds
`TransactionOptions.WriteBatchFlushThreshold`); with the latter two, reads through `Database` see committed data only.

<!-- snippet:optimistic-transactions -->
```csharp
using OptimisticTransactionDatabase txnDb = OptimisticTransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true });
RocksDatabase db = txnDb.Database;                 // plain reads/writes outside transactions
db.Put("balance"u8, "100"u8);

using Transaction txn = txnDb.BeginTransaction(new WriteOptions(), new OptimisticTransactionOptions { SetSnapshot = true });
byte[]? balance = txn.GetForUpdate("balance"u8);   // tracked: commit fails if someone else wrote it since
txn.Put("balance"u8, "90"u8);
db.Put("balance"u8, "50"u8);                       // concurrent write outside the transaction

try
{
    await txn.CommitAsync();
}
catch (RocksDbException e) when (e.Code == StatusCode.Busy)
{
    Console.WriteLine("conflict, retry the transaction");   // optimistic validation failed
}
```

<!-- snippet:pessimistic-transactions -->
```csharp
using TransactionDatabase txnDb = TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true },
    new TransactionDbOptions { TransactionLockTimeout = TimeSpan.FromMilliseconds(100) });

using (Transaction t1 = txnDb.BeginTransaction(new WriteOptions()))
using (Transaction t2 = txnDb.BeginTransaction(new WriteOptions(), new TransactionOptions { DeadlockDetect = true }))
{
    t1.Put("k"u8, "from t1"u8);                     // takes an exclusive lock on "k"
    try
    {
        t2.Put("k"u8, "from t2"u8);                 // waits for the lock, then times out
    }
    catch (RocksDbException e) when (e.Code == StatusCode.TimedOut && e.SubCode == StatusSubCode.LockTimeout)
    {
        Console.WriteLine("lock timeout");
    }
    t1.Commit();
    t2.Rollback();
}
```

With a `RangeLockManager` in `TransactionDbOptions.RangeLockManager`, every lock is a range lock (port of the
range-tree lock manager): `GetRangeLock` locks a key range or prefix exclusively, point locks are single-key ranges,
deadlocks are always detected, and lock escalation merges a transaction's adjacent locks when the lock memory limit
is reached.

<!-- snippet:range-locks -->
```csharp
var rangeLocks = new RangeLockManager();             // keep it to inspect and tune the locks
using TransactionDatabase txnDb = TransactionDatabase.Open(path, new DbOptions { CreateIfMissing = true },
    new TransactionDbOptions { RangeLockManager = rangeLocks, TransactionLockTimeout = TimeSpan.FromMilliseconds(100) });

using Transaction t1 = txnDb.BeginTransaction(new WriteOptions());
using Transaction t2 = txnDb.BeginTransaction(new WriteOptions());
// Lock every key starting with "user:1" (a prefix range: the infinite suffix covers all extensions).
t1.GetRangeLock(null, new RangeLockEndpoint("user:1"u8.ToArray()), new RangeLockEndpoint("user:1"u8.ToArray(), infSuffix: true));
t1.Put("user:1:name"u8, "Ada"u8);                    // inside t1's range: no extra lock wait
try
{
    t2.Put("user:1:email"u8, "x"u8);                 // point locks are single-key ranges: conflicts
}
catch (RocksDbException e) when (e.Code == StatusCode.TimedOut)
{
    Console.WriteLine("locked by t1's range");
}
foreach (RangeLockInfo l in rangeLocks.GetRangeLockStatusData())
    Console.WriteLine($"{l.Start} .. {l.End} held by {string.Join(",", l.TransactionIds)}");
t1.Commit();                                          // releases the range
t2.Put("user:1:email"u8, "ada@example.com"u8);
t2.Commit();
```

Transactions read and write wide-column entities too (`PutEntity`, `TryGetEntity`, `TryGetEntityForUpdate`,
`MultiGetEntity`, `GetCoalescingIterator`, `GetAttributeGroupIterator`; `WriteBatchWithIndex` has
`TryGetEntityFromBatchAndDB` / `MultiGetEntityFromBatchAndDB`):

<!-- snippet:transaction-entities -->
```csharp
using Transaction txn = txnDb.BeginTransaction(new WriteOptions());
var columns = new WideColumns();
txn.TryGetEntityForUpdate(null, "user:2"u8, columns);   // locks the key, reads its columns
txn.PutEntity("user:2"u8, [new WideColumn("name"u8.ToArray(), "Grace"u8.ToArray()),
                           new WideColumn("lang"u8.ToArray(), "COBOL"u8.ToArray())]);
txn.TryGetEntity(null, "user:2"u8, columns);            // sees its own uncommitted entity: lang, name
txn.Commit();
```

<!-- snippet:two-phase-commit -->
```csharp
using (TransactionDatabase txnDb = TransactionDatabase.Open(path, dbOptions))
{
    Transaction txn = txnDb.BeginTransaction(new WriteOptions { Sync = true });
    txn.SetName("xid-42");                          // 2PC transactions are named
    txn.Put("order:42"u8, "paid"u8);
    txn.Prepare();                                  // prepare record is in the WAL (native-identical format)
    // ... process stops before Commit ...
}

// On reopen, prepared transactions are recovered (also from WALs written by native RocksDB).
using (TransactionDatabase txnDb = TransactionDatabase.Open(path, dbOptions))
{
    foreach (Transaction prepared in txnDb.GetAllPreparedTransactions())
    {
        Console.WriteLine($"{prepared.Name}: {prepared.State}");   // xid-42: Prepared
        prepared.Commit();                                          // or Rollback()
        prepared.Dispose();
    }
    Console.WriteLine(txnDb.Database.Get("order:42"u8) is not null);   // True
}
```

Transactions also support save points (`SetSavePoint`, `RollbackToSavePoint`), `GetForUpdate` with shared locks,
untracked writes, `TransactionIterator` (reads the transaction's writes merged with the DB) and async versions of
`Commit`, `Prepare`, `Rollback`, `GetForUpdate` and the write methods.

### WriteBatchWithIndex

<!-- snippet:write-batch-with-index -->
```csharp
using var wbwi = new WriteBatchWithIndex();
wbwi.Put("b"u8, "batch"u8).Delete("c"u8);

// Read your own uncommitted writes, falling back to the DB.
byte[]? b = wbwi.GetFromBatchAndDB(db, "b"u8);              // "batch"
byte[]? c = wbwi.GetFromBatchAndDB(db, "c"u8);              // null: deleted in the batch

// Iterate the DB merged with the batch (the merged iterator owns and disposes the base iterator).
using (BaseDeltaIterator it = wbwi.NewIteratorWithBase(db.NewIterator()))
    for (it.SeekToFirst(); it.Valid; it.Next())
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(it.Key));   // a, b

db.Write(wbwi);                                              // apply atomically
```

### Wide columns

<!-- snippet:wide-columns -->
```csharp
db.PutEntity("user:1"u8,
[
    WideColumn.Default("alice"u8.ToArray()),                       // the default column = plain Get value
    new WideColumn("email"u8.ToArray(), "alice@example.com"u8.ToArray()),
    new WideColumn("city"u8.ToArray(), "Lisbon"u8.ToArray()),
]);

WideColumns? columns = db.GetEntity("user:1"u8);
if (columns is not null && columns.TryGetValue("city"u8, out ReadOnlySpan<byte> city))
    Console.WriteLine(System.Text.Encoding.UTF8.GetString(city));    // Lisbon
Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get("user:1"u8)!));   // alice (default column)

using Iterator it = db.NewIterator();
for (it.SeekToFirst(); it.Valid; it.Next())
    if (it.IsEntity) Console.WriteLine(it.Columns.Count);           // 3
```

### Attribute groups, MultiGetEntity and lazy entity reads

<!-- snippet:attribute-groups -->
```csharp
// One logical row split across column families, written atomically.
db.PutAttributeGroups("user:1"u8,
[
    new AttributeGroup(profile, new[] { new WideColumn("name"u8.ToArray(), "alice"u8.ToArray()) }),
    new AttributeGroup(media, new[] { new WideColumn("avatar"u8.ToArray(), new byte[4096]) }),
]);

// Read the groups back (one consistent view); the result objects are reusable.
PinnableAttributeGroup[] groups = [new(profile), new(media)];
db.GetAttributeGroups(null, "user:1"u8, groups);
Console.WriteLine(groups[1].Columns.Count);                        // 1

// Batched entity reads on one column family.
using var batch = new MultiGetBatch();
batch.Add("user:1"u8);
var columns = new WideColumns();
profile.MultiGetEntity(batch);
if (batch.TryGetEntity(0, columns)) Console.WriteLine(columns.Count);   // 1

// Iterate rows across column families: columns of later families shadow earlier ones.
using (CoalescingIterator rows = db.NewCoalescingIterator(null, [profile, media]))
    for (rows.SeekToFirst(); rows.Valid; rows.Next())
        Console.WriteLine(rows.Columns.Count);                     // 2

// Lazy read: blob-backed columns are fetched only when pulled, in whole or by byte range.
media.Flush();
using LazyWideColumns? lazy = media.GetEntityLazy(null, "user:1"u8);
Span<byte> header = stackalloc byte[16];
int read = lazy!.ReadColumnRange(lazy.IndexOf("avatar"u8), 0, header);   // reads 16 bytes, not 4 KiB
```

### User-defined timestamps

<!-- snippet:user-defined-timestamps -->
```csharp
static byte[] Ts(ulong t)                                            // 8-byte little-endian timestamps
{
    byte[] b = new byte[8];
    BinaryPrimitives.WriteUInt64LittleEndian(b, t);
    return b;
}

var cfOptions = new ColumnFamilyOptions { Comparator = Comparator.BytewiseWithU64Ts };
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);

db.Put("k"u8, Ts(10), "v10"u8);
db.Put("k"u8, Ts(20), "v20"u8);
db.Delete("k"u8, Ts(30));

// Reads name a timestamp and see the newest version at or below it.
Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get(new ReadOptions { Timestamp = Ts(15) }, "k"u8)!));   // v10
Console.WriteLine(db.Get(new ReadOptions { Timestamp = Ts(35) }, "k"u8) is null);                                  // True

// TryGet can also return the timestamp of the version found.
var value = new ArrayBufferWriter<byte>();
var timestamp = new ArrayBufferWriter<byte>();
db.TryGet(new ReadOptions { Timestamp = Ts(25) }, "k"u8, value, timestamp);   // v20 @ 20

// History below full_history_ts_low may be garbage collected by compaction.
db.IncreaseFullHistoryTsLow(Ts(20));
```

### Blob files (integrated BlobDB)

<!-- snippet:blob-files -->
```csharp
var cfOptions = new ColumnFamilyOptions
{
    EnableBlobFiles = true,                 // integrated BlobDB: large values go to .blob files at flush
    MinBlobSize = 512,
    BlobCompressionType = RocksDb.Managed.Compression.CompressionType.LZ4,
    EnableBlobGarbageCollection = true,
};
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
db.Put("big"u8, new byte[4096]);
db.Put("small"u8, "inline"u8);
db.Flush();
Console.WriteLine(db.GetProperty(DbProperties.NumBlobFiles));        // 1
Console.WriteLine(db.Get("big"u8)!.Length);                          // 4096, read through the blob file
```

### TTL

<!-- snippet:ttl -->
```csharp
// Values carry a 4-byte write time (native DBWithTTL format); compaction drops expired entries.
using TtlDatabase db = TtlDatabase.Open(path, new DbOptions { CreateIfMissing = true }, ttlSeconds: 3600);
db.Put("session"u8, "token"u8);
Console.WriteLine(System.Text.Encoding.UTF8.GetString(db.Get("session"u8)!));   // token (timestamp stripped)
using (Iterator it = db.NewIterator())                                           // iterators strip it too
    for (it.SeekToFirst(); it.Valid; it.Next()) Console.WriteLine(it.Value.Length); // 5
db.CompactRange();                                                               // expired entries are removed here
```

### Checkpoints, backups and restore

<!-- snippet:checkpoint-backup -->
```csharp
using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }))
{
    db.Put("k"u8, "v"u8);

    // Checkpoint: an openable copy of the DB (SST files hard-linked when on the same file system).
    await db.CreateCheckpointAsync(checkpointDir);

    // Backups: incremental, shared SST files, native-identical layout (native BackupEngine can read them).
    using BackupEngine backups = BackupEngine.Open(new BackupEngineOptions(backupDir) { MaxBackgroundOperations = 4 });
    uint id = backups.CreateNewBackup(db, flushBeforeBackup: true);
    backups.VerifyBackup(id, verifyWithChecksum: true);
    foreach (BackupInfo info in backups.GetBackupInfo())
        Console.WriteLine($"backup {info.BackupId}: {info.NumberFiles} files, {info.Size} bytes");
    backups.PurgeOldBackups(numBackupsToKeep: 5);
}

using (BackupEngineReadOnly backups = BackupEngineReadOnly.Open(new BackupEngineOptions(backupDir)))
    backups.RestoreDbFromLatestBackup(restoreDir, restoreDir, new RestoreOptions());

using RocksDatabase restored = RocksDatabase.Open(restoreDir, new DbOptions());
using RocksDatabase fromCheckpoint = RocksDatabase.Open(checkpointDir, new DbOptions());
```

`BackupEngine` also supports app metadata, progress callbacks, rate limiting (`BackupRateLimiter`), `DeleteBackup`,
`GarbageCollect`, `StopBackup`, parallel copies (`MaxBackgroundOperations`) and excluding shared files known to live in
another backup directory (`CreateBackupOptions.ExcludeFilesCallback`, restored with `RestoreOptions.AlternateDirs`). Every operation that does I/O has an async version.

### SstFileWriter and ingestion

<!-- snippet:sst-ingest -->
```csharp
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

// Build an SST file offline (keys must be added in comparator order)...
using (SstFileWriter writer = db.CreateSstFileWriter(db.DefaultColumnFamily))   // or new SstFileWriter(cfOptions)
{
    writer.Open(sstPath);
    for (int i = 0; i < 100; i++)
        writer.Put(System.Text.Encoding.UTF8.GetBytes($"bulk:{i:D4}"), "v"u8);
    ExternalSstFileInfo info = await writer.FinishAsync();
    Console.WriteLine($"{info.NumEntries} entries");
}

// ...and ingest it atomically (the file is placed at the lowest level it fits; also works with native SSTs).
// Several files may overlap: later files in the list win. IngestBehind = true back-fills below existing data.
db.IngestExternalFiles([sstPath], new IngestExternalFileOptions { MoveFiles = true });

// Replace a whole key range atomically: existing keys in ["bulk:", "bulk;") disappear in the same step as the new
// file appears (EXPERIMENTAL atomic_replace_range; requires SnapshotConsistency = false).
using (SstFileWriter writer = db.CreateSstFileWriter(db.DefaultColumnFamily))
{
    writer.Open(sstPath);
    writer.Put("bulk:0001"u8, "v2"u8);
    await writer.FinishAsync();
}
db.IngestExternalFiles(db.DefaultColumnFamily, new IngestExternalFileArg
{
    ExternalFiles = [sstPath],
    Options = new IngestExternalFileOptions { SnapshotConsistency = false },
    AtomicReplaceRange = new IngestReplaceRange("bulk:"u8.ToArray(), "bulk;"u8.ToArray()),
});
```

### Column family export and import

<!-- snippet:export-import -->
```csharp
ExportImportFilesMetadata exported;
using (RocksDatabase db = RocksDatabase.Open(source, new DbOptions { CreateIfMissing = true }))
{
    ColumnFamily users = db.CreateColumnFamily("users", new ColumnFamilyOptions());
    users.Put("user:1"u8, "alice"u8);
    // Flushes the column family and hard-links (or copies) its table files into a new directory.
    exported = await users.ExportColumnFamilyAsync(exportDir);
}

// Import into another DB (managed or native: the metadata maps to rocksdb_export_import_files_metadata_t).
using RocksDatabase other = RocksDatabase.Open(target, new DbOptions { CreateIfMissing = true });
ColumnFamily imported = other.CreateColumnFamilyWithImport("users", new ColumnFamilyOptions(),
    new ImportColumnFamilyOptions { MoveFiles = true }, exported);      // or a list of non-overlapping exports
Console.WriteLine(Utf8(imported.Get("user:1"u8)));                      // alice
```

### Remote compaction

A `CompactionService` runs compactions outside the DB process. The job description and the result are byte-compatible
with native RocksDB, so a managed primary can use native `DB::OpenAndCompact` workers and vice versa.

<!-- snippet:compaction-service-class -->
```csharp
/// <summary>
/// A compaction service whose "remote" worker runs in-process. A real one ships the input bytes to a worker host that
/// can read the DB directory (shared storage) and returns the bytes OpenAndCompact produced there.
/// </summary>
private sealed class InProcessCompactionService(string dbPath) : CompactionService
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _jobs = new();

    public override string Name => "InProcess";

    public override ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info,
        ReadOnlyMemory<byte> input, CancellationToken cancellationToken)
    {
        string jobId = $"{info.DbSessionId}-{info.JobId}";
        _jobs[jobId] = input.ToArray();          // byte-compatible with native CompactionServiceInput
        return ValueTask.FromResult(new CompactionServiceScheduleResponse(jobId, CompactionServiceJobStatus.Success));
    }

    public override async ValueTask<CompactionServiceWaitResult> WaitAsync(string jobId, CancellationToken cancellationToken)
    {
        _jobs.TryRemove(jobId, out byte[]? input);
        try
        {
            // The worker side (native DB::OpenAndCompact works too); outputs go to <db>/<jobId>.
            byte[] result = await RocksDatabase.OpenAndCompactAsync(new OpenAndCompactOptions(), dbPath, jobId, input!,
                new CompactionServiceOptionsOverride(), cancellationToken);
            return new CompactionServiceWaitResult(CompactionServiceJobStatus.Success, result);
        }
        catch (OpenAndCompactException e)
        {
            return new CompactionServiceWaitResult(CompactionServiceJobStatus.Failure, e.Result);
        }
    }
}
```

<!-- snippet:remote-compaction -->
```csharp
var options = new DbOptions { CreateIfMissing = true, CompactionService = new InProcessCompactionService(path) };
using RocksDatabase db = RocksDatabase.Open(path, options);
for (int f = 0; f < 3; f++)
{
    for (int i = 0; i < 100; i++) db.Put(System.Text.Encoding.UTF8.GetBytes($"key:{i:D3}"), "v"u8);
    db.Flush();
}
await db.CompactRangeAsync();     // compacted by the service; its outputs are moved into the DB and installed
```

### Merge operands and multi-range scans

<!-- snippet:merge-operands-multiscan -->
```csharp
var cfOptions = new ColumnFamilyOptions { MergeOperator = new StringAppendOperator(","u8) };
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }, cfOptions);
db.Put("list"u8, "a"u8);
db.Merge("list"u8, "b"u8);
db.Merge("list"u8, "c"u8);

// The operands without merging them (oldest first, base value included); reusable, pooled result.
using var operands = new MergeOperandsResult();
var limits = new GetMergeOperandsOptions
{
    ExpectedMaxNumberOfOperands = 10,
    ContinueCallback = operand => !operand.SequenceEqual("b"u8),   // newest first: stop at "b"
};
if (db.TryGetMergeOperands("list"u8, limits, operands))
    for (int i = 0; i < operands.Count; i++) Console.WriteLine(Utf8(operands[i]));   // b, c

// Several key ranges with one iterator; the data blocks of all ranges are prefetched up front.
for (int i = 0; i < 100; i++) db.Put(System.Text.Encoding.UTF8.GetBytes($"row:{i:D3}"), "x"u8);
var ranges = new MultiScanArgs().Add("row:010"u8.ToArray(), "row:013"u8.ToArray()).Add("row:090"u8.ToArray(), "row:092"u8.ToArray());
using (MultiScan scan = db.NewMultiScan(ranges))
    while (await scan.MoveNextRangeAsync())
        for (; scan.Valid; await scan.NextAsync())
            Console.WriteLine(Utf8(scan.Key));
```

### File-level operations

<!-- snippet:file-level-operations -->
```csharp
using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true },
    new ColumnFamilyOptions { DisableAutoCompactions = true });
for (int f = 0; f < 3; f++)
{
    for (int i = 0; i < 10; i++) db.Put(System.Text.Encoding.UTF8.GetBytes($"k{f}{i}"), "v"u8);
    db.Flush();                               // three non-overlapping L0 files
}
db.PromoteL0(targetLevel: 1);                 // move them to L1 without rewriting
db.SuggestCompactRange(null, null);           // mark files for compaction; returns at once
db.DeleteFilesInRange("k1"u8.ToArray(), "k2"u8.ToArray(), includeEnd: false);   // drop whole files in a range
db.ClipColumnFamily("k0"u8.ToArray(), "k2"u8.ToArray());                         // keep only [k0, k2)
Dictionary<string, TableProperties> props =
    db.GetPropertiesOfTablesInRange([new KeyRange("k0"u8.ToArray(), "k1"u8.ToArray())]);

// Freeze the DB (writes stop until disposed), e.g. to copy a consistent state.
using (WalLock frozen = await db.LockWalAsync())
    db.CreateCheckpoint(Path.Combine(path, "..", "frozen-checkpoint"));

db.DisableManualCompaction();                 // CompactRange now fails with Incomplete / ManualCompactionPaused
db.EnableManualCompaction();
ulong oldest = db.GetCreationTimeOfOldestFile();   // unix seconds (max_open_files = -1)
```

### WAL tailing: GetUpdatesSince and TailUpdatesAsync

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

Set `DbOptions.WalTtlSeconds` / `WalSizeLimitMb` to archive obsolete WALs for readers that fall behind, and
`DisableFileDeletions` / `EnableFileDeletions` to pin files. `WaitForSequenceAsync`, `GetSortedWalFiles`,
`GetLiveFiles` and `CreateCheckpointAsync` cover the rest of what a replication layer needs; see
[src/RocksDb.Managed.Replication](src/RocksDb.Managed.Replication/README.md).

### Read-only and secondary instances

<!-- snippet:read-only-secondary -->
```csharp
using RocksDatabase primary = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
primary.Put("k"u8, "v1"u8);

// Read-only: no LOCK, nothing written; works next to a running primary (sees the state at open).
using (RocksDatabase readOnly = RocksDatabase.OpenReadOnly(path, new DbOptions()))
    Console.WriteLine(System.Text.Encoding.UTF8.GetString(readOnly.Get("k"u8)!));   // v1

// Secondary: follows the primary (managed or native) on demand.
using RocksDatabase secondary = await RocksDatabase.OpenAsSecondaryAsync(path, secondaryPath, new DbOptions());
primary.Put("k"u8, "v2"u8);
await secondary.TryCatchUpWithPrimaryAsync();
Console.WriteLine(System.Text.Encoding.UTF8.GetString(secondary.Get("k"u8)!));      // v2
```

### Statistics, event listeners and the info LOG

<!-- snippet:listener-class -->
```csharp
/// <summary>Listener callbacks run on background threads: keep them short and thread-safe.</summary>
private sealed class PrintingListener : EventListener
{
    public int Flushes;

    public override void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info)
    {
        Interlocked.Increment(ref Flushes);
        Console.WriteLine($"flushed {info.ColumnFamilyName} -> {Path.GetFileName(info.FilePath)} ({info.FlushReason})");
    }

    public override void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info) =>
        Console.WriteLine($"compacted L{info.BaseInputLevel} -> L{info.OutputLevel}: {info.InputFiles.Count} -> {info.OutputFiles.Count} files");

    public override void OnBackgroundError(BackgroundErrorEventArgs e) =>
        Console.WriteLine($"background error ({e.Reason}): {e.Error?.Message}");
}

/// <summary>Routes the DB's info log to the console instead of the LOG file.</summary>
private sealed class ConsoleInfoLogger() : InfoLogger(InfoLogLevel.Warn)
{
    protected override void LogCore(InfoLogLevel level, string message) => Console.WriteLine($"[{level}] {message}");
}
```

<!-- snippet:monitoring -->
```csharp
var statistics = new Statistics();                  // all upstream tickers and histograms
var listener = new PrintingListener();
var options = new DbOptions { CreateIfMissing = true, Statistics = statistics };
options.Listeners.Add(listener);
// InfoLog = null (default) writes a native-style LOG file in the DB directory (rolled per MaxLogFileSize /
// LogFileTimeToRoll, KeepLogFileNum old files kept); set InfoLog = new ConsoleInfoLogger() to redirect it.

using RocksDatabase db = RocksDatabase.Open(path, options);
for (int i = 0; i < 100; i++) db.Put(BitConverter.GetBytes(i), "value"u8);
db.Get(BitConverter.GetBytes(7));
db.Flush();

Console.WriteLine(statistics.GetTickerCount(Ticker.NumberKeysWritten));          // 100
HistogramData gets = statistics.GetHistogramData(Histogram.DbGet);                 // micros per Get
Console.WriteLine($"get p99 {gets.Percentile99} us over {gets.Count} calls");
string dump = statistics.ToString();                 // same text as native Statistics::ToString()

Console.WriteLine(db.GetIntProperty(DbProperties.EstimateNumKeys));               // 100
Dictionary<string, TableProperties> tables = db.GetPropertiesOfAllTables();
```

`GetProperty` accepts the native property names (constants in `DbProperties`), including `rocksdb.stats`,
`rocksdb.cfstats`, `rocksdb.levelstats`, `rocksdb.num-files-at-level<N>`, `rocksdb.estimate-num-keys` and the blob and
block-cache properties. `KeyMayExist`, `GetApproximateSizes`, `GetApproximateMemTableStats`, `GetLiveFilesMetadata`
and `GetPropertiesOfAllTables` are also available.

### Cache tiers, simulated caches and memtable budgets

`BlockCache.CreateTiered` combines the primary block cache with a compressed secondary tier under one budget
(`TieredCacheOptions`, `UpdateTieredCache`); `LruCacheOptions.SecondaryCache` / `HyperClockCacheOptions.SecondaryCache`
attach any `SecondaryCache` (e.g. `CompressedSecondaryCache`). A `WriteBufferManager` caps the memtable memory of any
number of DBs and column families, can stall writes at the limit and charge memtables to a block cache;
`DbOptions.DbWriteBufferSize` is the per-DB shortcut. `SimCache` reports the hit rate of a hypothetical capacity, and
`BlockBasedTableOptions.PersistentCache` (`VolatileCacheTier`, file-based `PersistentCache.Create`) is the legacy
persistent block cache.

<!-- snippet:cache-tiers -->
```csharp
// One memory budget for blocks and memtables: a 64 MiB tiered cache (primary HyperClock + compressed secondary tier,
// 25% of the budget) and a write buffer manager that charges memtables to it and caps them at 16 MiB.
BlockCache cache = BlockCache.CreateTiered(new TieredCacheOptions
{
    TotalCapacity = 64L << 20,
    CompressedSecondaryRatio = 0.25,
    CacheType = PrimaryCacheType.HyperClock,
    HyperClockCacheOptions = new HyperClockCacheOptions { EstimatedEntryCharge = BlockCache.EstimateBlockCharge(4096) },
})!;
var writeBufferManager = new WriteBufferManager(16L << 20, cache, allowStall: true);
var stats = new Statistics();
var dbOptions = new DbOptions { CreateIfMissing = true, WriteBufferManager = writeBufferManager, Statistics = stats };
var cfOptions = new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { BlockCache = cache } };

using (RocksDatabase db = RocksDatabase.Open(path, dbOptions, cfOptions))
{
    for (int i = 0; i < 10_000; i++) db.Put(System.Text.Encoding.UTF8.GetBytes("key" + i), new byte[200]);
    db.Flush();
    _ = db.Get("key42"u8);
    Console.WriteLine($"memtables: {writeBufferManager.MemoryUsage} bytes, charged to the cache: {writeBufferManager.DummyEntriesInCacheUsage}");
    Console.WriteLine($"secondary cache hits: {stats.GetTickerCount(Ticker.SecondaryCacheHits)}");
}
// Resize the whole budget, or the compressed share, at runtime.
BlockCache.UpdateTieredCache(cache, 128L << 20, 0.3, TieredAdmissionPolicy.Max);
writeBufferManager.SetBufferSize(32L << 20);

// What hit rate would a 1 GiB cache give? A SimCache tracks keys only, at no memory cost for values.
SimCache sim = BlockCache.CreateSimCache(BlockCache.CreateLru(8L << 20), 1L << 30, numShardBits: -1);
using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions(),
           new ColumnFamilyOptions { TableOptions = new BlockBasedTableOptions { BlockCache = sim } }))
{
    for (int i = 0; i < 10_000; i += 7) _ = db.Get(System.Text.Encoding.UTF8.GetBytes("key" + i));
}
Console.Write(sim.ToString());   // SimCache MISSes / HITs / HITRATE
```

### Tracing and replay

<!-- snippet:tracing -->
```csharp
using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true }))
{
    // Query trace (native trace file format: native trace_analyzer and replayers read it).
    db.StartTrace(new TraceOptions { SamplingFrequency = 1, Filter = TraceFilter.None }, trace);
    db.Put("k1"u8, "v1"u8);
    _ = db.Get("k1"u8);
    using (Iterator it = db.NewIterator()) it.Seek("k"u8);
    db.EndTrace();

    // Block cache trace: every block cache lookup of the table readers.
    db.Flush();
    db.StartBlockCacheTrace(new BlockCacheTraceOptions(), blockTrace);
    _ = db.Get("k1"u8);
    db.EndBlockCacheTrace();
}

using (var reader = new BlockCacheTraceReader(TraceReader.OpenFile(blockTrace)))
{
    reader.ReadHeader();
    while (reader.TryReadAccess(out BlockCacheTraceRecord? access))
        Line($"{access!.Caller} {access.BlockType} hit={access.IsCacheHit}");
}

// Replay the query trace against another DB (here: twice as fast, 2 threads).
using RocksDatabase target = RocksDatabase.Open(replayPath, new DbOptions { CreateIfMissing = true });
using Replayer replayer = target.NewDefaultReplayer(TraceReader.OpenFile(trace));
replayer.Prepare();
int executed = 0;
replayer.Replay(new ReplayOptions { NumThreads = 2, FastForward = 2.0 }, (error, result) => Interlocked.Increment(ref executed));
```

Query traces (`StartTrace` / `EndTrace`: writes, `Get`, `MultiGet`, iterator `Seek` / `SeekForPrev`, with
`TraceOptions.MaxTraceFileSize`, `SamplingFrequency`, `Filter`, `PreserveWriteOrder` and `IncludeReadWriteOptions`),
block cache traces (`StartBlockCacheTrace`) and IO traces (`StartIOTrace`) use the native file formats: native
`trace_analyzer`, `block_cache_trace_analyzer` and `io_tracer_parser` read managed traces, and the managed
`Replayer`, `BlockCacheTraceReader` and `IoTraceReader` read native ones. `Replayer.TryNext` / `Execute` step through a
trace one record at a time. `RocksDatabase.Repair(path, options)` (RepairDB) rebuilds a lost or corrupt MANIFEST from
the table files and WALs.

### Tools

`tools/RocksDb.Managed.Tools` is a console program with `ldb` and `sst_dump` equivalents. They work on managed and
native DB directories and files, take the native flags, and print the native output formats, so existing scripts keep
working:

```bash
dotnet run --project tools/RocksDb.Managed.Tools -f net10.0 -- ldb --db=/data/db scan --from=a --to=m
dotnet run --project tools/RocksDb.Managed.Tools -f net10.0 -- ldb --db=/data/db --hex get 0x6B31
dotnet run --project tools/RocksDb.Managed.Tools -f net10.0 -- ldb --db=/data/db manifest_dump --verbose
dotnet run --project tools/RocksDb.Managed.Tools -f net10.0 -- ldb dump_wal --walfile=/data/db --header --print_value
dotnet run --project tools/RocksDb.Managed.Tools -f net10.0 -- sst_dump --file=/data/db/000012.sst --command=scan --show_properties
```

`ldb` commands: `get`, `multi_get`, `put`, `batchput`, `delete`, `singledelete`, `deleterange`, `scan`, `dump`
(also `--path=<wal|sst|MANIFEST>`), `idump`, `manifest_dump`, `dump_wal`, `list_column_families`,
`create_column_family`, `drop_column_family`, `compact`, `checkconsistency`, `repair`, `approxsize`, `reduce_levels`,
`file_checksum_dump`, `list_live_files_metadata`, `get_property` and `checkpoint`, with `--hex` / `--key_hex` /
`--value_hex` and `--column_family`. `sst_dump` commands: `check` (default), `scan`, `raw`, `verify`, `identify` and
`recompress` (prints the input file only), with `--show_properties`, `--show_summary`, `--list_meta_blocks`,
`--from` / `--to` / `--prefix`, `--input_key_hex`, `--read_num` and `--output_hex`.

## Error handling

Native RocksDB returns a `Status`. The port throws `RocksDbException`, whose `Code` and `SubCode` have the same values
as `rocksdb::Status::Code` / `SubCode`. Background errors also carry `Severity`, `Retryable`, `DataLoss` and `Scope`.
Expected misses are not exceptions: `TryGet` returns false, `Get` returns null and `MultiGetBatch` reports a status
per key. Cancelling a `CancellationToken` throws `OperationCanceledException`.

<!-- snippet:error-handling -->
```csharp
// Failures are RocksDbException; Code/SubCode mirror rocksdb::Status codes.
string empty = Directory.CreateDirectory(Path.Combine(path, "empty")).FullName;
try
{
    using RocksDatabase missing = RocksDatabase.Open(empty, new DbOptions());   // CreateIfMissing = false
}
catch (RocksDbException e) when (e.Code == StatusCode.InvalidArgument)
{
    openCode = e.Code;
    Console.WriteLine(e.Message);                    // "Invalid argument: ... does not exist (create_if_missing is false)"
}

using (RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true })) db.Put("k"u8, "v"u8);
using (RocksDatabase readOnly = RocksDatabase.OpenReadOnly(path, new DbOptions()))
{
    try { readOnly.Put("k"u8, "v2"u8); }
    catch (RocksDbException e) when (e.Code == StatusCode.NotSupported) { readOnlyCode = e.Code; }
}

// Background errors (failed flush/compaction/WAL write) stop writes like in native RocksDB. Retryable I/O errors
// are resumed automatically (DbOptions.MaxBgErrorResumeCount); otherwise fix the cause and call Resume.
await using RocksDatabase live = await RocksDatabase.OpenAsync(path, new DbOptions());
if (live.BackgroundError is RocksDbException bg)
{
    Console.WriteLine($"{bg.Code} severity={bg.Severity} retryable={bg.Retryable}");
    await live.ResumeAsync();                         // throws if the error is still there
}
```

Common codes: `InvalidArgument` (bad options, a missing DB without `CreateIfMissing`, a comparator mismatch),
`NotSupported` (writes on a read-only instance, a feature that is not ported), `Corruption`, `IOError` (`SubCode`
`NoSpace`, `PathNotFound`, ...), `Busy` (optimistic transaction conflict), `TimedOut` with `LockTimeout` (lock wait),
`ShutdownInProgress` and `ObjectDisposedException` (the DB is closing), and `Incomplete` (`ReadTier.MemtableTier`
reads, `MaxSkippableInternalKeys`).

## Known deviations and gaps

The port aims for upstream behavior. The differences below are intentional or not yet closed;
[docs/validation.md](docs/validation.md) lists every file that was audited against the C++ sources, and
[todo.md](todo.md) tracks open work.

Format-level deviations (the output is always readable by native RocksDB):

- Compression: LZ4 acceleration > 1 runs with acceleration 1; LZ4HC with a dictionary compresses with fast LZ4; zlib
  deflates inputs above 16 KiB in independent 16 KiB chunks (so native's 16 KiB inflate window accepts them; not
  byte-identical) and writes without a dictionary; the ZSTD frame checksum option is ignored;
  BZip2 and Xpress blocks are stored uncompressed (the codecs cannot be read either).
- Ribbon filters with `optimize_filters_for_memory` can differ from native by up to 16 bytes.
  Compaction drops them later, so the data is the same.

Not ported yet (tracked in [todo.md](todo.md)):

- WAL recycling (`recycle_log_file_num`): recyclable WALs written by native are read, but the port never recycles its own.
- Out of scope (by decision): the encrypted file system (`env_encryption.h`); remote compaction of compactions with
  blob files and per-subcompaction remote jobs (such compactions run locally); `ldb` commands for TTL, timestamp, blob
  and transaction databases and `sst_dump` on PlainTable / CuckooTable files.
- Not supported, as upstream: user-defined timestamps in optimistic transactions and explicit-timestamp
  `WriteBatchWithIndex` writes.

API and behavior differences:

- Errors are exceptions (see above). Cancelling a write that is delayed by a write stall fails its whole write group
  with `OperationCanceledException`, because upstream has no cancellation.
- No optional or defaulted parameters except `CancellationToken`. Options are explicit overloads, and the column family
  is the receiver.

Performance: on the three-way db_bench-style benchmarks (warm, medians of 5 repetitions, the same librocksdb 11.12
for the native legs), the managed engine's geometric mean per configuration is 0.67x-0.81x the time of the RocksDB C
API. Over all 8 configurations it is 0.72x (95% CI 0.71-0.74), and 0.73x with the async API, while rocksdb-sharp is
1.05x. Writes, flushes, compactions, reads, seeks and scans are faster than C on average, including random writes on
4 threads (0.76-0.98x) and an explicit `Flush` (0.63-0.93x). Of the 232 managed cells, only 2 are above 1.1x of C,
both within run-to-run noise: synced puts (`fillsync`, one fsync per write) and a sub-microsecond async bloom-filter
miss. See [docs/benchmarks.md](docs/benchmarks.md).

## Documentation

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | module map (C++ to C#), threading and async model, memory and pooling, design deviations from C++ |
| [docs/testing.md](docs/testing.md) | the test suites (unit, native compat, crash-isolated round trips, replication, crash consistency) and how to run and extend them |
| [docs/benchmarks.md](docs/benchmarks.md) | three-way benchmark methodology, `run-all.sh`, latest results, micro-benchmarks |
| [bench/README.md](bench/README.md) | reference for the benchmark runner's flags and workloads |
| [docs/perf.md](docs/perf.md) | performance work log with before/after measurements |
| [docs/validation.md](docs/validation.md) | file-by-file audit of the port against the C++ sources |
| [src/RocksDb.Managed.Replication/README.md](src/RocksDb.Managed.Replication/README.md) | replication, switchover and Raft cluster layer |
| [CLAUDE.md](CLAUDE.md) | contributor guide: goals, layout, porting conventions, API design rules |
