# Replica apply with parallel memtable inserts (2026-11-03)

`bench/RocksDb.Managed.Replication.Benchmarks` on the 4-vCPU VM ("Intel Xeon Processor @ 2.10GHz"), tree `0b8509b`,
Release, .NET 10.0.14. **Another agent was building and benchmarking at the same time (load average 4.4-6.2)**, so the
absolute rates are below the quiet run of `2026-11-03-replication.md`; the comparison is interleaved (rounds alternate
`--apply_parallelism 1`, the serial insert of the previous tree and of rocksdb-sharp's consumer, and `4`, the new
default on this machine).

## Where the replica's time went (before)

`stream --batch 100 --threads 4`, in memory, with a timer around `TryIngestChunkAsync`: the apply loop was busy 95-98%
of the run, so the replica was CPU-bound in the apply, not waiting on the stream. Applying 1,000,000 entries of
recorded chunks (418 chunks of ~2,400 entries, 4 writers x 100-entry batches merged by the tailer) to a fresh replica
with nothing else running, profiled with `perf` (`DOTNET_PerfMapEnabled=1`):

```
56.0%  InlineSkipList<MemTableKeyComparer>.Insert          memtable insert (random 16-byte keys, cache-miss bound)
 3.7%  SpanHelpers.Memmove                                 SetContents copy + WAL buffer
 3.5%  FlushJob.AddPointEntries, 2.2% Lz4 CompressCore     background flush
 1.1%  WriteBatch.IterateRecords, 0.5% TryReadRecord       batch decode
 0.3%  LogWriter.AddRecordAsync                            WAL write
```

Allocation per applied chunk: ~400 B, the same with 1 and 4 apply threads (background flush; `GC.GetTotalAllocatedBytes`,
no gen0 collection during 1M entries). The primary's writers insert their batches in parallel (write groups with
`allow_concurrent_memtable_write`); the replica inserted each merged chunk with one thread.

Isolated apply rate (same recorded chunks, fresh replica per run, entries/s, load ~2):

| apply threads | run 1 | run 2 | run 3 |
|---|---|---|---|
| 1 | 473,054 | 493,151 | 456,096 |
| 2 | 773,652 | 701,389 | 760,397 |
| 4 | 1,023,472 | 1,136,568 | 946,824 |

## End to end, interleaved

```bash
for r in 1 2 3; do for p in 1 4; do
  dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- stream --transport both \
      --num 1000000 --batch 100 --threads 4 --apply_parallelism $p
done; done
```

```
| transport | threads | batch | value | rate (ops/s) | apply threads | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms | chunks | apply busy % | JIT |
== round 1 p=1 09:26:47 load 5.47 3.98 3.23
| inmem | 4 | 100 | 100 | max | 1 | 1000000 | 390,230 | 375,153 | 54.48 | 109.82 | 125.39 | 135.5 | 103.0 | 980 | 97 | 793 |
| grpc | 4 | 100 | 100 | max | 1 | 1000000 | 368,200 | 326,369 | 198.80 | 384.69 | 416.93 | 421.5 | 348.1 | 447 | 95 | 885 |
== round 1 p=4 09:27:03 load 5.51 4.07 3.27
| inmem | 4 | 100 | 100 | max | 4 | 1000000 | 397,937 | 388,788 | 7.09 | 16.94 | 62.75 | 64.3 | 59.1 | 2296 | 93 | 846 |
| grpc | 4 | 100 | 100 | max | 4 | 1000000 | 387,346 | 383,926 | 6.88 | 26.83 | 57.86 | 61.0 | 23.0 | 2617 | 86 | 1299 |
== round 2 p=1 09:27:17 load 5.11 4.05 3.28
| inmem | 4 | 100 | 100 | max | 1 | 1000000 | 476,993 | 422,902 | 107.56 | 201.20 | 269.87 | 270.7 | 268.1 | 978 | 98 | 757 |
| grpc | 4 | 100 | 100 | max | 1 | 1000000 | 470,600 | 395,475 | 207.16 | 356.20 | 409.68 | 411.9 | 403.7 | 420 | 96 | 774 |
== round 2 p=4 09:27:31 load 5.85 4.24 3.36
| inmem | 4 | 100 | 100 | max | 4 | 1000000 | 393,677 | 386,409 | 5.40 | 19.76 | 48.39 | 49.6 | 47.8 | 2329 | 93 | 779 |
| grpc | 4 | 100 | 100 | max | 4 | 1000000 | 461,090 | 452,221 | 8.02 | 41.32 | 63.78 | 66.7 | 42.5 | 2469 | 90 | 1236 |
== round 3 p=1 09:27:45 load 6.22 4.37 3.41
| inmem | 4 | 100 | 100 | max | 1 | 1000000 | 439,811 | 419,525 | 39.13 | 83.92 | 109.27 | 111.7 | 109.9 | 1161 | 98 | 1025 |
| grpc | 4 | 100 | 100 | max | 1 | 1000000 | 528,675 | 407,046 | 268.02 | 452.70 | 565.90 | 567.8 | 565.2 | 440 | 96 | 778 |
== round 3 p=4 09:27:59 load 5.58 4.32 3.41
| inmem | 4 | 100 | 100 | max | 4 | 1000000 | 427,101 | 418,165 | 8.19 | 16.79 | 53.90 | 55.1 | 50.0 | 2540 | 96 | 775 |
| grpc | 4 | 100 | 100 | max | 4 | 1000000 | 366,748 | 357,844 | 8.00 | 31.23 | 70.95 | 73.7 | 67.8 | 2094 | 89 | 1250 |
```

## `all` with the new default (apply threads 4)

```
start 09:28:27 load 4.59 4.21 3.40
| transport | threads | batch | value | rate (ops/s) | apply threads | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms | chunks | apply busy % | JIT |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| inmem | 1 | 1 | 100 | max | 4 | 200000 | 258,785 | 257,476 | 3.24 | 8.10 | 17.46 | 22.3 | 3.9 | 7738 | 63 | 174 |
| inmem | 4 | 1 | 100 | max | 4 | 400000 | 206,892 | 206,883 | 0.84 | 7.57 | 46.51 | 52.5 | 0.1 | 50687 | 73 | 605 |
| inmem | 4 | 100 | 100 | max | 4 | 1000000 | 351,044 | 342,855 | 7.28 | 26.46 | 71.05 | 79.9 | 68.0 | 2179 | 93 | 366 |
| inmem | 4 | 1 | 100 | 20000 | 4 | 100000 | 20,004 | 20,004 | 0.06 | 0.39 | 3.99 | 28.4 | 0.0 | 28483 | 10 | 62 |
| grpc | 1 | 1 | 100 | max | 4 | 200000 | 392,172 | 389,993 | 5.15 | 17.00 | 23.17 | 29.6 | 2.8 | 5481 | 64 | 52 |
| grpc | 4 | 1 | 100 | max | 4 | 400000 | 206,716 | 206,472 | 3.34 | 33.45 | 67.56 | 71.6 | 2.3 | 24607 | 69 | 1302 |
| grpc | 4 | 100 | 100 | max | 4 | 1000000 | 387,737 | 381,288 | 10.86 | 48.44 | 85.51 | 94.0 | 43.6 | 2047 | 88 | 305 |
| grpc | 4 | 1 | 100 | 20000 | 4 | 100000 | 20,006 | 19,991 | 0.27 | 1.39 | 4.44 | 6.1 | 3.7 | 24544 | 9 | 607 |

| transport | writers | handoffs | first handoff: held / client gap ms | held p50 ms | p90 | max | client gap p50 ms | max |
|---|---|---|---|---|---|---|---|---|
| inmem | 4 | 10 (10 completed) | 14.6 / 11.9 | 4.7 | 9.4 | 9.4 | 4.9 | 9.4 |
| grpc | 4 | 10 (10 completed) | 11.0 / 10.9 | 5.8 | 9.8 | 9.8 | 7.2 | 10.6 |
end 09:29:12 load 4.41 4.22 3.44
```
