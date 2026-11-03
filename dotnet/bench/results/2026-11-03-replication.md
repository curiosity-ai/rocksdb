# Replication benchmarks on the quiet machine (2026-11-03)

`bench/RocksDb.Managed.Replication.Benchmarks` on the quiet 4-vCPU KVM VM ("Intel Xeon Processor @ 2.10GHz", no other
agents; 1-minute load average 1.1 before the first run - the load rises to 3-5 during the runs because the benchmark
itself keeps all 4 vCPUs busy), tree `cfb30f9`, Release, .NET 10.0.14, after the dbbench and BenchmarkDotNet runs of
`2026-11-03-final-*`. Commands:

```bash
dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- all        # 3 runs
dotnet run -c Release --project bench/RocksDb.Managed.Replication.Benchmarks -f net10.0 -- switchover --transport both --handoffs 20 --writers 4
dotnet run -c Release --project samples/SwitchoverTest -f net10.0                                       # 4 runs, 2 handoffs each
```

Per-handoff lines are omitted. The previous numbers (docs/benchmarks.md "Replication") were measured on the older
2.80 GHz CPU with other agents running (load average 6-25).

## Run 1 (`all`, then `switchover --handoffs 20`)
```
start 08:46:27 load 1.12 1.17 1.12
# replication benchmarks: 4 CPUs, .NET 10.0.14, load average 1.12

| transport | threads | batch | value | rate (ops/s) | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms | chunks | JIT |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| inmem | 1 | 1 | 100 | max | 200000 | 307,256 | 306,043 | 3.00 | 6.28 | 10.59 | 15.2 | 2.6 | 12297 | 27 |
| inmem | 4 | 1 | 100 | max | 400000 | 207,522 | 207,497 | 0.78 | 5.13 | 34.57 | 41.4 | 0.2 | 66856 | 538 |
| inmem | 4 | 100 | 100 | max | 1000000 | 559,077 | 425,155 | 314.50 | 467.55 | 564.41 | 567.8 | 563.4 | 658 | 453 |
| inmem | 4 | 1 | 100 | 20000 | 100000 | 20,008 | 20,008 | 0.05 | 0.24 | 3.84 | 5.5 | 0.0 | 35886 | 49 |
| grpc | 1 | 1 | 100 | max | 200000 | 354,079 | 346,829 | 4.80 | 9.83 | 16.45 | 18.1 | 11.8 | 7211 | 169 |
| grpc | 4 | 1 | 100 | max | 400000 | 194,008 | 193,648 | 2.25 | 6.13 | 21.60 | 31.0 | 3.8 | 61978 | 1077 |
| grpc | 4 | 100 | 100 | max | 1000000 | 502,968 | 379,940 | 336.34 | 524.23 | 643.92 | 646.2 | 643.8 | 414 | 239 |
| grpc | 4 | 1 | 100 | 20000 | 100000 | 20,007 | 20,006 | 0.27 | 0.63 | 4.23 | 5.9 | 0.2 | 25413 | 677 |

| transport | writers | handoffs | first handoff: held / client gap ms | held p50 ms | p90 | max | client gap p50 ms | max |
|---|---|---|---|---|---|---|---|---|
| inmem | 4 | 10 (10 completed) | 13.4 / 9.8 | 6.1 | 8.4 | 8.4 | 5.5 | 8.5 |
| grpc | 4 | 10 (10 completed) | 8.3 / 8.8 | 5.6 | 8.3 | 8.3 | 6.4 | 8.1 |
mid 08:47:11 load 3.14 1.71 1.31
# replication benchmarks: 4 CPUs, .NET 10.0.14, load average 3.14

| transport | writers | handoffs | first handoff: held / client gap ms | held p50 ms | p90 | max | client gap p50 ms | max |
|---|---|---|---|---|---|---|---|---|
| inmem | 4 | 20 (20 completed) | 15.0 / 13.0 | 4.6 | 6.9 | 8.5 | 5.0 | 8.3 |
| grpc | 4 | 20 (20 completed) | 11.3 / 12.7 | 5.3 | 9.7 | 9.7 | 6.1 | 9.6 |
end 08:47:26 load 3.82 1.94 1.39
EXIT
```

## Runs 2-3 (`all`) and the multi-process `samples/SwitchoverTest` (4 runs)

```
== all run 2 08:47:56 load 2.58 1.82 1.37
# replication benchmarks: 4 CPUs, .NET 10.0.14, load average 2.58

| transport | threads | batch | value | rate (ops/s) | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms | chunks | JIT |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| inmem | 1 | 1 | 100 | max | 200000 | 317,728 | 316,177 | 3.50 | 8.15 | 13.15 | 15.1 | 3.1 | 5902 | 237 |
| inmem | 4 | 1 | 100 | max | 400000 | 221,678 | 221,580 | 0.41 | 5.29 | 12.59 | 16.5 | 0.8 | 77927 | 549 |
| inmem | 4 | 100 | 100 | max | 1000000 | 504,730 | 401,603 | 201.36 | 465.85 | 514.05 | 520.1 | 508.8 | 757 | 526 |
| inmem | 4 | 1 | 100 | 20000 | 100000 | 20,005 | 20,005 | 0.05 | 0.21 | 3.81 | 5.4 | 0.0 | 36527 | 80 |
| grpc | 1 | 1 | 100 | max | 200000 | 365,603 | 361,442 | 9.12 | 17.07 | 20.38 | 21.7 | 6.3 | 5967 | 279 |
| grpc | 4 | 1 | 100 | max | 400000 | 175,809 | 175,549 | 2.53 | 7.11 | 15.40 | 40.2 | 3.4 | 74772 | 684 |
| grpc | 4 | 100 | 100 | max | 1000000 | 488,808 | 372,894 | 342.32 | 547.73 | 635.71 | 638.6 | 635.9 | 557 | 252 |
| grpc | 4 | 1 | 100 | 20000 | 100000 | 19,910 | 19,899 | 0.25 | 0.69 | 4.38 | 7.7 | 2.7 | 25808 | 390 |

| transport | writers | handoffs | first handoff: held / client gap ms | held p50 ms | p90 | max | client gap p50 ms | max |
|---|---|---|---|---|---|---|---|---|
| inmem | 4 | 10 (10 completed) | 14.0 / 11.3 | 5.1 | 7.8 | 7.8 | 5.1 | 6.8 |
| grpc | 4 | 10 (10 completed) | 7.2 / 8.6 | 4.8 | 6.5 | 6.5 | 5.5 | 8.0 |
== all run 3 08:48:41 load 3.03 2.03 1.46
# replication benchmarks: 4 CPUs, .NET 10.0.14, load average 3.03

| transport | threads | batch | value | rate (ops/s) | ops | primary ops/s | replica ops/s | lag p50 ms | p90 | p99 | max | catch-up ms | chunks | JIT |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| inmem | 1 | 1 | 100 | max | 200000 | 287,240 | 280,873 | 4.94 | 17.88 | 46.86 | 50.2 | 15.8 | 2499 | 379 |
| inmem | 4 | 1 | 100 | max | 400000 | 192,018 | 191,756 | 0.73 | 4.70 | 22.43 | 30.7 | 2.8 | 70151 | 540 |
| inmem | 4 | 100 | 100 | max | 1000000 | 477,696 | 391,363 | 237.37 | 410.55 | 467.35 | 469.1 | 461.8 | 681 | 530 |
| inmem | 4 | 1 | 100 | 20000 | 100000 | 19,999 | 19,998 | 0.06 | 0.27 | 3.94 | 5.5 | 0.2 | 35603 | 50 |
| grpc | 1 | 1 | 100 | max | 200000 | 318,642 | 315,185 | 4.85 | 8.35 | 14.75 | 17.3 | 6.9 | 10368 | 1027 |
| grpc | 4 | 1 | 100 | max | 400000 | 179,944 | 179,735 | 2.69 | 8.82 | 18.71 | 23.8 | 2.6 | 66301 | 851 |
| grpc | 4 | 100 | 100 | max | 1000000 | 510,825 | 352,284 | 475.18 | 787.88 | 880.43 | 883.1 | 881.0 | 414 | 232 |
| grpc | 4 | 1 | 100 | 20000 | 100000 | 19,994 | 19,993 | 0.27 | 0.79 | 4.28 | 26.1 | 0.1 | 25127 | 413 |

| transport | writers | handoffs | first handoff: held / client gap ms | held p50 ms | p90 | max | client gap p50 ms | max |
|---|---|---|---|---|---|---|---|---|
| inmem | 4 | 10 (10 completed) | 16.1 / 12.8 | 5.5 | 9.1 | 9.1 | 5.3 | 6.9 |
| grpc | 4 | 10 (10 completed) | 6.5 / 7.1 | 5.1 | 11.2 | 11.2 | 6.0 | 14.4 |
== SwitchoverTest run 1 08:49:26 load 3.88 2.40 1.61
Coordinator: handoff from A: Completed epoch 2, writes held 28.9698ms 
Coordinator: handoff from B: Completed epoch 3, writes held 19.0254ms 
Coordinator: 64991 acknowledged writes; A keys=64991 seq=64991, B keys=64991 seq=64991
Coordinator: OK
== SwitchoverTest run 2 08:49:41 load 4.48 2.60 1.69
Coordinator: handoff from A: Completed epoch 2, writes held 25.7418ms 
Coordinator: handoff from B: Completed epoch 3, writes held 26.6702ms 
Coordinator: 55313 acknowledged writes; A keys=55313 seq=55313, B keys=55313 seq=55313
Coordinator: OK
== SwitchoverTest run 3 08:49:56 load 5.44 2.90 1.80
Coordinator: handoff from A: Completed epoch 2, writes held 27.8755ms 
Coordinator: handoff from B: Completed epoch 3, writes held 17.9096ms 
Coordinator: 55876 acknowledged writes; A keys=55876 seq=55876, B keys=55876 seq=55876
Coordinator: OK
== SwitchoverTest run 4 08:50:11 load 5.55 3.03 1.86
Coordinator: handoff from A: Completed epoch 2, writes held 30.5566ms 
Coordinator: handoff from B: Completed epoch 3, writes held 31.0443ms 
Coordinator: 54701 acknowledged writes; A keys=54701 seq=54701, B keys=54701 seq=54701
Coordinator: OK
EXIT
```
