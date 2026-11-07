# RocksDb.Managed.Replication

Replication, planned switchover and a Raft cluster layer for the fully managed RocksDB port (`RocksDb.Managed`).
It is the managed port of rocksdb-sharp's `csharp/src/Replication`, `Replication/Switchover` and `Cluster` folders,
re-based on the managed engine's first-class replication APIs. rocksdb-sharp itself stays a native binding and is not
used here (except as the native reference in tests).

The library is **transport-agnostic**: nodes talk through small interfaces (`IPeerTransport`, `ISwitchoverPeer`,
`IClusterNodeClient`, `IPairNodeClient`, `IReplicationSourceClient`). The optional project
`RocksDb.Managed.Replication.MagicOnion` implements them over gRPC (MagicOnion), the tests use an in-memory transport
built on channels, and `dotnet/samples/{ReplicationTest,SwitchoverTest,ClusterTest}` are multi-process gRPC programs.

```
RocksDb.Managed.Replication            namespace RocksDb.Managed.Replication           checkpoint + WAL streaming
  Replication/                         ReplicationSource/Session/Consumer, WalTailer, ReplicationDelta, snapshot transfer,
                                       AdaptiveCommitDelayController
  Switchover/                          namespace ...Replication.Switchover             PrimaryWriteGate, ReplicationRoleState, SwitchoverNode
  Cluster/                             namespace RocksDb.Managed.Cluster               RaftClusterNode, RaftConfig, RaftPersistentState, RaftLogTerms, ...
  Hosting/                             namespace ...Replication.Hosting                ready-made node hosts (the logic of rocksdb-sharp's
                                       sample hosts, transport factored out): ClusterNodeHost, PairNodeHost,
                                       ReplicationPrimary/ReplicaFollower, WalStreamSender, DatabaseDigest
RocksDb.Managed.Replication.MagicOnion  IClusterService/IPairService/IReplicationService + gRPC clients + GrpcNodeServer
```

## File map (rocksdb-sharp -> managed)

| rocksdb-sharp | managed |
|---|---|
| `Replication/ReplicationClasses.cs` (`ReplicationFile`, `ReplicationSession`, batch DTOs) | `Replication/ReplicationClasses.cs` (`ReplicationFile`, `ReplicationSession`, `ReplicationFileInfo`, `SnapshotChunk`) |
| `Replication/ReplicationSource.cs` | `Replication/ReplicationSource.cs` (+ WAL retention by replica progress from the sample `ReplicationService`) |
| `Replication/ReplicationConsumer.cs` | `Replication/ReplicationConsumer.cs` (+ `SnapshotReceiver`, from the sample hosts' snapshot pull) |
| `Replication/ReplicationDelta.cs` | `Replication/ReplicationDelta.cs` |
| `Replication/WalTailer.cs` | `Replication/WalTailer.cs` |
| `Replication/RocksDbWalInspector.cs` | removed: `RocksDatabase.GetSortedWalFiles()` + `ReplicationSource.RetireArchivedWals` |
| `Replication/AdaptiveCommitDelayController.cs` | `Replication/AdaptiveCommitDelayController.cs` |
| `Replication/Switchover/{PrimaryWriteGate,ReplicationRoleState,SwitchoverNode}.cs` | `Switchover/` (same names) |
| `Cluster/{ClusterMode,RaftRole,ClusterMember,ClusterEvents,RaftConfig}.cs` | `Cluster/ClusterTypes.cs` |
| `Cluster/{RaftClusterNode,RaftPersistentState,RaftLogTerms,PeerState,ElectionTimer}.cs` | `Cluster/` (same names) |
| `Tests/ClusterTest/ClusterNodeHost.cs`, `Tests/SwitchoverTest/PairNodeHost.cs`, `Tests/ReplicationTest/ReplicationService.cs` (host logic) | `Hosting/ClusterNodeHost.cs`, `Hosting/PairNodeHost.cs`, `Hosting/ReplicationPrimary.cs` |
| `Tests/*/I*Service.cs`, `*Service.cs`, `ClusterPeerTransport.cs`, `PairPeer` | `RocksDb.Managed.Replication.MagicOnion` |
| `Tests/*/Program.cs` | `dotnet/samples/*/Program.cs` (gRPC, multi-process) and `tests/RocksDb.Managed.Replication.Tests` (in-process) |

## Replication: checkpoint + WAL stream

```csharp
// Primary
var source = new ReplicationSource(db);
using ReplicationSession session = await source.GetInitialStateAsync(checkpointDir, ct);   // pins file deletions
await foreach (SnapshotChunk c in session.StreamSnapshotAsync(consumerInventory, 1 << 20, ct)) Send(c);

using WalTailer tailer = source.TailWal(replicaNextSeq);
await foreach (WalChunk chunk in tailer.ReadChunksAsync(ct)) Send(chunk.Memory);   // merged batches, valid until next

// Replica
await SnapshotReceiver.ReceiveAsync(Receive(), fs, dbPath, preserveNames: ["raft"], ct);
await using var replica = await RocksDatabase.OpenAsync(dbPath, options);
var consumer = new ReplicationConsumer(replica);
if (!await consumer.TryIngestChunkAsync(chunkBytes, ct)) Resync();   // must start at latest + 1
```

* A replica applies the source's write batches at exactly the same sequence numbers; everything (switchover handoff
  points, Raft log indexes, attach checks) rests on that. Writes with `DisableWal`, `IngestExternalFiles` on the source
  and any write on a replica break it.
* `WalTailer` merges consecutive WAL batches into one serialized write batch under a single 12-byte header (summed
  count) - one message and one replica write per chunk. The bytes are a valid native `WriteBatch` (a test feeds them to a
  native rocksdb-sharp replica).
* The replica inserts a chunk into its memtable with up to `ReplicationConsumer.ApplyParallelism` threads (default:
  processor count, at most 4; `WriteOptions.MemtableInsertParallelism`). A primary's concurrent writers insert their
  batches in parallel, so a replica inserting each merged chunk with one thread fell behind a primary writing 4 x
  100-entry batches at full speed (75-80% of its rate, 0.5-0.9 s catch-up). The chunk is still one WAL record and one
  atomic write at the source's sequence numbers, applied in stream order and published only after every range is in;
  chunks with merge operands, blob indexes, timed puts or 2PC markers are inserted serially.
* When the WAL a replica needs was purged, the tailer throws `RocksDbException(NotFound)` (the hosts turn this into a
  "resync required" stream message) and the replica rebuilds from a new snapshot. `ReplicationDelta` keeps the
  immutable SST/blob files both sides hold byte-identically (name + size + SHA-256 of length and last 1 MiB), so a
  rebuild only transfers what changed.
* A snapshot stream is: plan (files the consumer keeps), file chunks with CURRENT last, end marker.
  `SnapshotReceiver` refuses a stream without plan or end marker, so a cut-off transfer is never opened as a database.
* WAL retention: with `DbOptions.WalTtlSeconds`/`WalSizeLimitMb` obsolete WALs are archived;
  `ReplicationSource.ReportReplicaProgress` + `RetireArchivedWals()` delete archived WALs once every replica applied
  them (nothing while a checkpoint session is in flight). Size the archive for the longest expected replica downtime.
* Steady-state streaming allocates nothing per replicated batch in this library: the tailer's and the replica's waits,
  the chunk buffer, the pooled `WriteBatch` the consumer applies, `ProgressReporter`, the WAL-retention check of
  `ReportReplicaProgress` / `RetireArchivedWals()` (the WAL files are listed only after a WAL rotation or once the
  slowest replica finished an archived WAL), Raft progress and commit waits, and `PairNodeHost.WriteAsync` /
  `ClusterNodeHost.WriteCommittedAsync` (with a non-cancellable token; a cancellable one costs a linked token source per
  write). The transport's own serialization is extra; see `docs/allocations.md` ("Replication and WAL tailing").
* Use the same column families on the replica (batches carry column family ids) and keep the WAL in the DB directory
  (`WalDir` empty) on replicas restored from a checkpoint (a checkpoint stores its WALs next to the tables).

## Planned switchover (two nodes)

`SwitchoverNode` hands the primary role to an up-to-date replica without losing an acknowledged write: lag check ->
persist `HandingOff` -> hold writes (`PrimaryWriteGate`) -> promote the replica in epoch + 1 once it holds exactly the
handoff point (it persists the promise first) -> demote. A lost or late answer is settled with `RejectEpoch` (the
replica promises the epoch, so a delayed promote can never succeed); an unreachable replica leaves the outcome
`Unresolved` with writes refused - two writable primaries are worse than none. Epochs are the Raft-term equivalent and
an epoch map (`startSeq -> epoch`, a `RaftLogTerms`) lets a rejoining node prove its log is a prefix of the primary's;
a diverged node (after `ForcePromoteAsync`) is rebuilt from a snapshot. `PairNodeHost` wires this with the WAL stream,
client writes with primary hints, snapshot resync and fault injection. See rocksdb-sharp's
`Replication/Switchover/README.md` for the full protocol discussion; it applies unchanged.

## Raft cluster

`RaftClusterNode` treats the RocksDB sequence number as the Raft log index, the WAL stream as log replication and a
checkpoint as catch-up from far behind. It adds terms/votes (`RaftPersistentState`, fsync'd before answering), a term
side-log (`RaftLogTerms`, `startSeq -> term`), elections, heartbeats carrying `(term, prevLogSeq, prevLogTerm,
leaderCommit, mode)`, quorum commit restricted to current-term entries (with a leader no-op entry, Raft 5.4.2), and the
one-way Bootstrap -> Cluster transition (a designated primary leads without election until a quorum is in sync).
Divergence is detected at stream attach (`CheckFollowerLogConsistency`), per chunk (sequence check) and per heartbeat
(prev term); since a WAL cannot be truncated, a diverged follower rebuilds from a leader snapshot (`BeginResync` /
`CompleteResync`). `ClusterNodeHost` implements the follower stream, progress reports, attach check and resync.

Durability: every log entry (client writes, the leader no-op, replicated chunks on followers) is fsync'd before it
counts - before the leader counts itself towards the commit quorum, before a follower reports or acknowledges it, and
before a node's latest sequence number (used in votes and heartbeats) moves past it (`ClusterNodeHostOptions.SyncWal`,
default true; concurrent client writes share one fsync, a follower syncs once per chunk). Without it a power loss of
the leader and one follower lost acknowledged writes (found by the cluster soak). A node that finds Raft state but no
database (killed during a snapshot restore) rebuilds from the current leader before it takes part, and a follower that
attaches adopts the leader's term map for everything past its own log. `WriteCommittedAsync(WriteBatch, ...)` writes a
batch atomically and returns a `ClusterWriteStatus` that separates a definite rejection (`NotLeader`: nothing written)
from the indeterminate outcomes (`LeadershipLost`, `TimedOut`). A brand-new cluster stays in Bootstrap mode until the
bootstrap primary has written something (an empty log never counts as in sync).
rocksdb-sharp's `Cluster/README.md` describes the protocol, failure modes and tuning; it applies unchanged, except
where noted below. Out of scope as before: pre-vote, membership changes, ReadIndex, leadership transfer.

## Differences from rocksdb-sharp (and why)

| rocksdb-sharp | managed | why |
|---|---|---|
| `WalTailer` keeps a native iterator, reopens it after a 250 us "grace period" because native iterators do not follow WAL rotation; waits by spinning then `Task.Delay(1)` | one `TransactionLogIterator` for the tailer's life (it follows rotation), waits on `RocksDatabase.WaitForSequenceAsync` (signalled by the write path) | no polling, no reopen scans; `Reopens`/`ReopenTime`/`ReopenGraceMicroseconds` are gone |
| `RocksDbWalInspector` parses WAL files (and cannot read compressed WALs) to find first sequence numbers | `RocksDatabase.GetSortedWalFiles()` | engine knows its WALs; handles archive, recycling, compression |
| `GetWalUpdates` returns `byte[]` per batch, `GetPooledWalUpdates` pooled arrays | `GetWalUpdatesAsync` yields `WalBatch` views; chunks are pooled | no allocation per batch |
| caller must `DisableFileDeletions` around `GetInitialState` | `ReplicationSession` pins deletions until released/disposed | WALs right after the checkpoint cannot be purged before the replica attaches |
| sync `IngestBatch(ReplicationBatch)` / `IngestChunk(span)` | `IngestChunkAsync`, `TryIngestChunkAsync` (sequence check), pooled `WriteBatch` | async-first; the per-chunk check every host did is now in the library |
| a chunk is one `Write`, inserted into the memtable by one thread | inserted in parallel ranges (`ApplyParallelism`, default min(cores, 4)), published atomically | the serial insert capped the replica below a primary with several writers |
| file transfer through `FileStream`s and `System.IO` | `IFileSystem` everywhere (checkpoint, snapshot transfer, Raft/role state) | works on `MemoryFileSystem` |
| `PrimaryWriteGate.HoldAsync` spins and polls | drained signal raised by the last `Exit` | no polling; `TryEnterAsync` is allocation-free while open |
| promoted replica spins until it reaches the handoff point | `WaitForSequenceAsync` | no polling |
| promote RPC sent cold while writes are held | a probe promote (`PromoteRequest.Probe`, refused without side effects) warms the RPC path on both nodes before the hold | the first promote of a process paid 15-30 ms per side over MagicOnion |
| promoted replica persists its epoch map and role separately; old primary persists its demotion, then closes the gate | one combined write (`ReplicationRoleState.BecomePrimary`); the gate closes first (the node is durably `HandingOff`) | 3 fewer fsyncs while writes are held |
| a follower sets its commit index to `min(leaderCommit, myLatestSeq)` | `min(leaderCommit, prevLogSeq)`: only the prefix the heartbeat verified against the leader's log | Raft's "index of last new entry"; a deposed leader's divergent tail past `prevLogSeq` is never reported committed |
| hosts poll `CommitSeq` every 15 ms to acknowledge writes | `RaftClusterNode.WaitForCommitAsync` | signalled |
| hosts wait fixed 500-1000 ms for "in-flight handlers" before disposing a DB on resync | `DatabaseUsageGate` drain | deterministic |
| snapshot stream ends without a marker | plan + files + `Complete` marker | a truncated transfer is detected |
| `RaftConfig`/`AdaptiveCommitDelayController` with defaulted ctor parameters | required ctor args + init properties / options object | no hidden defaults |
| `IPeerTransport.SendRequestVoteAsync(peer, term, id, seq, term, ct)` | `SendRequestVoteAsync(peer, RequestVotePayload, ct)`; `HandleAppendEntries(HeartbeatPayload)` (positional overload kept) | one payload type per RPC |
| `ElectionTimer`/heartbeats on `System.Threading.Timer`/`Task.Delay` | `TimeProvider` (`RaftConfig.TimeProvider`) | testable time |
| signature length prefix in machine byte order | little-endian | identical signatures across architectures |
| per-batch `EntryTerm` on the Raft stream | chunks merge batches, so a follower also merges the leader's term map at attach | a chunk may span a term boundary |
| MagicOnion in the samples only | same: `RocksDb.Managed.Replication.MagicOnion` is optional | the core stays transport-agnostic |

## Switchover cost

Writes held per handoff, measured on a shared 4-vCPU box (other builds running; see `docs/benchmarks.md`,
"Replication"): the gate drains in about 1 ms and the replica is writable 1-5 ms after the promote arrives; the rest
is the promote round trip and the fsync'd role-state writes on both nodes (one promise and one combined epoch/role
write on the replica). With a quiet disk a warm handoff holds writes for 5-8 ms in process and over gRPC on loopback;
under heavy load fsync and scheduling latency push the hold to 30-40 ms (median) and single handoffs past 100 ms. Of
our own costs, the first promote of a process paid 15-30 ms per side over MagicOnion (now probed before the hold) and
the hold contained four fsync'd role-state writes (now two; the demotion is persisted after the gate closed); the
remaining spread of the 60-220 ms measured earlier is machine load. `bench/RocksDb.Managed.Replication.Benchmarks`
(`switchover`) measures it.

## Tests and samples

`tests/RocksDb.Managed.Replication.Tests` runs every scenario of rocksdb-sharp's ReplicationTest / SwitchoverTest /
ClusterTest in process over an in-memory network (plus a native rocksdb-sharp replica fed with managed WAL chunks, and
2-node replication / switchover / 3-node cluster over real gRPC on localhost). The samples are the multi-process
versions (`dotnet run --project samples/ClusterTest`, etc.); each coordinator validates the final digests and exits
non-zero on a mismatch. `tools/RocksDb.Managed.ClusterSoak` (`tools/cluster-soak.sh`) is a Jepsen-style soak of the
Raft cluster mode over gRPC with network partitions through user-space proxies, kills, pauses, power loss, clock skew
and rolling restarts, for hours or days; see [docs/cluster-soak.md](../../docs/cluster-soak.md).
