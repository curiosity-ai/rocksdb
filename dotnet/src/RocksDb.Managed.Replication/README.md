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

## Versions: one version per deployment

Every node of a deployment - both nodes of a pair, all members of a cluster, a primary and its replicas, and the
clients that use the gRPC services - must run the **same version of this layer** (`RocksDb.Managed.Replication` and
`RocksDb.Managed.Replication.MagicOnion`). The protocol between nodes (the transport interfaces, the gRPC contracts and
their MessagePack DTOs, the role / Raft state files) is not versioned and nothing is negotiated: there is no
compatibility between versions of it, and therefore **no rolling-upgrade guarantee across versions of this layer
yet**. To change its version, stop every node and start them all on the new one.

This is not about the data or about your application: the database directories are native RocksDB format (the core
library's on-disk compatibility is untouched), and upgrading the *application* on top of an unchanged version of this
layer needs no downtime - hand the primary role to the other node, restart, hand it back ("Planned switchover" below;
the test `RollingUpgradeHandsOffBothWaysWithoutLosingWrites` and `samples/SwitchoverTest` do exactly that).

A peer is still not trusted to be well-formed: malformed fragments, unknown snapshot chunk kinds, plans that keep
mutable files, Raft requests of non-members, reports for another term or epoch are refused.

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
  count) - one message and one replica write per chunk. A chunk ends before a batch that does not fit into the rest of
  it, so only a single batch that is larger than the chunk size on its own makes a larger chunk. The bytes are a valid native `WriteBatch` (a test feeds them to a
  native rocksdb-sharp replica).
* The replica inserts a chunk into its memtable with up to `ReplicationConsumer.ApplyParallelism` threads (default:
  processor count, at most 4; `WriteOptions.MemtableInsertParallelism`). A primary's concurrent writers insert their
  batches in parallel, so a replica inserting each merged chunk with one thread fell behind a primary writing 4 x
  100-entry batches at full speed (75-80% of its rate, 0.5-0.9 s catch-up). The chunk is still one WAL record and one
  atomic write at the source's sequence numbers, applied in stream order and published only after every range is in;
  chunks with merge operands, blob indexes, timed puts or 2PC markers are inserted serially.
* When the WAL a replica needs was purged, the tailer throws `RocksDbException(NotFound)` (the hosts turn this into a
  "resync required" stream message) and the replica rebuilds from a new snapshot. `ReplicationDelta` keeps the
  immutable SST/blob files both sides hold byte-identically (name + size + SHA-256 of the length and the last 1 MiB of a table file - its properties name the one creation event - or of the whole of a blob file, which carries no identity), so a
  rebuild only transfers what changed. A source that cannot read its own WAL (Corruption) also asks for a resync,
  marked `WalStreamMessage.SourceFault` and counted in the host's `WalStreamFaults`; a follower that has applied
  nothing since its last rebuild then retries the stream instead of rebuilding again (`FollowerResyncPolicy`).
* A snapshot stream is: plan (files the consumer keeps), file chunks with CURRENT last, end marker.
  `SnapshotReceiver` refuses a stream without plan or end marker, so a cut-off transfer is never opened as a database;
  it writes CURRENT under a temporary name until the end marker (whatever order the sender uses) and refuses a plan
  that keeps anything but immutable files it holds.
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

## Large write batches and large client writes

A message on the wire is bounded; a write is not. Two places send a payload of the user's size, and both send it in
pieces of at most `MaxChunkBytes` (256 KiB by default) that the receiver puts together before it applies anything.
In both the split exists on the wire only: the WAL, the sequence numbers and the atomicity of the write are those of
one message.

**WAL stream** (`WalStreamMessage`, `WalStreamSender`, `WalStreamAssembler`). A chunk is at least one whole write
batch, so one large batch would be one message of its size - above gRPC's 4 MiB receive limit, and without any bound.

* The source always sends a batch larger than `MaxChunkBytes` as consecutive fragments, slices of the tailer's buffer
  (the tailer never merges such a batch with others): each carries the batch's first sequence number, source id,
  source term, entry term and total length, plus its own offset; the entry count is on the last fragment and 0 on the
  others. A chunk that fits is one message. Every consumer of a stream reassembles with `WalStreamAssembler` (the
  hosts and `ReplicaFollower` do).
* The follower collects the fragments in one pooled buffer of the announced length and applies the batch through the
  same path as an unfragmented chunk, once the last fragment arrived. Its log never ends inside a batch, and progress
  reports / acknowledgements (`WriteAck = Replica`, Raft progress) only ever name applied batches.
* A stream that ends or fails takes its partial batch with it; the follower attaches again at its last applied
  sequence number and is sent the batch from offset 0.
* A fragment that does not continue the stream - wrong offset, another first sequence / source id / source term /
  entry term / total length, an ordinary message inside a batch, a fragment without the beginning of its batch, bytes
  past the total, an empty fragment, an entry count before the end, a completed batch whose header disagrees with what
  its fragments announced - discards the partial batch. The first time at a position the follower drops the stream and
  attaches again (a fault of the connection does not repeat). If the next attempt fails at the same position - nothing
  was applied in between, so the source sends that batch wrongly every time - the follower rebuilds from a snapshot,
  which contains the batch as files, and follows from behind it (`FollowerResyncPolicy.ShouldRebuildAfterStreamFault`).
  It never attaches again and again to the same broken batch. `ReplicaFollower.FollowAsync` throws
  `WalStreamProtocolException` (with the replica's position) and leaves that decision to its caller.
* `MaxBatchBytes` (`PairNodeHostOptions`, `ClusterNodeHostOptions`, `ReplicaFollowerOptions`; default 1 GiB): a batch
  announced larger is refused before anything is allocated, and the follower rebuilds from a snapshot at once
  (`ReplicaFollower`: `ReplicationResyncRequiredException`) - the stream cannot get it past the batch, a snapshot
  carries the same data as files. The default is the largest array `RocksArrayPool` pools (a serialized batch is one
  array, below 2 GiB, on both nodes); lower it to bound a follower's memory. It also applies to an unfragmented chunk.

**Client writes over gRPC** (`IPairService` / `IClusterService`, `GrpcPairClient.WriteAsync` /
`GrpcClusterClient.WriteAsync`). The write RPC (`WriteAsync(string key, string value)`) is one unary request, and a
server refuses a request above its receive limit (gRPC's 4 MiB; `GrpcNodeServerOptions.MaxReceiveMessageSize`) with
ResourceExhausted, "Received message exceeds the maximum configured message size." Plain replication has no write RPC,
and no RPC takes a whole `WriteBatch`.

* The clients' `WriteAsync(key, value, ct)` sends a payload (UTF-8 bytes of key + value) of at most
  `GrpcWriteOptions.MaxChunkBytes` as the unary RPC (one request, no stream to set up - the cheaper path for the
  common small write) and a larger one through the client-streaming RPC
  `WriteFragmentsAsync`: pieces `WriteFragmentDto { TotalLength, KeyLength, Offset, Data }`, every one with the same
  total and key length; the node answers once, with the unary RPC's result type.
* The node checks the announced length against `FragmentedWriteOptions.MaxWriteBytes` (64 MiB) and against what it
  already holds (`MaxPendingBytes`, 256 MiB over all uploads in flight) before it allocates, collects the pieces in a
  pooled buffer, and applies the write through the same host call as the unary RPC (`PairNodeHost.WriteAsync` /
  `ClusterNodeHost.WriteCommittedAsync`: same role check, `SyncWal`, `WriteAck`, commit wait and result) after the
  last piece. An upload that is malformed (InvalidArgument), over a limit (ResourceExhausted), cancelled, cut off or
  silent for `IdleTimeout` (30 s) applies nothing and gives its buffer back. The client turns the node's refusals into
  `LargeWriteException` (`TooLarge`, `ServerBusy`, `Rejected`).
* Deadline: `GrpcWriteOptions.Timeout` (30 s) plus the payload size at `MinBytesPerSecond` (1 MiB/s).
* Retries are the caller's and the guarantee is the unary RPC's: a write whose call failed without a result may have
  been applied, and sending it again applies it again (harmless for the same key and value).

Limits of both: the receiver holds one whole batch / write in memory (the reassembly buffer, plus the engine's copy
while it is applied) - fragments bound the size of a message, not of a write; the caps above bound the write.

### Encryption and authentication over gRPC (`SharedSecretTls`)

By default the gRPC transport is plaintext and a server accepts every caller: use that on a trusted network only.
`SharedSecretTls` turns on TLS with authentication in both directions from one secret (a byte array of at least 16
random bytes) that every node and client of the deployment holds:

```csharp
using var tls = new SharedSecretTls(secret);                       // one per process
await using GrpcNodeServer server = await GrpcNodeServer.StartAsync<PairService, PairNodeHost>(
    host, address, port, new GrpcNodeServerOptions { Tls = tls }, ct);
using var peer = new GrpcPairClient("B", "https://10.0.0.2:5000", timeout, tls.CreateChannelOptions());
```

Each `SharedSecretTls` creates a random key pair and a self-signed certificate with an extension holding an HMAC of its
public key, keyed with a key derived from the secret. Server and client both present their certificate and accept the
other side only when that HMAC matches. Only a holder of the secret can make an accepted certificate; a copied
certificate is useless without its private key; the secret itself is never sent.

* Host names, validity dates, chains and revocation are not checked: the secret is the only identity, and every holder
  is trusted as any node and any client (there are no roles).
* All nodes and clients need the secret, including clients that only write.
* To change the secret, restart the nodes with the new one; a node accepts one secret at a time.
* A caller without the secret fails in the TLS handshake. `GrpcPairClient` and `GrpcClusterClient` wait for a
  connection, so there a wrong secret looks like an unreachable peer (calls end at their deadline).
* A host that runs the services in its own ASP.NET Core application uses `tls.ConfigureServer` in Kestrel's `UseHttps`
  and `tls.ConfigureClient` on its own `SocketsHttpHandler`.
* Dispose the `SharedSecretTls` after the servers and channels that use it.

### Message sizes over gRPC

Channels and servers keep gRPC's default receive limit of 4 MiB (`GrpcTransportDefaults.MaxReceiveMessageSize`,
`GrpcNodeServerOptions.MaxReceiveMessageSize`); `GrpcTransportDefaults.Apply` only adds keep-alive pings. What the
library sends stays below it whatever the size of the data:

| message | bound |
|---|---|
| WAL chunk, fragment of a large batch (`WalChunkDto`) | `MaxChunkBytes` (256 KiB) + ids |
| snapshot file data (`SnapshotChunkDto`) | `SnapshotChunkBytes` (1 MiB) + file name |
| client write, unary / one piece of an upload | `GrpcWriteOptions.MaxChunkBytes` (256 KiB) |
| hash request of a snapshot delta and its answer (`GetFileHashesAsync`) | asked in batches of at most `GrpcTransportDefaults.MaxFileListBytes` (1 MiB, about 10 000 files) each |
| inventory of a snapshot request and the plan that answers it | cut to the largest files that fit into `MaxFileListBytes`; the rest is transferred instead of reused |
| term map / epoch map (attach check, `GetRaftTermsAsync`, `GetEpochsAsync`) | not bounded by a setting: one line of at most 41 bytes per term (epoch) that wrote entries - 4 MiB after about 100 000 leadership (primary) changes |
| votes, heartbeats, status, progress reports, digests | fixed size |

The chunk sizes are options: keep them at or below `GrpcTransportDefaults.MaxPayloadBytes` (4 MiB less 64 KiB), or
raise the receive limits of every channel and server with them. A message above the limit is refused by the receiver
with ResourceExhausted - for a WAL or snapshot stream on every attempt. A cluster whose term map has outgrown the
limit (which takes a leader change per second for more than a day) needs the limits raised on its channels.

## Planned switchover (two nodes)

`SwitchoverNode` hands the primary role to an up-to-date replica without losing an acknowledged write: lag check ->
persist `HandingOff` -> hold writes (`PrimaryWriteGate`) -> promote the replica in epoch + 1 once it holds exactly the
handoff point (it persists the promise first) -> demote. A lost or late answer is settled with `RejectEpoch` (the
replica promises the epoch, so a delayed promote can never succeed); an unreachable replica leaves the outcome
`Unresolved` with writes refused - two writable primaries are worse than none. Epochs are the Raft-term equivalent and
an epoch map (`startSeq -> epoch`, a `RaftLogTerms`) lets a rejoining node prove its log is a prefix of the primary's;
a diverged node (after `ForcePromoteAsync`) is rebuilt from a snapshot. `PairNodeHost` wires this with the WAL stream,
client writes with primary hints, snapshot resync and fault injection; while a node is `HandingOff` outside a running
handoff (target unreachable, or a restart in the middle of one) it asks the other node every
`PairNodeHostOptions.ResolveInterval` how the handoff ended. See rocksdb-sharp's
`Replication/Switchover/README.md` for the protocol discussion; it applies except for the durability modes below.

### Durability of a pair

What `PairWriteStatus.Applied` promises is set by two options (set them alike on both nodes - either can be primary):

| `SyncWal` | `WriteAck` | an acknowledged write is | lost when |
|---|---|---|---|
| false (default) | `Primary` (default) | in the primary's log, not fsync'd; streamed to the replica afterwards | the primary loses power and the replica had not received it; or had, but cannot be reached while the primary restarts (or `YieldToReplicaAheadOnRestart` is off); the primary is lost for good before the replica received it; a node promoted by a handoff loses power before the entries it took over with reached its disk and the other node is unreachable when it restarts |
| true | `Primary` | on the primary's disk | the primary is lost for good (disk included) before the replica received it |
| false | `Replica` | in both nodes' logs, not fsync'd | both nodes lose power; or the primary loses power and the replica is unreachable while it restarts |
| true | `Replica` | on both disks | both disks are lost |

* `SyncWal` (as `ClusterNodeHostOptions.SyncWal`): the primary fsyncs a client write before acknowledging it (group
  commit: concurrent writes share one fsync) and before it becomes visible to the WAL stream, so the replica never holds
  an entry the primary's disk does not; the replica fsyncs every chunk before its sequence number moves past it, so a
  node promoted by a handoff holds the handoff point on disk.
* `WriteAck = PairWriteAck.Replica`: the replica reports the position it holds after every chunk
  (`IPairNodeClient.ReportProgressAsync`, repeated until it arrives) together with the epoch it has for that entry; the
  primary counts a report only under the attach check's rule (it holds that entry in the same epoch) and acknowledges a
  write once it is covered. Without a replica that follows, a write ends as `PairWriteStatus.NotReplicated` after
  `ReplicaAckTimeout`: it is in the primary's log and not acknowledged (indeterminate, like a timeout). The wait is
  inside the write gate, so a handoff starts only after the writes in flight were acknowledged or gave up.
* `YieldToReplicaAheadOnRestart` (default true): a node that restarts as primary and finds the replica holding entries
  past the end of its recovered log hands the role to the replica instead of rebuilding it
  (`SwitchoverNode.YieldToReplicaAheadAsync`), and then follows it. It does so only if the replica's last entry carries
  the epoch its own fsync'd epoch map has for that sequence number - an epoch has one primary and one history, so the
  replica's log is then what this node had written - and it uses the handoff protocol (promise, durable `HandingOff`,
  promote at an exact sequence number, `RejectEpoch` to settle), so it cannot make two primaries. Until the restarted
  node has settled this it does not answer attach checks or serve streams. The restarted node itself may be rebuilt
  from the new primary (a replica stores a chunk as one WAL batch and a stream cannot start inside one); the node that
  holds the acknowledged writes never is.
* One epoch, one history: the epochs a node starts on its own (after a restart, after an aborted handoff, by
  `ForcePromoteAsync`) are even for the node whose id sorts first and odd for the other, so a forced promotion and a
  primary restarting out of reach never end in the same epoch. Handoff epochs are agreed by both nodes.
* `ForcePromoteAsync` is outside all of this: it gives up whatever the replica had not received, and with two nodes it
  can make two primaries.

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
attaches adopts the leader's term map for everything past its own log. A follower reports progress only while its term
is at most the term of the leader it follows (a vote it granted in a later term may have been for a candidate without
the entries it is applying), and it stops applying a stream from a leader of an older term. `WriteCommittedAsync(WriteBatch, ...)` writes a
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
| hosts report follower progress on a 200 ms timer | one report per applied chunk (coalesced); a report whose send failed is repeated every `RetryDelay` until one arrives | no idle traffic, and the last report of a burst still cannot be lost (the leader could not commit a quorum-held entry without it) |
| `HandleRejectEpochAsync` looks for a running promotion, releases the node's lock, takes it again to promise the epoch | one critical section | a promote for that epoch landing in between was promoted although the reject answered "replica, epoch promised": two primaries |
| pair writes are never fsync'd and are acknowledged by the primary alone; after a power loss of the primary the replica is rebuilt from it | `PairNodeHostOptions.SyncWal`, `WriteAck = Replica`, and a restarted primary yields to a replica that holds its lost tail | acknowledged writes the replica still held were lost; see "Durability of a pair" |
| every node allocates "promised + 1" for an epoch it starts on its own | even / odd by node id | a forced promotion and a primary restarting (or resuming after an aborted handoff) out of reach became primaries of the same epoch with different logs; the attach check then takes one log for a prefix of the other |
| a pair primary that restarts continues in its epoch; a bootstrap primary that restarts continues in its term |
 a new epoch / term starts at the end of the recovered log (`SwitchoverNode.StartNewEpochAfterRestart`, `RaftPersistentState.StartTerm`) | after a power loss the recovered log may end before entries a replica holds (pair writes, and Raft with `SyncWal = false`, are not fsync'd); new entries at those sequence numbers in the same epoch / term let the replica's stale tail pass the attach check - silent divergence |
| a Raft state file that cannot be read starts the node "fresh" (term 0, no vote) | the read error is thrown | a node that forgot its vote can vote twice in one term |
| a vote request or heartbeat of any node is handled | only of a member | a node configured for another cluster could get the vote, and its term deposed the leader |
| gRPC channels with default settings | `GrpcTransportDefaults`: HTTP/2 keep-alive pings on channels and servers; the 4 MiB receive limit stays, and every message is bounded below it | a replica reading a stream never noticed a primary that vanished without closing the connection |
| a write batch larger than the chunk size is one stream message of its size; a client write is one unary request | fragments of at most `MaxChunkBytes` on the WAL stream (always) and on the write RPC (`WriteFragmentsAsync`), reassembled before anything is applied; `MaxBatchBytes` / `MaxWriteBytes` caps | no message was bounded (2 GB at most): a WAL chunk above gRPC's 4 MiB receive limit never reached the replica, and a client write above it was refused; see "Large write batches and large client writes" |
| snapshot delta reuse by name + size + hash of the last 1 MiB for every file | the whole file for blob files | a blob file has no identity; blob files of two histories can agree in name, size and tail |

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
