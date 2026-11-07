# Cluster soak test (Jepsen style, Raft cluster mode)

`tools/RocksDb.Managed.ClusterSoak` is a long-running test of the Raft cluster mode of `RocksDb.Managed.Replication`
(`ClusterNodeHost` + `RaftClusterNode` over MagicOnion gRPC). The single-node soak ([soak.md](soak.md)) tests one
database; this one runs a real multi-process cluster and faults the network, the processes, their disks and their
clocks. It follows [Jepsen](https://github.com/jepsen-io/jepsen): a control node that is never faulted runs the
nodes, the clients and the nemesis; every client operation is recorded as `:invoke` followed by `:ok`, `:fail` or
`:info`; the checks run against that history and the state the nodes converge to.

```bash
cd dotnet
tools/cluster-soak.sh --dir /data/csoak --nodes 3 --duration 72h   # run (or resume) a cluster soak; --nodes 5 works too
tools/cluster-soak.sh --dir /tmp/csoak-smoke --smoke               # ~2.5-minute smoke run (40 s epochs, faults every ~3 s)
tools/cluster-soak.sh report --dir /data/csoak                     # summary of a run directory (also while it runs)
tools/cluster-soak.sh selftest                                     # prove every check catches an injected bug (~3 min)
tools/cluster-soak.sh export-history --dir /data/csoak --format edn --out /data/csoak/edn   # for Elle / Knossos
```

`tools/cluster-soak.sh` builds the tool in Release once and runs the executable (`FRAMEWORK=net11.0` for .NET 11). The
smoke run and the self-test are documented commands, not unit tests, so they do not slow the existing suites.

## What is checked, and what Raft here does not promise

Promised, and checked as failures:

- **No lost acknowledged writes.** A write the leader acknowledged as committed (`WriteCommittedAsync` returned
  `Committed`) is on every node once the cluster converged, also after power loss (the node's unsynced data is dropped).
- **No phantoms.** Every value on a node was written by some client operation that was invoked and did not fail
  definitely; a write rejected with `NotLeader` (the only definite rejection: the host returns it before writing
  anything) never appears.
- **State-machine safety / convergence.** After healing, all nodes have the same latest sequence number, commit index
  and database digest.
- **Election safety.** At most one leader per term (from every node's event log), at most one vote per (node, term)
  across restarts, and a node never restarts in a lower term than it had reached (the Raft state is durable).
- **Log matching / committed prefix.** All nodes' term maps (`RaftLogTerms`) agree on every entry, and the term of every
  entry committed at the previous VERIFY never changes.
- **Atomic batches.** A bank transfer is one write batch of two merges; any snapshot read on any node (leader or
  follower, at any time) sums to the initial total.
- **Counters.** Each counter is within [acknowledged increments, every increment that did not fail definitely].
- **Monotonic per-key versions** on every node from one VERIFY to the next.
- **Reads never return a value nobody wrote** (online and rechecked offline), and values always decode with a matching
  checksum.
- **Binary compatibility.** A checkpoint of one node read by native librocksdb (rocksdb-sharp) gives the same digest.
- **Liveness under a healed network.** After every fault is undone, the cluster elects a leader and converges within
  `--converge-timeout` (default 180 s); a node that never converges, exits unexpectedly or hangs in a graceful stop
  is a failure.

Not promised by this Raft implementation, therefore reported as statistics, never as failures:

- **Linearizable reads.** There is no ReadIndex or lease read: a read is served by one node's local database. A
  follower (and a deposed leader that does not know it yet) can return an older value than one acknowledged before
  the read started (`read.stale.leader` / `read.stale.follower`). Local reads can also see entries that are not yet
  committed and later disappear when the node resyncs, so a sequence of reads from one node can go back
  (`read.session-backwards.*`).
- **Membership changes, pre-vote, leadership transfer** are out of scope (README "Raft cluster"). Without pre-vote a
  node that was cut off comes back with an inflated term and forces an election; this costs availability only.
- **Availability during faults.** Writes need a quorum. When the nemesis takes out a majority (overlapping faults are
  allowed on purpose) writes must time out or fail, never be acknowledged; the time without a leader is reported.

## Process and network layout

```
supervisor (control node, never faulted)                       per node process ("node" sub-command)
  ClusterControl ------- control calls, direct ------------>   Kestrel :port   IClusterService (Raft RPCs, WAL stream,
  NodeHandle x N: spawn / SIGKILL / SIGSTOP / SIGCONT,                         snapshots; votes logged) + ISoakService
                  stdin: crash (power loss) | clock R | stop                   (client writes/reads, dump, checkpoint,
  client workers c0..cK ---- cK->nJ proxy ----------------->                   self-test injections)
  nemesis                                                      ClusterNodeHost(db/, raft/) on the fault file system
  LinkProxy for every directed link nI->nJ and cK->nJ   <--->  GrpcClusterClient to each peer = the nI->nJ proxy port
```

- Every node is configured with its peers' **proxy** ports, never their real ports, so the supervisor owns every
  node-to-node byte. Clients reach nodes through their own proxies too. The supervisor's control calls (status, dump,
  checkpoint) bypass the proxies, like Jepsen's SSH control connection.
- A proxy (`Proxy.cs`) is a user-space TCP relay. **Cut** closes every open connection (RST) and accepts-and-resets
  new ones, so RPCs fail and WAL streams break; **heal** lets new connections through; **delay** holds each
  forwarded chunk for N ms. No iptables / tc is needed.
- A link carries the RPCs its "from" side initiates and their answers, so cutting `A->B` alone is an **RPC-level
  one-way partition**: A cannot call B (heartbeats, votes, its follower stream), B can still call A. (A packet-level
  one-way drop would break both directions' TCP round trips, i.e. be a full cut at the RPC level.)
- Each node runs on two in-process fault file systems (`NodeFaultFs` over the single-node soak's
  `LazyFaultFileSystem`, `FaultFs.cs`): one for the database directory, one for its `raft/` subdirectory (term, vote,
  mode, term map). The `crash` command freezes both, drops everything not fsync'd (data, and directory entries not
  covered by a directory fsync) and ends the process with `_exit(77)`: a power loss of that node.
- Each node uses a `SkewedTimeProvider` as `ClusterNodeHostOptions.TimeProvider` (`RaftConfig.TimeProvider`): the
  `clock R` command makes its election timer and heartbeats run R times as fast.
- Each node appends role, leader, vote, start, resync and power-loss events to `events/<node>.log` (outside the
  fault file system, one `write` per line); the election-safety check reads them.
- Timings: election timeout 800..1600 ms, heartbeat 150 ms, RPC deadline 450 ms, client write timeout 2 s; memtable
  size (128 KiB .. 2 MiB) and WAL retention (0, 5 or 30 s: how often a lagging follower needs a snapshot instead of the
  WAL stream) are chosen per run from the seed. gRPC channels use a 0.5 s maximum reconnect back-off so healed links
  are used again quickly.

## Workload and history

Clients are Jepsen processes: each worker runs one operation at a time; after an `:info` it continues under a new
process id (old id + number of workers), so a process is never reused after an indeterminate op.

| Key space | Operation | Outcome mapping | Checked by |
|---|---|---|---|
| registers `rNNNNN` (workers x `--keys-per-client`, default 4 x 200; one writer per key) | `Put` of (key, version, deterministic payload, CRC32); versions strictly increase per key | `Committed` = `:ok`; `NotLeader` = `:fail`; `LeadershipLost`, `TimedOut`, a database error or any RPC error = `:info` | no-lost-acks, no-phantoms, monotonic-versions, reads |
| bank `bNNN` (16 accounts, 1000 each, UInt64Add) | transfer = one batch of two merges (+x, -x as wrapping uint64) | as above | bank (final), online bank reads |
| counters `cNNN` (16, UInt64Add) | merge +1..9 | as above | counters |
| reads | 1..4 registers, or all accounts, under one snapshot, from the believed leader or a random node | ok / fail (unavailable) | reads (statistics + phantom checks) |

Writes go to the node the client believes leads; a `NotLeader` answer carries the node's leader hint. Write mix: 50%
register writes, 12% transfers, 12% counter adds, 18% register reads, 8% bank reads.

`history/epoch-N.tsv` holds one op per line in a total order (index, time, type, process, f, node, keys, values,
note); nemesis actions are `:info` ops of process -1. The last `--keep-history` epochs (default 6) are kept.
`export-history --format edn` writes `register.edn` (rw-register: `[[:w k v]]`, `[[:r k v] ...]`, `nil` for absent)
and `bank.edn` (`:transfer` / `:read`) for [elle-cli](https://github.com/ligurio/elle-cli); ops still open at the
end of an epoch get an `:info` completion. Elle's `strict-serializable` will (correctly) flag stale follower reads:
check with `--consistency-models serializable` or filter to leader reads to look for real anomalies.

## Faults (nemesis) and what VERIFY expects

Every `--mean-fault` seconds (exponential, default 6 s) the nemesis starts a seeded, weighted fault and undoes it after
a hold time (2..15 s for network faults). Up to `--max-faults` (default 2) overlap: one network fault at a time and at
most one process fault per node, so e.g. a partition plus a killed node can leave no quorum for a while.

| Fault (`--faults` name, weight) | How | Expectation (in addition to the always-checked ones) |
|---|---|---|
| `PartitionHalves` (10) | random majority / minority; half the time the leader is put in the minority | the minority cannot acknowledge; no lost acks after healing; the stale leader's uncommitted tail is discarded (resync) |
| `IsolateLeader` (10), `IsolateNode` (6) | all links between the node and its peers cut; clients still reach it | as above; clients writing to the stale leader get `:info`, never `:ok` |
| `Bridge` (4) | two halves that only talk through one bridge node | at most one leader per term |
| `Ring` (4) | majorities ring (5 nodes: each node sees only its two ring neighbours); with 3 nodes a halves partition | as above |
| `OneWay` (6) | one link `A->B`, every outbound link of a node, or every inbound link of a node (leader half the time) | as above; a node that receives heartbeats but cannot answer, or answers but cannot send |
| `Slow` (4) | 20..320 ms latency on random node links | elections and commits under latency |
| `ClientCut` (3) | some clients cut from the leader | `:info` ops; nothing else |
| `Kill` (6), `KillLeader` (8) | SIGKILL, restart after 1..12 s | acked writes survive (page cache survives a process kill) |
| `Pause` (4), `PauseLeader` (6) | SIGSTOP for 0.5..6 s (longer than the election timeout), SIGCONT | a paused leader resumes believing it leads: it must not acknowledge, and steps down on the first higher term |
| `PowerLoss` (6), `PowerLossLeader` (8) | node drops all unsynced data (both directories), `_exit`, restart after 1..10 s | acked writes survive: the log entries a quorum counted were fsync'd (WAL) and the term/vote were fsync'd before answering |
| `KillDuringCatchup` (5) | SIGKILL, wait 3..12 s while writes continue (the WAL it needs may be purged), restart, SIGKILL again 0..600 ms after the first catch-up line (snapshot pull, resync or stream attach), restart | a node killed during a snapshot restore (no `CURRENT`, Raft state kept) rebuilds from the leader before voting; counted as `kill-during-snapshot` / `kill-during-stream-catchup` |
| `ClockSkew` (4) | one node's Raft clock at 0.25x, 0.5x, 2x or 4x | safety does not depend on clocks (no leases): only liveness changes |
| `RollingRestart` (2) | graceful stop (`stop`: host and database closed, exit 0 within 30 s) and restart of every node in turn | clean close and reopen lose nothing; a hang is a failure |

## RUN and VERIFY

```
init        start n1 (bootstrap primary), then the others; write the bank's initial balances through n1 (the cluster
            switches from Bootstrap to Cluster mode once a quorum replicated something); wait for convergence
repeat until --duration, a failure, or SIGINT/SIGTERM:
  RUN       clients + nemesis for --verify-every (default 5 min); online checks; an unexpected node exit or an online
            violation ends the epoch as a failure
  VERIFY    stop the nemesis and the clients (open ops stay :info), then the steps below; fold the converged state into
            the model (Jepsen's resolution of :info ops: they happened or not, as the cluster shows)
final       graceful stop of every node
```

| # | VERIFY step | Checks |
|---|---|---|
| 1 | `heal-and-converge` | undo every active fault, heal every link, SIGCONT, reset clocks, restart dead nodes; then one leader followed by every node in its term, Cluster mode, every node's latest sequence = commit index = the leader's, no resync running, stable over two polls |
| 2 | `convergence` | every node's dump (all keys and values under one snapshot) is identical: latest sequence, commit index, `DatabaseDigest`, every key |
| 3 | `no-lost-acks` | per node, every register's version >= the highest acknowledged version (this epoch) and >= the version at the previous VERIFY |
| 4 | `no-phantoms` | every value decodes (key, deterministic payload, CRC); every register version newer than the model was invoked in this epoch and did not fail; no key outside the workload |
| 5 | `bank` | all accounts present, total = 16000 on every node |
| 6 | `counters` | model + acked <= value <= model + acked + indeterminate |
| 7 | `monotonic-versions` | no register older than at the previous VERIFY, on any node |
| 8 | `election-safety` | event logs: <= 1 leader per term; <= 1 granted vote per (node, term); no restart in a lower term |
| 9 | `log-matching` | term maps of all nodes agree at every range boundary (and the entry before it); the committed prefix of the previous VERIFY keeps its terms; the committed log never shrinks |
| 10 | `reads` | online violations (phantom or undecodable reads, bank snapshot totals, partial bank states); offline recheck of every recorded read against the invoked versions |
| 11 | `native-cross-check` | every `--native-every` VERIFY (default 3): checkpoint of one node, native librocksdb (child process, read-only, uint64add merge operator) scans it; key count and digest must equal the node's |
| 12 | `availability` | statistics only: leaderless time (no leader followed by a quorum in its term), leaderless intervals, terms with a leader, write latency percentiles |

Each step records pass/fail, a count, its time and up to 20 problems in `verify/epoch-N.json`, `soak.log` and
`status.json`.

## Results and status.json

| Path | Contents |
|---|---|
| `status.json` | rewritten every 10 s and after every phase: `Phase` (init, run, verify, verified, stopping, done, failed), `Epoch`, `Seed`, `Nodes`, `ElapsedSeconds`, `DurationSeconds`, `Verifies`, `Faults` (per kind, plus `kill-during-snapshot` / `kill-during-stream-catchup`), `Ops` (`f.type` counts), `Stats` (stale / backwards reads by leader or follower, keys read), `OpsByFaultPhase` (the active faults when the op was invoked -> `f.type` counts), `TermsWithLeader`, `ObservedSeconds`, `LeaderlessSeconds`, `LeaderlessIntervals`, `LongestLeaderlessSeconds`, `LeaderlessIntervalSeconds` and `WriteLatencyMs` (count, p50, p95, p99, max), `NativeChecks`, `StepTotals`, `LastVerify`, `Failures`, `Result` (running, passed, stopped, failed) |
| `soak.log`, `nemesis.log` | the supervisor's log; every nemesis action with its time (the schedule) |
| `verify/epoch-N.json` | the VERIFY report: steps, op counts, statistics, a line per node (seq, commit, term, keys, digest) |
| `history/epoch-N.tsv` | the histories of the last epochs |
| `events/<node>.log`, `logs/<node>.log` | node event logs (checked), node output (every restart appends) |
| `nodes/<node>/` | node.json (the node's configuration), `db/` (database, `db/raft/` Raft state) |
| `run.json`, `model.json` | configuration, the model (register versions, counters, term map, latest sequence at the last VERIFY) |

`report` prints the result, faults per kind, ops ok / fail / info per function, read statistics, availability,
write latency, ops per fault phase, the VERIFY step totals and the last VERIFY, and exits non-zero after a failure.

## Running for days

Disk: the databases stay small (bounded key spaces, ~1 MB of live data; WAL archives up to the retention time). The
histories take about 5..8 MB per minute of RUN with 4 clients; with the defaults (6 epochs of 5 minutes kept) plan
for 0.5 GB plus the failure copies. Memory: about 150..250 MB per node process (5 nodes ~1.2 GB) plus the supervisor.

```bash
tmux new -s csoak 'cd dotnet && tools/cluster-soak.sh --dir /data/csoak --nodes 3 --duration 72h'
cd dotnet && nohup tools/cluster-soak.sh --dir /data/csoak --nodes 5 --duration 72h > /data/csoak.out 2>&1 &
systemd-run --user --unit cluster-soak --working-directory "$PWD/dotnet" tools/cluster-soak.sh --dir /data/csoak --duration 72h
```

Stop with Ctrl-C or SIGTERM: the RUN phase ends early, VERIFY completes, the nodes stop gracefully, `status.json`
gets `Result: stopped`. Running the same command again resumes: `run.json` and `model.json` are kept, the nodes are
restarted on their existing directories, and only `--duration` applies (counted from the restart).

Options: `--nodes N`, `--seed N`, `--clients N`, `--keys-per-client N`, `--verify-every 5m`, `--mean-fault 6s`,
`--max-faults N`, `--native-every N`, `--keep-history N`, `--converge-timeout 180s`, `--wal-ttl S`, and
`--faults PartitionHalves=10,IsolateLeader=10,...` (weights; 0 disables a fault).

## Reproducing a failure

On a failure the supervisor SIGKILLs every node (freezing their disks as they are), copies every node directory,
the event logs, node logs, histories, verify reports, `run.json`, `model.json`, `nemesis.log`, `soak.log` and
`status.json` into `failures/<n>/`, writes `summary.txt` (reason, epoch, seed, the failing report) and exits 1. Then:

- read `summary.txt` and the failing step's problems, then `nemesis.log` around the epoch, and the merged event logs
  (`cat failures/1/events/*.log | sort -n`) for who led which term;
- `export-history --dir failures/1 --format edn --out failures/1/edn` for Elle / Knossos;
- the node databases open with the managed engine or native `ldb` (`db/`; the Raft state is `db/raft/raft.state` and
  `raft.terms`);
- `tools/cluster-soak.sh --dir /tmp/replay --seed <seed> --duration 1h` replays the same plan (fault choices, holds,
  workload choices); thread interleavings and timing differ, so a failure may need several runs.

## Self-test

`tools/cluster-soak.sh selftest` (about 3 minutes) brings up a real 3-node cluster and runs one short epoch per case;
each case injects one bug and expects the named step to fail. It exits non-zero if a control fails or a bug is
missed, and keeps its state then.

| Case | Expected failing step |
|---|---|
| control: 25 s with the random nemesis | none (must pass) |
| a leader that acknowledges writes once they are in its own log (test flag `ack-early`), isolated from its peers while clients keep writing to it | `no-lost-acks` or `counters` (later register versions can mask a lost register write; the lost counter increments cannot hide) |
| control: power loss of the leader, then of a follower, on the fault file system | none (must pass) |
| the history claims an extra acknowledged write | `no-lost-acks` |
| an acknowledged register deleted through the leader without a history record | `no-lost-acks` |
| a register version nobody wrote, committed through the leader | `no-phantoms` |
| the write that produced a register's final value is recorded as a definite failure | `no-phantoms` |
| bank total broken (+1 merged into an account; repaired afterwards) | `bank` |
| a counter above every increment | `counters` |
| a register older than at the previous VERIFY (model manipulated) | `monotonic-versions` |
| a second leader in one term (a fake line in a copy of the event logs) | `election-safety` |
| one node's term map disagrees on a committed entry | `log-matching` |
| a read of a register version nobody wrote | `reads` |
| a write applied directly to one follower's database (outside the Raft log) | `convergence` |
| a follower whose Raft state (`raft/`) fsyncs make nothing durable, an election (leader killed), then a power loss of that follower | `election-safety` (it restarts in a lower term: its vote and term were not durable) |

## Jepsen and here

| Jepsen | Here |
|---|---|
| control node, `db` setup/teardown over SSH | the supervisor; node processes on localhost; control calls bypass the proxies |
| `nemesis.combined` partition package: `:majority`, `:minority-third`, `:primaries` | `PartitionHalves` (random majority / minority, leader in the minority half the time), `IsolateLeader`, `IsolateNode` |
| `partition-majorities-ring` | `Ring` |
| `bridge` | `Bridge` |
| asymmetric / one-way partitions (`iptables` DROP in one direction) | `OneWay` (RPC-level: link `A->B`, all outbound or all inbound links of a node) |
| `slow` / `flaky` net (`tc netem`) | `Slow` (per-link latency in the proxy) |
| `kill` / `start` (process crash) | `Kill`, `KillLeader`, `KillDuringCatchup` |
| `pause` / `resume` (SIGSTOP / SIGCONT) | `Pause`, `PauseLeader` |
| LazyFS `lose-unfsynced-writes` on a node | `PowerLoss`, `PowerLossLeader` (in-process fault file system, database and Raft state) |
| `clock` nemesis (bump / strobe the clock) | `ClockSkew` (`RaftConfig.TimeProvider` rate 0.25x..4x); wall-clock jumps are not modelled because nothing in the protocol reads wall time |
| rolling restarts | `RollingRestart` |
| `:invoke` / `:ok` / `:fail` / `:info`, fresh process after `:info` | same, `history/epoch-N.tsv`, `export-history --format edn` |
| Elle `rw-register` / Knossos linearizability | `register.edn` for Elle; built-in final-state checks (lost acks, phantoms, monotonic versions); stale reads reported, not failed (no ReadIndex) |
| `bank` checker | `bank` step and online snapshot reads; `bank.edn` |
| counter checker | `counters` step |
| Raft-specific: election safety, log matching | `election-safety` (event logs), `log-matching` (term maps) |

## What it found

1. **Acknowledged writes lost on power loss** (fixed). Client writes, the leader no-op and the followers' replicated
   chunks were written without WAL sync, while the leader counted its own latest sequence and the followers' (equally
   unsynced) progress towards the commit quorum. The first smoke run lost an acknowledged counter increment after two
   overlapping leader power losses. Every log entry is now fsync'd before it counts (`ClusterNodeHostOptions.SyncWal`,
   default true; `RaftConfig.SyncLogWrites`; `ReplicationConsumer.SyncWrites`). Regression test:
   `ClusterTests.AcknowledgedWritesSurviveAPowerLossOfTheWholeCluster` (MemoryFileSystem power loss of the whole
   cluster: all 70 acknowledged writes were lost before the fix).
2. **A node killed during a snapshot resync voted with an empty log** (fixed). The restore deletes the old files
   before CURRENT arrives; on restart the bootstrap primary opened an empty database (keeping its term and vote) and
   voted for any candidate, so a candidate missing a committed write could be elected; other nodes pulled their
   snapshot only from the bootstrap member (which may not lead) and gave up after 100 attempts, failing the start.
   Now any node with Raft state but no database rebuilds from the current leader first. Regression test:
   `ClusterTests.NodeRestartedWithoutItsDatabaseRebuildsFromTheLeaderBeforeVoting`.
3. **Stale term ranges past a follower's log** (fixed, found while reading the attach path for the log-matching
   check). An attaching follower recorded the leader's term ranges past its log with `RaftLogTerms.Record`, which
   ignores a range that starts before the last recorded one; a range learned from a deposed leader (or recorded just
   before a power loss) won, mislabelling the incoming entries (wrong `lastLogTerm` in votes, spurious resyncs).
   `RaftPersistentState.AdoptTermsAfter` replaces our ranges past the attach point with the leader's. Test:
   `BuildingBlockTests.AttachingFollowerAdoptsTheLeadersTermsPastItsLog`.
4. Behaviour worth knowing (not a bug, documented): a brand-new cluster stays in Bootstrap mode until the bootstrap
   primary wrote something, because an empty log never counts as in sync.

## Results

See the end of this file for the latest validation runs (filled in by the runs below).

## Limitations

- All nodes run on one machine: no real network, disks or clocks are separate; the proxies, the fault file system and
  the Raft `TimeProvider` stand in for them. Power loss is simulated per node for the files directly in its database
  directory and its `raft/` directory; torn writes (covered by the single-node soak) are not applied here.
- Final-state checks see the last version of each register; an acknowledged register write overwritten by a later
  committed write of the same key cannot be told apart from one that was lost. Counters and the bank catch lost
  writes that registers can mask; the EDN history lets Elle check the register reads in full.
- A write that a client gave up on (deadline) can in principle still be applied by the server after the VERIFY's
  convergence snapshot; it would show as a phantom in the next epoch. The server-side write path cannot outlive the
  client's deadline by more than the commit wait, so this has not happened, but a report of a phantom should be read
  with that in mind (its history line tells).
- Bank transfers use merges, not read-modify-write transactions (the cluster host has no transactions).
- Membership changes, ReadIndex, pre-vote and leadership transfer are not implemented, so they are not tested.
