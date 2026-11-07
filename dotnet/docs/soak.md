# Soak test (Jepsen style)

`tools/RocksDb.Managed.Soak` is a long-running stability test for the managed port. You start it, leave it running
for hours or days, and it repeatedly runs a concurrent workload on one database, ends it with a fault (process kill,
simulated power loss, torn writes, I/O errors, pauses, a crash during recovery, or a clean shutdown), and then checks
the recovered database against a model of everything the workload did. The design follows
[Jepsen](https://github.com/jepsen-io/jepsen): each operation is recorded as `:invoke` followed by `:ok`, `:fail` or
`:info`, faults come from "nemeses", and the checks run against the history instead of a known expected state.

```bash
cd dotnet
tools/soak.sh --dir /data/soak --duration 72h      # run (or resume) a soak
tools/soak.sh report --dir /data/soak              # summary of a run directory (also while it runs)
tools/soak.sh selftest                             # prove that every check catches an injected bug (~1 min)
tools/soak.sh --dir /tmp/soak-smoke --smoke        # 2-minute smoke run with short cycles (CI)
```

`tools/soak.sh` builds the tool in Release once and runs the executable. The equivalent without the script is
`dotnet run -c Release --project tools/RocksDb.Managed.Soak -f net10.0 -- run --dir /data/soak --duration 72h`. Set
`FRAMEWORK=net11.0` to run on .NET 11.

## Process model

```
supervisor (long-lived, never crashed on purpose)
  init        child: creates the DB (5 column families, bank accounts), model.bin
  repeat until --duration ends, a check fails, or SIGINT/SIGTERM:
    RUN       child "worker": opens the DB, runs the workload for a random time, records the history
    FAULT     the supervisor ends the worker with the cycle's nemesis (see the faults table)
    VERIFY    child "verifier": opens the DB, runs the named checks against model + history
    CORRUPT   (every --corrupt-every cycles) child "corrupt-check" on a damaged COPY of the DB
    FOLD      the verifier's observation becomes the new model; old histories are deleted
  final VERIFY with the native cross-check
```

Each cycle has a seed derived from `--seed` and the cycle number, which picks the fault, the RUN length (exponential
with mean `--mean-run`, default 40 s, clamped to `[--min-run, --max-run]`, default 3 s .. 10 min, so there are many
short cycles per hour), the per-cycle options and the workload's random choices. The seed is printed at start and
kept in `run.json`.

The supervisor talks to the worker over stdin (`stop`, `crash clear|tornop|tornseq <seed>`, `arm wal|manifest|sst <n>`,
`ioerr <write ppm> <sync ppm> <read ppm>`, `ioerr-off`) and reads `OPENED <ms>` from its stdout. Worker exit codes:
0 clean stop, 3 online check violation, 5 close failed, 77 simulated crash, 137 SIGKILL. Any other exit (a crash,
an unhandled exception) is a failure.

## The workload and its invariants

The worker opens the DB as an `OptimisticTransactionDatabase` (validation policy `ValidateSerial`; see below) with
these column families. All sizes are fixed per run (`run.json`), so the model and the history per cycle stay bounded.

| Key space | Column family | Threads | Operations | Invariant |
|---|---|---|---|---|
| Registers | `reg0`, `reg1` (key index parity) | 4 writers, one writer per key, 1500 keys each | `Put`/`Delete` through `cf.Put`, `cf.PutAsync`, `WriteBatch` + `Write` / `WriteAsync`; `Sync = true` on 0, 0.05% or 0.5% of all writes (chosen per worker; sync writes sync the whole WAL, so they stay rare) | value = (writer, key, version, payload length, CRC32 of the payload, a payload of 0..4 KiB derived from the version); versions strictly increase per writer, so a writer's ops are totally ordered |
| Bank | `bank` (UInt64Add) | 2 | transfers as a `WriteBatch` of two merges (+x, -x as wrapping uint64) and as optimistic transactions (`GetForUpdate` both, `Put` both, `Commit`; `Busy`/`TryAgain` = `:fail`) | 24 accounts, total constant |
| Counters | `ctr` (UInt64Add) | 2 | `Merge` (+1..9) sync, async and in a batch | acked increments <= value <= acked + indeterminate |
| Range deletes | `rdel` | 1 | `Put` with a version, `DeleteRange` over up to 64 keys (sync and async) | the key space equals the effect of a prefix of the thread's ops |
| Syncs | - | 1 | `SyncWal`, `FlushWal(sync: true)`, `SyncWalAsync`, `FlushWalAsync(true)` every 0.1..1.5 s | marks the earlier acknowledged writes as synced |
| Admin | all, plus `scratch-*` | 1 | `Flush`/`FlushAsync`, `CompactRange`/`CompactRangeAsync` (whole or partial), `GetLiveFilesMetadata` sanity, `GetProperty`/`TryGetIntProperty`, a checkpoint every 20 s (opened read-only: bank total, register values), create / write / flush / drop a scratch column family every 15 s | - |

Online checks run in two reader threads while the workload runs. A violation is logged as `VIOLATION ...` and ends
the worker with exit code 3, which the supervisor treats as a failure:

- monotonic reads: a read that starts after a write's ack returns that version or a newer one, never a version newer
  than any invoked write (`TryGet`, `GetPooled`, `GetAsync`, `MultiGetAsync`); values decode and their checksums match;
- bank: under one snapshot, `MultiGet` (sync or async) of all accounts sums to the total, and an iterator over the bank
  column family under the same snapshot returns the same balances;
- scans: an iterator under a snapshot from a random key returns strictly increasing keys with valid values; the reverse
  scan (`SeekForPrev`, `Prev`) returns the same entries; point reads under the same snapshot agree;
- `Iterator.Refresh`: a refreshed iterator keeps returning sorted, valid entries.

Why `ValidateSerial`: with upstream's default `ValidateParallel` the conflict check runs before the write group and
only excludes other optimistic transactions, so a plain write that lands between the check and the commit's write is
lost. Upstream behaves the same way; the first run of this soak caught exactly that (bank total off by one transfer)
with the parallel policy. The serial policy checks inside the write group, which also sees the plain merge batches.

### Options

Fixed per database (from the seed): compression (none, Snappy, LZ4, ZSTD), `format_version` 2..7, filter (none,
Bloom, Ribbon), checksum type (crc32c, xxHash, xxHash64, XXH3), block size (1, 4, 16 KiB), blob files on `reg1`
(min_blob_size 1 KiB, blob GC) on or off. Always: `file_checksum_gen_factory` = crc32c (for `VerifyFileChecksums`),
`paranoid_checks`.

Varied per cycle (safe to change across a reopen): `write_buffer_size` 64 KiB..1 MiB, `max_background_jobs` 1..4,
`compaction_readahead_size` 0 / 256 KiB / 2 MiB, block cache 1 / 8 / 32 MiB, `recycle_log_file_num` 0 or 1,
`target_file_size_base` 256 KiB..2 MiB, `max_bytes_for_level_base` 1..4 MiB, `level0_file_num_compaction_trigger`
2..6, `max_subcompactions` 1..2. The small sizes keep flushes and compactions running all the time and the LSM several
levels deep.

## Faults (nemeses) and what VERIFY expects

| Fault (`--faults` name) | How | Durability expectation |
|---|---|---|
| `Kill` (default weight 30) | SIGKILL at a random moment of the RUN phase (flushes, compactions, column family creation and checkpoints are running all the time) | every acknowledged write survives (the OS keeps what was written) |
| `Graceful` (8) | `stop`: the workload stops, the DB is disposed; the worker must exit 0 | every acknowledged write survives |
| `PowerLoss` (12) | LazyFS-style `clear-cache` on the in-process fault file system, then `_exit` | every write acknowledged as synced (a sync write, or acked before a successful `SyncWal`/`FlushWal(true)`/sync write started) survives; each writer's surviving writes form a prefix of its ops (no holes); no phantoms; open must not fail |
| `TornOp` (7) | LazyFS `torn-op`: one unsynced write to a WAL, SST or blob file is split into 2..4 parts and some parts persist (zeros elsewhere), all other unsynced data is lost | as `PowerLoss` |
| `TornSeq` (7) | LazyFS `torn-seq`: of the unsynced writes to one WAL, SST or blob file a non-prefix subset persists (e.g. writes 1 and 4, not 2 and 3; gaps read as zeros: writeback reordering); everything else unsynced is lost | as `PowerLoss`; a hole in the WAL must stop point-in-time recovery, not resurrect later records |
| `FsyncTrigger` (8) | `clear-cache` right after the N-th fsync of a WAL (N 1..39), MANIFEST or SST file (N 1..11) completes, before the engine sees it return | as `PowerLoss` |
| `RecoveryKill` (8) | SIGKILL; then a second worker is started and SIGKILLed after a random delay in [0, 1.5 x the last open time + 100 ms] (during or right after recovery); a third process verifies | every acknowledged write survives |
| `Pause` (6) | SIGSTOP / SIGCONT for 0.1..5 s every 0.3..3 s during the RUN phase, then a clean stop or SIGKILL | every acknowledged write survives; online checks keep holding after each pause |
| `IoError` (8) | the fault file system fails appends, fsyncs (and directory fsyncs, creates, renames, deletes) and reads at random (200..5000 per million) for a third of the RUN phase, with one error kind per cycle: plain IOError, retryable IOError or NoSpace; then it stops failing and the worker calls `Resume`; then a clean stop or SIGKILL. As upstream, a plain IOError in a flush or WAL write is a fatal background error that `Resume` refuses until a reopen, while retryable and NoSpace errors recover (counted as `resume-ok`) | failed writes are `:info` (or `:fail` for a transaction conflict), never `:ok`; every acknowledged write survives; a close that fails after unrecovered background errors is counted (`close-failed-after-io-errors`), not a failure |
| `LazyFsClear` (0; only with LazyFS) | SIGKILL, then `lazyfs::clear-cache` through the LazyFS FIFO (see "Real LazyFS mode") | as `PowerLoss` |

### The in-process fault file system

`PowerLoss`, `TornOp`, `TornSeq`, `FsyncTrigger` and `IoError` cycles run the worker on `LazyFaultFileSystem`
(`FaultFs.cs`), an in-process equivalent of LazyFS and of upstream's `FaultInjectionTestFS` over the real
`LocalFileSystem`. It tracks the files directly in the DB directory:

- per file, the length covered by the last successful fsync and the unsynced writes since then (offset, length); for
  overwrites of synced data (WAL recycling) the old bytes are saved;
- the directory listing as of the last directory fsync. Deleted, renamed-over or truncated files that the durable
  listing still names are kept (hard link or copy in a graveyard) so they can come back.

On a crash command the worker first freezes the history (no ack is recorded after this point), then the file system
waits for in-flight operations and blocks new ones, rewrites the directory to what the fault leaves (entries created
since the last directory fsync disappear, deleted ones come back, every file is cut to its synced length, the torn
parts are written back), and the process ends with `_exit(77)`. Files that existed when the worker started count as
durable (we do not model a power loss across cycles). The history is on the real file system and is never subject to
the injected loss.

For `TornOp` / `TornSeq` the worker first waits (up to 3 s) until a WAL file with a durable directory entry has
unsynced writes (1, or 3 for torn-seq), like picking LazyFS's `occurrence`; the victim is a WAL file 3 times in 4
when one qualifies. If no file qualifies the fault degrades to clear-cache (counted as `torn-no-candidate`).

Torn writes are applied to WAL, SST and blob files only. A torn MANIFEST tail is a corrupted record to upstream as
well (`VersionSet::Recover` fails with Corruption), so the MANIFEST gets clear-cache only. Unsynced SST and blob files
are not referenced yet, so tearing them must be harmless; tearing the WAL is the interesting case.

## VERIFY steps

Each step is reported separately (pass/fail, count, time) in `verify/cycle-N.json`, `soak.log` and `status.json`.

| # | Step | Checks |
|---|---|---|
| 1 | `open` | the DB opens (recovery time; after `RecoveryKill` the note says the previous open was killed) |
| 2 | `durability` | registers and range-delete keys: some cut (prefix of each writer's ops) explains the state and reaches the last op that must have survived (all acked ops after a kill or clean stop; acked ops covered by a sync after a power loss). Reports the lost acked writes |
| 3 | `registers` | every value decodes, its payload checksum and its deterministic payload match; every observed version was written (no phantoms) with the same length and checksum; one prefix of each writer's ops explains every key of that writer, across both column families (no holes) |
| 4 | `bank` | all accounts present, total constant |
| 5 | `counters` | each counter within [model + increments that must have survived, model + all non-failed increments] |
| 6 | `range-deletes` | as registers, for the `rdel` key space (a `DeleteRange` is a delete of every key in its range) |
| 7 | `scans` | full forward and reverse scan of every column family with `VerifyChecksums = true`: strictly sorted, unique, reverse = forward, every entry equal to the point read, per column family counts equal to the point reads |
| 8 | `integrity` | after `WaitForCompact`: `VerifyFileChecksums` (whole-file crc32c of every live table and blob file), live-file metadata against the directory (exists, size equals the MANIFEST size, level, key order), `rocksdb.num-files-at-level<N>` sums equal the live files, `rocksdb.estimate-num-keys` available |
| 9 | `native-cross-check` | every `--native-every` cycles (default 10) and at the end: checkpoint, then native librocksdb (rocksdb-sharp, read-only, in a child process) scans every column family; per column family entry count and xxHash64 of the scan must equal the managed scan |
| 10 | `close` | the DB closes cleanly |

On success the observation becomes the model (Jepsen-style resolution of the `:info` ops: they did or did not happen,
whichever the verifier saw). The model is a fixed-size snapshot of every key's state plus each writer's next version.

## File corruption (Jepsen's file nemesis)

Every `--corrupt-every` cycles (default 3), after a successful VERIFY, the stopped DB is copied and one file of the
copy is damaged like `jepsen.nemesis.file` / `corrupt-file.c` does: `bitflip` (a 512..1024 byte region, each bit
flipped with probability 1e-3 or 1e-2), `truncate` (to a random length, or trimming 1..200 bytes off the tail),
`copy` (a 16 B..4 KiB chunk copied to another offset of the same file) and `restore` (a chunk snapshotted from an
earlier copy written back: a lost write, or a misdirected one when it comes from another file of the same kind). The
target is a random SST, WAL, MANIFEST, blob, OPTIONS or CURRENT file. The soak DB itself is never touched.

A `corrupt-check` child (3 min timeout) opens the copy and classifies the outcome ("detect or be correct"):

| Outcome | Meaning | Verdict |
|---|---|---|
| `detected-open` | open failed with a `RocksDbException` | ok |
| `detected-read` | a read or scan threw Corruption / IOError | ok |
| `detected-verify` | all reads correct, `VerifyFileChecksums` or the metadata check reported the damage | ok |
| `correct` | every read returns exactly the verified state | ok |
| `wal-suffix-loss` | a damaged WAL lost a suffix: the state is a prefix of the cycle's history (checked like a power loss without durability) | ok, counted separately |
| `silent-wrong-same-as-native` | wrong data without an error, and native librocksdb reads the same damaged bytes the same way | upstream behaviour, logged and counted |
| `silent-wrong` | wrong data, a phantom or a lost value without an error (and native differs) | FAILURE |
| `hang` / `crash` | the check timed out, or ended with anything but a classified result (e.g. a non-`RocksDbException`) | FAILURE |

Counts by `target/mode/outcome` are in `status.json` (`Corruption`) and in the report. A failing copy is kept in
`failures/cycle-N-corrupt/` with a description next to it.

## Jepsen nemeses and their equivalents here

| Jepsen | Here | Guarantee checked |
|---|---|---|
| `kill` (process crash, `kill -9`) | `Kill`, `RecoveryKill` | acked writes survive; open succeeds; crash during recovery is recoverable |
| `pause` (SIGSTOP / SIGCONT) | `Pause` | no lost or stale reads across the pause; acked writes survive |
| LazyFS `lose-unfsynced-writes!` / `clear-cache` | `PowerLoss`; `LazyFsClear` with a real LazyFS | synced acks survive, per-writer prefix, no phantoms, open succeeds |
| LazyFS `torn-op` | `TornOp` | as above, with a torn unsynced write |
| LazyFS `torn-seq` | `TornSeq` | as above; a WAL hole stops point-in-time recovery |
| LazyFS clear-cache at the N-th fsync (`op = "fsync"`, `occurrence`) | `FsyncTrigger` | crash right after an fsync of a WAL, MANIFEST or SST file |
| `jepsen.nemesis.file` bitflip / truncate / copy / snapshot+restore | the corruption trial | detect or be correct |
| disk faults (I/O errors, e.g. via `charybdefs`) | `IoError` | failed ops are never `:ok`; background errors and `Resume`; acked writes survive |
| clean restart | `Graceful` | clean close and reopen lose nothing |

Not applicable to an embedded single-process engine: network partitions, membership changes and bridge/ring
topologies (there is no network; the replication layer has its own partition tests in
`tests/RocksDb.Managed.Replication.Tests`), and clock skew (the workload uses no TTL, timestamps or time-based
compaction, so wall-clock jumps cannot change the result).

## Running for days

Disk: the DB stays small (bounded key spaces: about 10..50 MB with the defaults). The histories take about
50..60 MB per minute of RUN time with the defaults (every write, sync and transfer, and one in eight reads, is
recorded); only the last `--keep-history` cycles (default 20, about 40 s each on average) are kept, so plan for about
1..2 GB in the run directory plus the failure copies (each about the DB size plus one history). Memory: about 300 MB
per worker.

```bash
# in tmux / screen
tmux new -s soak 'cd dotnet && tools/soak.sh --dir /data/soak --duration 72h'

# or detached
cd dotnet && nohup tools/soak.sh --dir /data/soak --duration 72h > /data/soak.out 2>&1 &

# or as a transient systemd unit (logs in the journal)
systemd-run --user --unit rocksdb-soak --working-directory "$PWD/dotnet" tools/soak.sh --dir /data/soak --duration 72h
journalctl --user -fu rocksdb-soak
```

Stop with Ctrl-C or `kill -TERM <supervisor pid>` (`systemctl --user stop rocksdb-soak`): the supervisor finishes the
current phase (the RUN phase ends early with its fault, VERIFY completes), writes `status.json` and exits. Running the
same command again resumes the run from the last folded cycle (`run.json` and `model.bin` are kept; only
`--duration` applies, counted from the restart).

Options: `--seed N`, `--mean-run S`, `--min-run S`, `--max-run S`, `--native-every N`, `--corrupt-every N`,
`--keep-history N`, `--writers N`, `--keys-per-writer N`, and `--faults Kill=30,PowerLoss=12,...` (weights; 0 disables
a fault).

## Results

The run directory holds:

| Path | Contents |
|---|---|
| `status.json` | machine-readable state, rewritten after every phase |
| `soak.log` | human log: every phase, every step result, every crash and corruption trial |
| `report` command | the summary below |
| `verify/cycle-N.json` | the verifier's report: steps, op counts by type and outcome, per column family digests |
| `history/cycle-N-pK.bin` | the histories of the last cycles (K = worker process of the cycle) |
| `logs/cycle-N-*.log` | worker, verifier and corruption-check output |
| `failures/cycle-N/` | everything about a failed cycle (see below) |
| `run.json`, `model.bin`, `cycle.json` | configuration, the model, the current cycle's plan |

`status.json` fields: `Phase` (`init`, `run`, `verify`, `corrupt`, `folded`, `done`, `failed`), `Cycle`, `Fault`,
`Seed`, `ElapsedSeconds`, `DurationSeconds`, `CyclesCompleted`, `Faults` (cycles per fault), `Ops` (`Type.outcome` ->
count, summed over the verified histories), `Notes` (e.g. `fsync-trigger-wal`, `fsync-trigger-not-reached`,
`torn-seq-applied`, `torn-op-applied`, `torn-no-candidate`, `recovery-kill-during-open` / `-after-open`, `pauses`,
`io-error-kind-plain|retryable|nospace`, `io-errors-injected`, `resume-ok`, `resume-attempts-failed`,
`close-failed-after-io-errors`), `Corruption` (`target/mode/outcome` -> count),
`NativeChecks`, `LastVerify` (cycle, pass, each step), `Failures`, `Result` (`running`, `passed`, `failed`).

`tools/soak.sh report --dir /data/soak` prints the result, the faults per type, the operations per type and outcome
(ok / fail / info), the corruption outcomes, the verify step timings (runs, failures, average and maximum time) and
the last verification. It exits non-zero if the run has a failure.

## Reproducing a failure

On a failure the supervisor stops and copies, before anything else touches them, the DB directory, the cycle's
history, `model.bin`, `run.json`, `cycle.json`, the verify report and the logs into `failures/cycle-N/` (the same
layout as a run directory), writes `summary.txt` (the reason, the seed, the cycle seed, the fault and the options) and
exits with code 1. Then:

```bash
RocksDb.Managed.Soak verify --run-dir failures/cycle-N            # re-run the checks on the preserved state
RocksDb.Managed.Soak export-history --run-dir failures/cycle-N --cycle N --format edn --out failures/cycle-N/edn
tools/soak.sh --dir /tmp/replay --seed <seed> --duration 1h        # the same plan: faults, options, workload choices
```

The seed reproduces the plan (which fault, how long, which options, which keys and values), not the thread
interleaving or the exact kill moment.

## History format and independent checking (EDN)

Each worker writes `history/cycle-N-pK.bin`: one 48-byte record per event (phase, op type, thread, flags such as
sync/async, op id, key fields, version or amount, payload CRC and length, time in microseconds; bank reads carry the
balances). Records are written with one `write(2)` each, without user-space buffering, so they survive a SIGKILL; the
file order is a total order of events.

```bash
RocksDb.Managed.Soak export-history --run-dir /data/soak --cycle all --format edn --out /data/soak/edn
```

writes two Jepsen histories, one op map per line:

- `register.edn`: rw-register transactions, e.g. `{:index 7, :type :ok, :process 3001, :time 1200, :f :txn,
  :value [[:w 1503 812]]}`. Key = writer x keys-per-writer + key index, value = version; a delete is a write of the
  negated version. Reads come from the online reader threads; a read of an absent key is exported as `nil` while the
  key has not been deleted earlier in the export, and left out otherwise.
- `bank.edn`: `{:f :transfer, :value {:from 3, :to 7, :amount 34}}` and `{:f :read, :value {0 1005, 1 1011, ...}}`
  (balances as signed integers; they may go negative; the total is accounts x 1000).

Process ids are `cycle * 1000 + worker part * 100 + thread` (plus 1,000,000 per earlier `:info` op of the thread: a
process is never reused after `:info`), and every op left open by a killed worker gets an `:info`
completion, as Jepsen requires. Check them with [elle-cli](https://github.com/ligurio/elle-cli), e.g.
`java -jar elle-cli.jar --model rw-register --consistency-models strict-serializable register.edn` and
`--model bank bank.edn` (exporting all kept cycles gives the checker the writes that later reads observe; values
written in cycles older than `--keep-history` appear as unexplained reads).

## Self-test

`tools/soak.sh selftest` (about 1 minute) runs one real cycle (worker SIGKILLed after 6 s), then for each case copies
the state, injects one bug behind the model's back, runs the verifier, and expects the named step to fail:

| Case | Expected failing step |
|---|---|
| control: unmodified state | none (must pass) |
| an acknowledged register deleted | `durability` |
| a bit flipped in the largest SST | `integrity`, `scans` or `registers` (block checksum mismatch on read) |
| bank total broken (+1) | `bank` |
| counter rolled back below its acked increments | `counters` |
| the history claims an extra ack | `durability` |
| a phantom register version | `registers` |
| a silently corrupted value (payload byte changed, header kept) | `registers` |
| a range-delete key removed | `range-deletes` / `durability` |
| a mishandled torn-seq image (older write lost, newer one survived) | `registers` (hole) |
| corruption trial classifier: an unmodified copy, then a copy with a register rolled back to an older valid version | `correct`, then `silent-wrong` |
| control: power loss (clear-cache) on the fault file system | none (must pass) |
| control: torn-seq on the fault file system | none (must pass) |
| missing fsync (`SkipSync`: fsync makes nothing durable) + power loss | `durability` |

It exits non-zero if a case is not caught and keeps its state.

## CI smoke mode

`--smoke` sets a 2-minute duration, 1.5..10 s RUN phases, smaller key spaces, a native cross-check every 4 cycles
and a corruption trial every 2. It runs about 15..20 cycles with every fault type:

```bash
tools/soak.sh --dir "$(mktemp -d)" --smoke && tools/soak.sh selftest
```

It is a documented command, not a unit test, so it does not slow the existing suites.

## Real LazyFS mode (optional, for your own machine)

The in-process fault file system models the page cache inside the worker. A real
[LazyFS](https://github.com/dsrhaslab/lazyfs) mount also exercises the real `LocalFileSystem` fsync calls
(`fdatasync`, directory fsyncs) through the kernel. The supervisor implements it the way Jepsen's `jepsen.lazyfs`
does. It was run in the development container (see "LazyFS results" below).

Setup (Ubuntu):

```bash
sudo apt-get install -y libfuse3-dev fuse3 g++ cmake
git clone https://github.com/dsrhaslab/lazyfs.git && cd lazyfs
(cd libs/libpcache && ./build.sh) && (cd lazyfs && ./build.sh)      # binary: lazyfs/build/lazyfs
# If GitHub release tarballs are blocked (libpcache fetches spdlog as a tarball), use a git clone instead:
#   git clone --depth 1 -b v1.10.0 https://github.com/gabime/spdlog.git /path/to/spdlog
#   (cd libs/libpcache && mkdir -p build && cd build && cmake .. -DFETCHCONTENT_SOURCE_DIR_SPDLOG=/path/to/spdlog && cmake --build .)
#   (cd lazyfs && mkdir -p build && cd build && CXXFLAGS=-I/path/to/spdlog/include cmake -DLAZYFS_BUILD_TESTS=OFF .. && cmake --build .)
sudo sed -i '/\s*user_allow_other/s/^#//g' /etc/fuse.conf           # allow_other, as Jepsen does
```

Run:

```bash
tools/soak.sh --dir /data/soak-lazyfs --duration 24h --lazyfs-bin /path/to/lazyfs/lazyfs/build/lazyfs
```

Layout: `<dir>/lazyfs/data` is the backing root on the real disk, `<dir>/db` is the LazyFS mount point (the DB lives
there), `<dir>/lazyfs/fifo` and `fifo-completed` are the fault FIFOs, `<dir>/lazyfs/config.toml` the config
(`fifo_path`, `fifo_path_completed`, a 1 GB simple cache without eviction) and `lazyfs.log` its log. The daemon runs
as `lazyfs <dir>/db --config-path config.toml -o allow_other -o modules=subdir -o subdir=<dir>/lazyfs/data -f`.

With LazyFS the worker runs on the plain `LocalFileSystem`. The in-process power-loss faults are replaced by
`LazyFsClear`: SIGKILL, then the supervisor writes `lazyfs::clear-cache` to the FIFO and waits for
`finished::clear-cache` on the completion FIFO, then VERIFY with the power-loss expectations. If you mount LazyFS
yourself, pass `--lazyfs-fifo <fifo> [--lazyfs-fifo-completed <fifo>]` instead of `--lazyfs-bin` and put `--dir` so
that `<dir>/db` is on the mount. LazyFS's own `torn-op` / `torn-seq` faults (configured in its TOML `[[injection]]`
sections, which crash LazyFS itself) are not driven by the supervisor.

Options: `--lazyfs-cache 1GB` (LazyFS `custom_size`), `--lazyfs-eviction` (`apply_eviction = true`: dirty pages are
written back when the cache is full, like a real page cache; off by default, as in Jepsen).

**LazyFS hangs.** LazyFS (checked at dsrhaslab/lazyfs d93e54a) can deadlock: `lfs_create` -> `lfs_getattr` ->
`put_data_blocks` holds the global inode lock and waits for a file's item lock, while `lfs_write` holds that item
lock and waits for the global lock. Every process touching the mount then sits in uninterruptible sleep. In the
container this happened after 10 to 15 minutes of soak. The supervisor therefore runs a watchdog: every 15 s it runs
`ls -f` (a readdir) on the mount (a `stat` of the mount point alone still answers during the deadlock); after 3 misses it kills
the daemon and aborts the FUSE connection, so the blocked calls fail with `ENOTCONN` and the phase ends. With
`--lazyfs-bin` the supervisor then remounts LazyFS and verifies the cycle again with power-loss expectations
(restarting LazyFS loses exactly its unsynced cache, i.e. a power loss; synced data was on the backing disk before
LazyFS acknowledged the fsync), counts it as fault `LazyFsHangRestart` and continues; the cycle's logs and
`lazyfs.log` are kept in `failures/cycle-N-lazyfs-hung/`. With `--lazyfs-fifo` (a mount you own) it stops with exit
code 3 and `status.json` result `infrastructure-failure`. A hang is never reported as a failure of the port.

**LazyFS results (development container, 2026-11-07).** A 20-minute run (seed 4242) verified 22 cycles with ~24M
operations: 14 `LazyFsClear`, 4 kills, 2 kills during recovery, 1 graceful shutdown, 3 native cross-checks and 7
corruption trials, no failures. An earlier run with the same configuration hit the LazyFS deadlock described above
after 13 cycles (before the watchdog existed); no deadlock occurred in the final run.

**Proving the checks bite.** `--unsafe-skip-fsync` (test only, LazyFS mode) makes the worker drop every fsync and
directory fsync, also on the real file system. The first `LazyFsClear` must then fail VERIFY. In the container it
failed at `open` (`Corruption: CURRENT file does not end with newline`: the never-synced CURRENT was dropped, as on
a real power loss), otherwise `durability` catches the lost synced writes:

```bash
tools/soak.sh --dir /tmp/soak-skip --duration 5m --lazyfs-bin /path/to/lazyfs --unsafe-skip-fsync   # must exit 1
```

Cleanup: on exit (end of the duration, Ctrl-C, SIGTERM, or a failure, after the failure copy) the supervisor drops
the cache (`clear-cache`), unmounts with `fusermount3 -uz <dir>/db` and waits for the daemon. After a hard kill of the
supervisor, run `fusermount3 -uz <dir>/db` yourself and check `pgrep lazyfs`.

## What it has found

- Engine: with several column families a memtable switch synced the closed WAL but not the WAL directory
  (upstream's `SyncClosedWals` fsyncs it when it was not synced since the WAL was created). With a separate `wal_dir`
  a power loss could drop the closed WAL while the flushed column family kept its half of a cross-column-family
  batch. The fault file system's diagnostics showed a synced WAL without a durable directory entry; fixed, with the
  regression test `DbCrashConsistencyTests.MemtableSwitchMakesTheClosedWalDurableIncludingItsDirectoryEntry`.
- Usage: optimistic transactions with `ValidateParallel` lose plain (non-transactional) writes to the same keys that
  land between validation and commit, as upstream does (see "The workload" above).

## Limitations

- Power loss is simulated inside the worker process for the files directly in the DB directory; the real file system
  is not power-cycled (use the LazyFS mode for that). File metadata other than names and sizes (permissions, times)
  is not modelled.
- Torn writes are not applied to the MANIFEST (see above).
- The DB uses the default `wal_dir` (the DB directory); the fault file system tracks one directory, so a separate WAL
  directory (where the bug above showed) is covered by the unit test, not by the soak.
- Pessimistic `TransactionDatabase` and WritePrepared transactions are not part of the workload (they are covered by
  the unit, round-trip and 2PC round-trip suites); only optimistic transactions are.
- The native cross-check reads; native RocksDB does not write to the soak DB.
