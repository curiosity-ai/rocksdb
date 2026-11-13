# Upstream RocksDB issues

These are bugs in native RocksDB found while porting and testing the managed engine (01-03 by the round-trip tests, 04-07 by audit #5). Each file follows RocksDB's
`issue_template.md` (Expected behavior / Actual behavior / Steps to reproduce) and includes a self-contained C API
program that reproduces the bug against the native library. Every program was run against the RocksDB 11.12.0 Linux
x86-64 library from the `RocksDB` NuGet package (11.12.0.7385). The code involved was checked unchanged on
facebook/rocksdb `main`.

| File | Summary |
|---|---|
| [01-unordered-write-manual-wal-flush-deadlock.md](01-unordered-write-manual-wal-flush-deadlock.md) | `unordered_write` + `manual_wal_flush`: the first `Put` self-deadlocks on `wal_write_mutex_` |
| [02-dbiter-blob-base-value-merge-use-after-free.md](02-dbiter-blob-base-value-merge-use-after-free.md) | An iterator returns freed memory when a merge result is the blob base value (`max` operator + blob files) |
| [03-hash-index-hash-skiplist-out-of-domain-keys.md](03-hash-index-hash-skiplist-out-of-domain-keys.md) | The hash index and hash skip list call `Transform` on out-of-domain keys, so `Get` misses existing keys |
| [04-blob-direct-write-sealed-file-deleted-after-crash.md](04-blob-direct-write-sealed-file-deleted-after-crash.md) | Blob direct write (`main` only): blob files sealed before a crash are purged on the next open and their keys fail. **Found by reading and in the port, not reproduced natively; the reproduction is a sketch** |
| [05-backup-gc-deletes-backups-it-cannot-load.md](05-backup-gc-deletes-backups-it-cannot-load.md) | BackupEngine garbage collection deletes the private and shared files of a backup whose meta file it cannot load (e.g. written by a newer version) |
| [06-backup-gc-tmp-dir-empties-live-backup.md](06-backup-gc-tmp-dir-empties-live-backup.md) | BackupEngine garbage collection of a `private/<id>.tmp` directory deletes `private/<id>/`, the live backup |
| [07-backup-restore-deletes-destination-before-validating.md](07-backup-restore-deletes-destination-before-validating.md) | A restore that fails on an unparsable file name has already deleted the destination database |

01-03 and 05-07 were reproduced with the C API programs they contain; 04 concerns a feature the released library does not have yet.

How the managed port handles each one is described in [validation.md](../validation.md), "Combinatorial round trips".
