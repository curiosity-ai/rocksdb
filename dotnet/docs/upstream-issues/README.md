# Upstream RocksDB issues

These are bugs in native RocksDB found while porting and testing the managed engine (01-03 by the round-trip tests,
04-07 by audit #5, 08-13 by audit #7, 14 by audit #8, 15 by audit #10). Each file follows RocksDB's
`issue_template.md` (Expected behavior / Actual behavior / Steps to reproduce) and includes a self-contained program
(C API; C++ for 14) that reproduces the bug against the native library. Every program was run against the RocksDB 11.12.0 Linux
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
| [08-log-reader-recyclable-type-hang.md](08-log-reader-recyclable-type-hang.md) | One flipped record type byte (a recyclable type in a non-recycled log) makes `log::Reader::ReadRecord` loop forever; `DB::Open` never returns |
| [09-lz4-uncompressed-size-0xffffffff-accepted.md](09-lz4-uncompressed-size-0xffffffff-accepted.md) | An LZ4 block with a stored uncompressed size of 0xFFFFFFFF is accepted (int -1 == LZ4's error -1); 4 GiB of uninitialized memory is read as the block (NotFound or SIGSEGV) |
| [10-block-handle-huge-size-aborts-process.md](10-block-handle-huge-size-aborts-process.md) | A damaged table footer with a 2^40 block size aborts the process (`std::bad_alloc`), one whose size + trailer wraps segfaults; reached by `IngestExternalFile` and `DB::Open` |
| [11-timedput-short-value-aborts-process.md](11-timedput-short-value-aborts-process.md) | A TimedPut WriteBatch record with a value shorter than 8 bytes aborts the process (`std::length_error`); the batch is already in the WAL, so every later open aborts too |
| [12-backup-restore-ignores-stale-wal-delete-failure.md](12-backup-restore-ignores-stale-wal-delete-failure.md) | A restore that cannot delete a stale WAL in the destination reports success; the next open replays writes that were never in the backup |
| [13-log-reader-drops-records-without-reporting.md](13-log-reader-drops-records-without-reporting.md) | A zeroed WAL record header mid-log, or a CRC-valid zstd WAL record that fails to decompress, drops records without any error in every recovery mode, `kAbsoluteConsistency` included |
| [14-user-write-callback-not-exception-safe.md](14-user-write-callback-not-exception-safe.md) | A `UserWriteCallback` that throws leaves its writer linked in the write queue; every later write hangs |
| [15-reverse-iteration-merge-without-operator-segfaults.md](15-reverse-iteration-merge-without-operator-segfaults.md) | A DB with merge operands in its table files, opened without a merge operator: reverse iteration over an operand, and `Get` / `MultiGet` of an operand above an older value, segfault (only `assert(merge_operator_ != nullptr)`); forward iteration fails with InvalidArgument |

01-03, 05-07, 09-13 and 15 were reproduced with the C API programs they contain, 08 with the NuGet package's library
in a child process (validation.md, "Audit #7"), 14 with the ABI-independent C++ variant it contains (the plain C++ program
needs a library built from the same headers); 04 concerns a feature the released library does not have yet. 09-15 were
checked unchanged on `main` at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`.

How the managed port handles each one is described in [validation.md](../validation.md), "Combinatorial round trips"
(01-07), "Audit #7" and "Audit #8" (08-14), and at the end of each draft.
