# Upstream RocksDB issues

These are bugs in native RocksDB that the managed port's round-trip tests found. Each file follows RocksDB's
`issue_template.md` (Expected behavior / Actual behavior / Steps to reproduce) and includes a self-contained C API
program that reproduces the bug against the native library. Every program was run against the RocksDB 11.12.0 Linux
x86-64 library from the `RocksDB` NuGet package (11.12.0.7385). The code involved was checked unchanged on
facebook/rocksdb `main`.

| File | Summary |
|---|---|
| [01-unordered-write-manual-wal-flush-deadlock.md](01-unordered-write-manual-wal-flush-deadlock.md) | `unordered_write` + `manual_wal_flush`: the first `Put` self-deadlocks on `wal_write_mutex_` |
| [02-dbiter-blob-base-value-merge-use-after-free.md](02-dbiter-blob-base-value-merge-use-after-free.md) | An iterator returns freed memory when a merge result is the blob base value (`max` operator + blob files) |
| [03-hash-index-hash-skiplist-out-of-domain-keys.md](03-hash-index-hash-skiplist-out-of-domain-keys.md) | The hash index and hash skip list call `Transform` on out-of-domain keys, so `Get` misses existing keys |

How the managed port handles each one is described in [validation.md](../validation.md), "Combinatorial round trips".
