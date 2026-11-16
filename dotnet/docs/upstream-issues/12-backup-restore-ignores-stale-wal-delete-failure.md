# BackupEngine restore reports success when it cannot delete a stale WAL in the destination; the next open replays it

**Suggested title:** `RestoreDBFromBackup: DeleteChildren ignores DeleteFile errors, so a stale WAL left in the destination is replayed into the restored DB`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

`RestoreDBFromBackup` (with the default `keep_log_files = false`) either leaves the destination holding exactly the
backup's DB, or fails. If a file of the old DB in the destination cannot be deleted, the restore returns that error.

### Actual behavior

The restore returns OK, the old DB's WAL stays in the destination, and the next `DB::Open` replays it on top of the
restored files: the restored DB contains writes that were never in the backup (here `k0 = stale` instead of the backed
up `k0 = v0`, plus a key the backup never had). Nothing reports it.

Cause, in `BackupEngineImpl::DeleteChildren` (`utilities/backup/backup_engine.cc`), which `RestoreDBFromBackup` calls for
the WAL directory, its archive and the DB directory before copying:

```cpp
    db_fs_->DeleteFile(dir + "/" + f, io_options_, nullptr)
        .PermitUncheckedError();  // ignore errors
```

(`GetChildren` errors are ignored the same way.) The WAL is replayed because its number is above the restored
MANIFEST's log number, as it is whenever the destination DB lived longer than the backed-up one. The `keep_log_files`
branch of the same function already treats the analogous failure as fatal: "if we can't move log file from archive_dir
to wal_dir, we should fail, since it might mean data loss".

### Steps to reproduce

The program below uses only the C API. It backs up a DB holding `k0 = v0`, creates another DB at the destination whose
live WAL holds `k0 = stale` and `only-in-stale-wal = x` (20 flushes first, so the WAL number is above the backup's),
makes that WAL undeletable with `chattr +i` and restores the backup over the destination. `chattr +i` needs root and a
file system that supports it (ext2/3/4, xfs, btrfs); it only stands in for any `DeleteFile` failure, whose status is
discarded the same way.

```c
/* A restore whose deletion of a stale WAL in the destination fails reports success; the next open replays that WAL. */
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)

static void print_key(rocksdb_t* db, const char* key) {
  char* err = NULL;
  size_t len = 0;
  rocksdb_readoptions_t* r = rocksdb_readoptions_create();
  char* v = rocksdb_get(db, r, key, strlen(key), &len, &err); CHECK(err);
  printf("  %s = %.*s\n", key, v ? (int)len : 9, v ? v : "NotFound");
  free(v);
  rocksdb_readoptions_destroy(r);
}

int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  char* err = NULL;
  system("chattr -i /tmp/up12/dest/*.log 2>/dev/null; rm -rf /tmp/up12 && mkdir -p /tmp/up12");
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  rocksdb_flushoptions_t* f = rocksdb_flushoptions_create();

  /* The source DB: k0 = v0, backed up (with a flush). */
  rocksdb_t* db = rocksdb_open(o, "/tmp/up12/src", &err); CHECK(err);
  rocksdb_put(db, w, "k0", 2, "v0", 2, &err); CHECK(err);
  rocksdb_backup_engine_t* be = rocksdb_backup_engine_open(o, "/tmp/up12/backup", &err); CHECK(err);
  rocksdb_backup_engine_create_new_backup_flush(be, db, 1, &err); CHECK(err);
  rocksdb_close(db);

  /* The destination: another DB whose live WAL (numbered above the backup's files) holds unflushed writes. */
  db = rocksdb_open(o, "/tmp/up12/dest", &err); CHECK(err);
  for (int i = 0; i < 20; i++) {
    rocksdb_put(db, w, "filler", 6, "x", 1, &err); CHECK(err);
    rocksdb_flush(db, f, &err); CHECK(err);
  }
  rocksdb_put(db, w, "k0", 2, "stale", 5, &err); CHECK(err);
  rocksdb_put(db, w, "only-in-stale-wal", 17, "x", 1, &err); CHECK(err);
  rocksdb_close(db);

  /* Make that WAL undeletable (needs root and ext2/3/4, xfs or btrfs), then restore the backup over the DB. */
  if (system("chattr +i /tmp/up12/dest/*.log") != 0) { fprintf(stderr, "chattr +i failed\n"); return 1; }
  rocksdb_restore_options_t* ro = rocksdb_restore_options_create();
  rocksdb_backup_engine_restore_db_from_latest_backup(be, "/tmp/up12/dest", "/tmp/up12/dest", ro, &err);
  printf("restore: %s\n", err ? err : "OK");
  CHECK(err);
  rocksdb_backup_engine_close(be);
  printf("files in the destination after the restore:\n");
  system("ls /tmp/up12/dest | sed 's/^/  /'");

  rocksdb_options_set_create_if_missing(o, 0);
  db = rocksdb_open(o, "/tmp/up12/dest", &err); CHECK(err);
  printf("open: OK\n");
  print_key(db, "k0");
  print_key(db, "only-in-stale-wal");
  rocksdb_close(db);
  system("chattr -i /tmp/up12/dest/*.log");
  return 0;
}
```

Build and run it on Linux as root (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=gnu11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385, ext4):

```
restore: OK
files in the destination after the restore:
  000008.log
  000009.sst
  000049.log
  CURRENT
  MANIFEST-000005
  OPTIONS-000007
open: OK
  k0 = stale
  only-in-stale-wal = x
```

`000049.log` is the destination DB's old WAL; the backup holds `000008.log`, `000009.sst` and `MANIFEST-000005`.

### Suggested fix

Return the first `DeleteFile` error (ignoring NotFound, for a file removed concurrently) from `DeleteChildren`, and fail
the restore with it before copying anything. If best-effort deletion is wanted for some files, it should at least not
apply to WAL files, which the next open replays.

The managed port (RocksDb.Managed) behaves like native here (compat test
`RestoreOverAnUndeletableStaleWalReplaysItNativelyAndInThePort`).
