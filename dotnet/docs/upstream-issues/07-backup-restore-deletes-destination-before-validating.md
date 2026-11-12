# BackupEngine restore deletes the destination DB before finding that the backup cannot be restored

**Suggested title:** `RestoreDBFromBackup: the destination is emptied before the backup's file names are parsed, so a failing restore destroys it`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64). The code involved is unchanged on `main` (checked
at `02815ee01c2548afc52b82df26af460e28d1ee22`).

### Expected behavior

A restore that is bound to fail because of the backup's own contents - here, a meta file listing a name that is not a
DB file name - fails before it modifies the destination, so the database that was there can still be opened.

### Actual behavior

The restore first deletes the files of the destination DB directory (and of the WAL and archive directories), then
parses the backup's file names one by one and fails with Corruption at the unparsable one. The destination is left with
the files copied before the failure and a `CURRENT.tmp` but no `CURRENT`: it is neither the old database nor the
restored one, and it does not open.

Cause, in `BackupEngineImpl::RestoreDBFromBackup` (`utilities/backup/backup_engine.cc`): the `DeleteChildren` calls come
before the copy loop, which only then does

```cpp
bool ok = ParseFileName(dst, &number, &type);
if (!ok) {
  return IOStatus::Corruption("Backup corrupted: Fail to parse filename " +
                              dst);
}
```

Parsing every name (and building every destination path) before the first deletion would make this failure harmless.
Related, found by reading only: `GetFileFromChecksumFile` erases `file_copy.find_last_of('.') - first_underscore`
characters, which wraps around for a `shared_checksum` name without a `.` after its `_`.

### Steps to reproduce the behavior

The program below uses only the C API. It creates two backups, adds a file `private/2/README` to backup 2's meta file
(file count 6 -> 7), creates an unrelated database (holding the key `precious`) at the destination and restores backup
2 over it.

```c
/* A restore that cannot succeed (a backup listing a name that is not a DB file) first deletes the destination DB. */
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)

/* Creates db_dir with 100 keys, flushes, backs it up to backup_dir; then 100 more keys and a second backup. */
static void make_db_and_two_backups(const char* db_dir, const char* backup_dir) {
  char* err = NULL;
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_t* db = rocksdb_open(o, db_dir, &err); CHECK(err);
  rocksdb_backup_engine_t* be = rocksdb_backup_engine_open(o, backup_dir, &err); CHECK(err);
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  char k[32], v[32];
  for (int round = 0; round < 2; round++) {
    for (int i = round * 100; i < (round + 1) * 100; i++) {
      snprintf(k, sizeof k, "k%06d", i); snprintf(v, sizeof v, "v%06d", i);
      rocksdb_put(db, w, k, strlen(k), v, strlen(v), &err); CHECK(err);
    }
    rocksdb_backup_engine_create_new_backup_flush(be, db, 1, &err); CHECK(err);
  }
  rocksdb_writeoptions_destroy(w);
  rocksdb_backup_engine_close(be);
  rocksdb_close(db);
  rocksdb_options_destroy(o);
}

static inline int count_entries(const char* dir) {
  DIR* d = opendir(dir); int n = 0; struct dirent* e;
  if (!d) return -1;
  while ((e = readdir(d))) if (strcmp(e->d_name, ".") && strcmp(e->d_name, "..")) n++;
  closedir(d); return n;
}
int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  char* err = NULL;
  system("rm -rf /tmp/up07 && mkdir -p /tmp/up07");
  make_db_and_two_backups("/tmp/up07/db", "/tmp/up07/backup");

  /* The destination: an unrelated, existing database. */
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_t* dest = rocksdb_open(o, "/tmp/up07/dest", &err); CHECK(err);
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  rocksdb_put(dest, w, "precious", 8, "data", 4, &err); CHECK(err);
  rocksdb_flushoptions_t* f = rocksdb_flushoptions_create();
  rocksdb_flush(dest, f, &err); CHECK(err);
  rocksdb_close(dest);

  /* Backup 2 lists one more private file whose name is not a DB file name (count line 6 -> 7). */
  system("touch /tmp/up07/backup/private/2/README && "
         "sed -i '3s/^6$/7/' /tmp/up07/backup/meta/2 && echo 'private/2/README crc32 0' >> /tmp/up07/backup/meta/2");
  printf("destination before: %d files\n", count_entries("/tmp/up07/dest"));

  rocksdb_backup_engine_t* be = rocksdb_backup_engine_open(o, "/tmp/up07/backup", &err); CHECK(err);
  rocksdb_restore_options_t* ro = rocksdb_restore_options_create();
  rocksdb_backup_engine_restore_db_from_backup(be, "/tmp/up07/dest", "/tmp/up07/dest", ro, 2, &err);
  printf("restore: %s\n", err ? err : "OK");
  free(err); err = NULL;
  rocksdb_backup_engine_close(be);

  printf("destination after: %d files\n", count_entries("/tmp/up07/dest"));
  system("ls /tmp/up07/dest");
  rocksdb_options_set_create_if_missing(o, 0);
  dest = rocksdb_open(o, "/tmp/up07/dest", &err);
  printf("open destination: %s\n", err ? err : "OK");
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=c11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
destination before: 8 files
restore: Corruption: Backup corrupted: Fail to parse filename README
destination after: 6 files
000009.sst
000010.log
000011.sst
CURRENT.tmp
MANIFEST-000005
OPTIONS-000007
open destination: Invalid argument: /tmp/up07/dest/CURRENT: does not exist (create_if_missing is false)
```

The database at the destination is gone and nothing was restored in its place.
