# BackupEngine garbage collection of `private/<id>.tmp` deletes the live backup `<id>`

**Suggested title:** `BackupEngine::GarbageCollect: a private/<id>.tmp directory makes it delete private/<id>/ instead`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64). The code involved is unchanged on `main` (checked
at `02815ee01c2548afc52b82df26af460e28d1ee22`).

### Expected behavior

`GarbageCollect` (also run by the first `PurgeOldBackups` / `DeleteBackup` of a writable engine) removes a leftover
temporary directory `private/<id>.tmp` and leaves backup `<id>` intact.

### Actual behavior

It deletes the files of `private/<id>/` and that directory, and leaves `private/<id>.tmp` in place. Backup `<id>` is
then broken (`VerifyBackup`: "File missing").

Cause, in `BackupEngineImpl::GarbageCollect` (`utilities/backup/backup_engine.cc`):

```cpp
for (auto& child : private_children) {
  BackupID backup_id = 0;
  bool tmp_dir = child.find(".tmp") != std::string::npos;
  sscanf(child.c_str(), "%u", &backup_id);
  if (!tmp_dir &&  // if it's tmp_dir, delete it
      (backup_id == 0 || backups_.find(backup_id) != backups_.end())) {
    // it's either not a number or it's still alive. continue
    continue;
  }
  // here we have to delete the dir and all its children
  std::string full_private_path =
      GetAbsolutePath(GetPrivateFileRel(backup_id));
```

For `child == "2.tmp"`, `sscanf` yields `2`, and `GetPrivateFileRel(backup_id)` (its `tmp` argument defaults to
`false`) is `private/2/`, the live backup. The path should be built from `child` itself. `child.find(".tmp")` also
matches any name that merely contains `.tmp`.

The current engine does not create `private/<id>.tmp` directories itself, so this needs one left by an older version or
by another tool; what it deletes is a backup the engine reports as valid.

### Steps to reproduce the behavior

The program below uses only the C API. It creates two backups, creates an empty directory `private/2.tmp` and calls
`rocksdb_backup_engine_purge_old_backups(be, 10)`, which has nothing to purge.

```c
/* A directory private/<id>.tmp makes garbage collection empty private/<id>, the live backup <id>. */
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
  system("rm -rf /tmp/up06 && mkdir -p /tmp/up06");
  make_db_and_two_backups("/tmp/up06/db", "/tmp/up06/backup");
  system("mkdir /tmp/up06/backup/private/2.tmp");   /* e.g. left behind by an older version or a copy tool */
  printf("before: private/2 has %d files\n", count_entries("/tmp/up06/backup/private/2"));

  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_backup_engine_t* be = rocksdb_backup_engine_open(o, "/tmp/up06/backup", &err); CHECK(err);
  rocksdb_backup_engine_purge_old_backups(be, 10, &err); CHECK(err);   /* nothing to purge; runs garbage collection */
  printf("after:  private/2 has %d files (-1 = directory gone), private/2.tmp has %d\n",
         count_entries("/tmp/up06/backup/private/2"), count_entries("/tmp/up06/backup/private/2.tmp"));
  rocksdb_backup_engine_verify_backup(be, 2, &err);
  printf("verify backup 2: %s\n", err ? err : "OK");
  rocksdb_backup_engine_close(be);
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=c11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
before: private/2 has 4 files
after:  private/2 has -1 files (-1 = directory gone), private/2.tmp has 0
verify backup 2: NotFound: File missing: /tmp/up06/backup/private/2/MANIFEST-000005
```
