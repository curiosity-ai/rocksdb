# BackupEngine garbage collection deletes the data of a backup whose meta file it cannot load

**Suggested title:** `BackupEngine: PurgeOldBackups / GarbageCollect delete the private and shared files of a backup in corrupt_backups_`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64). The code involved is unchanged on `main` (checked
at `02815ee01c2548afc52b82df26af460e28d1ee22`).

### Expected behavior

A backup that this version of the engine cannot load - its meta file names a schema version it does not know (a backup
written by a newer RocksDB), or the meta file is damaged in a way an administrator could repair - is reported by
`GetCorruptedBackups` and left alone until the application deletes it with `DeleteBackup(id)`. A `PurgeOldBackups(n)`
that has nothing to purge deletes nothing.

### Actual behavior

The first `PurgeOldBackups` (or `DeleteBackup`, or explicit `GarbageCollect`) of a writable engine deletes that backup's
private directory and every shared file only it referenced. Its meta file stays, so the backup is still listed as
corrupt, but its data is gone: an older engine opening a backup directory written by a newer one destroys the newer
backups.

Cause, in `utilities/backup/backup_engine.cc`:

- `Initialize` moves a backup whose `LoadFromFile` returns Corruption or NotSupported into `corrupt_backups_`. Its
  files are never added to `backuped_file_infos_`, so they have no reference counts.
- `GarbageCollect` deletes every file under `shared/` and `shared_checksum/` that has no reference count, and every
  `private/<id>` directory whose id is not in `backups_` - which a backup in `corrupt_backups_` is not.

The test in `backup_engine_test.cc` that opens an engine with corrupt backups says "make sure that no corrupt backups
have actually been deleted", but does not run a garbage collection at that point.

A possible fix: while a backup is in `corrupt_backups_`, keep every path its meta file names (the first word of each
file line can be read without understanding the rest of the file) and its `private/<id>` directory out of garbage
collection, and delete them only in `DeleteBackup(id)`. The managed port does this; a meta file too damaged to yield
file names stays unprotected there.

### Steps to reproduce the behavior

The program below uses only the C API. It creates two backups, prepends `schema_version 9` to `meta/2` (how a future
schema version looks to this engine) and calls `rocksdb_backup_engine_purge_old_backups(be, 10)`, which has nothing to
purge.

```c
/* A backup whose meta file this version cannot load is destroyed by a PurgeOldBackups that deletes nothing. */
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
  system("rm -rf /tmp/up05 && mkdir -p /tmp/up05");
  make_db_and_two_backups("/tmp/up05/db", "/tmp/up05/backup");

  /* Backup 2 as a newer RocksDB would write it: a schema version this one does not know. */
  system("{ echo 'schema_version 9'; cat /tmp/up05/backup/meta/2; } > /tmp/up05/m && mv /tmp/up05/m /tmp/up05/backup/meta/2");
  printf("before: private/2 has %d files, shared_checksum has %d files\n",
         count_entries("/tmp/up05/backup/private/2"), count_entries("/tmp/up05/backup/shared_checksum"));

  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_backup_engine_t* be = rocksdb_backup_engine_open(o, "/tmp/up05/backup", &err); CHECK(err);
  const rocksdb_backup_engine_info_t* info = rocksdb_backup_engine_get_backup_info(be);
  printf("backups the engine lists: %d\n", rocksdb_backup_engine_info_count(info));
  rocksdb_backup_engine_info_destroy(info);
  rocksdb_backup_engine_purge_old_backups(be, 10, &err); CHECK(err);   /* keep up to 10: nothing to purge */
  rocksdb_backup_engine_close(be);

  printf("after:  private/2 has %d files (-1 = directory gone), shared_checksum has %d files\n",
         count_entries("/tmp/up05/backup/private/2"), count_entries("/tmp/up05/backup/shared_checksum"));
  printf("meta/2 still exists: %s\n", fopen("/tmp/up05/backup/meta/2", "r") ? "yes" : "no");
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=c11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
before: private/2 has 4 files, shared_checksum has 2 files
backups the engine lists: 1
after:  private/2 has -1 files (-1 = directory gone), shared_checksum has 1 files
meta/2 still exists: yes
```

Backup 2's private directory and its own table file are gone although nothing was asked to be deleted.
