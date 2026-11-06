# unordered_write=true with manual_wal_flush=true deadlocks on the first write

**Suggested title:** `unordered_write + manual_wal_flush: first Put self-deadlocks on wal_write_mutex_`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64). The code involved is unchanged on `main` (checked at
`ee13e2a64042d4952efe46dfe90dd3bef11cd351`).

### Expected behavior

With `DBOptions::unordered_write = true` and `DBOptions::manual_wal_flush = true` (and `two_write_queues = false`, the
default), `DB::Put` writes the record to the WAL buffer and returns. The options are accepted together at `DB::Open`.

### Actual behavior

The first `Put` never returns. The writing thread blocks on `wal_write_mutex_`, which it already holds:

```
#4  rocksdb::port::Mutex::Lock()
#5  rocksdb::InstrumentedMutex::Lock()
#6  rocksdb::DBImpl::WriteToWAL(...)
#7  rocksdb::DBImpl::ConcurrentWriteGroupToWAL(...)
#8  rocksdb::DBImpl::WriteImplWALOnly(...)
#9  rocksdb::DBImpl::WriteImpl(...)
#10 rocksdb::DBImpl::Write(...)
#11 rocksdb::DB::Put(...)
```

Cause, in `db/db_impl/db_impl_write.cc`:

- With `unordered_write`, `WriteImpl` writes the WAL through `WriteImplWALOnly`, which calls
  `ConcurrentWriteGroupToWAL`.
- `ConcurrentWriteGroupToWAL` calls `wal_write_mutex_.Lock()`, then calls `WriteToWAL`.
- `WriteToWAL` locks the same mutex again when `manual_wal_flush_ && !two_write_queues_`:

  ```cpp
  // When two_write_queues_ WriteToWAL has to be protected from concurretn calls
  // from the two queues anyway and wal_write_mutex_ is already held. Otherwise
  // if manual_wal_flush_ is enabled we need to protect log_writer->AddRecord
  // from possible concurrent calls via the FlushWAL by the application.
  const bool needs_locking = manual_wal_flush_ && !two_write_queues_;
  ```

The condition assumes the mutex is already held only when `two_write_queues_` is set. That assumption is false for
`unordered_write`, which also reaches `ConcurrentWriteGroupToWAL` (whose own assertion is
`two_write_queues_ || immutable_db_options_.unordered_write`). `port::Mutex` is not recursive, so the thread deadlocks.

A possible fix is to pass the "already holds `wal_write_mutex_`" state into `WriteToWAL` from its callers instead of
deriving it from `two_write_queues_`. Alternatively, reject the option combination in `DB::Open`.

Related issue in the same function (found by reading the code, not reproduced): when
`MaybeAddUserDefinedTimestampSizeRecord` fails, `WriteToWAL` returns early without the matching
`wal_write_mutex_.Unlock()` for the `needs_locking` case.

### Steps to reproduce the behavior

The program below uses only the C API. Build and run it:

```
gcc repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -o repro && ./repro
```

It prints `put...` and is then killed by the 10 s alarm. `put returned` is never printed.

```c
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "rocksdb/c.h"
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(2); } } while (0)

int main(void) {
  system("rm -rf /tmp/repro_unordered");
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_options_set_unordered_write(o, 1);
  rocksdb_options_set_manual_wal_flush(o, 1);
  char* err = NULL;
  rocksdb_t* db = rocksdb_open(o, "/tmp/repro_unordered", &err); CHECK(err);
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  alarm(10);  /* the Put below never returns */
  fprintf(stderr, "put...\n");
  rocksdb_put(db, wo, "k", 1, "v", 1, &err); CHECK(err);
  fprintf(stderr, "put returned\n");
  rocksdb_close(db);
  return 0;
}
```

The equivalent C++ code also hangs. Open with `options.unordered_write = true; options.manual_wal_flush = true;`, then call
`db->Put(WriteOptions(), "k", "v")`.
