# DBIter returns freed memory when a merge result is the blob base value itself

**Suggested title:** `Iterator returns garbage (use-after-free) for merge result that is the blob base value (e.g. MaxOperator + enable_blob_files)`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64). The code involved is unchanged on `main` (checked at
`ee13e2a64042d4952efe46dfe90dd3bef11cd351`).

### Expected behavior

Take a key whose base value is stored in a blob file (`enable_blob_files = true`) and that has merge operands on top.
An iterator positioned on that key returns the merge result. With the built-in `max` merge operator, a base value of
100 bytes of `'z'` and an operand `"a"`, the result is the base value, and `DB::Get` correctly returns it.

### Actual behavior

`Iterator::value()` has the right length, but it points into freed heap memory: its first bytes are garbage, and the
bytes differ from run to run. This happened on every one of 1000 iterations. `DB::Get` on the same DB returns the correct value.

Cause:

1. `DBIter::MergeWithBlobBaseValue` (`db/db_iter.cc`) reads the blob into `blob_state_` and calls
   `MergeWithPlainBaseValue(blob_state_->reader.GetBlobValue(), user_key)`.
2. That calls `MergeHelper::TimedFullMerge(..., &saved_value, &pinned_value_, ...)` with a `result_operand`.
3. When the merge operator reports its result through `MergeOperationOutput::existing_operand` and the result is
   the existing value (`MaxOperator::FullMergeV2` does this; `put`, `stringappend2` and TTL also use
   `existing_operand`), `TimedFullMerge` sets `*result_operand = operand`. So `pinned_value_` points into the blob
   buffer.
4. `MergeWithBlobBaseValue` then calls `blob_state_.Reset()`, which frees that buffer while `pinned_value_`, and so the
   iterator's value, still points into it.

Possible fixes: copy the result into `saved_value` when it aliases the blob value before calling `blob_state_.Reset()`.
Or keep the blob value alive until the iterator moves, as is already done when the iterator is positioned on a plain
blob value.

### Steps to reproduce the behavior

The program below uses only the C API. Build and run it:

```
gcc repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -o repro && ./repro
```

Output with 11.12.0:

```
Get:      len=100 OK
Iterator: len=100 WRONG, first bytes: e6 34 41 eb 3c 56 00 00 3a 96 39 22 9c 79 60 e3
Iterator: 1000 of 1000 reads returned a wrong value
```

Running it under ASan or valgrind with a RocksDB build should report the read as a heap-use-after-free.

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rocksdb/c.h"
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(2); } } while (0)

int main(void) {
  system("rm -rf /tmp/repro_blobmax");
  char* err = NULL;
  rocksdb_options_t* base_opts = rocksdb_options_create();
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_get_options_from_string(base_opts,
      "create_if_missing=true;enable_blob_files=true;min_blob_size=0;merge_operator=max",
      o, &err); CHECK(err);
  rocksdb_t* db = rocksdb_open(o, "/tmp/repro_blobmax", &err); CHECK(err);
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  rocksdb_readoptions_t* ro = rocksdb_readoptions_create();
  rocksdb_flushoptions_t* fo = rocksdb_flushoptions_create();
  char base[100];
  memset(base, 'z', sizeof base);
  /* The base value goes to a blob file; the smaller operand stays in the memtable,
     so MaxOperator returns the base value itself via existing_operand. */
  rocksdb_put(db, wo, "key", 3, base, sizeof base, &err); CHECK(err);
  rocksdb_flush(db, fo, &err); CHECK(err);
  rocksdb_merge(db, wo, "key", 3, "a", 1, &err); CHECK(err);

  size_t len;
  char* v = rocksdb_get(db, ro, "key", 3, &len, &err); CHECK(err);
  printf("Get:      len=%zu %s\n", len, len == sizeof base && !memcmp(v, base, len) ? "OK" : "WRONG");
  free(v);

  int bad = 0;
  for (int round = 0; round < 1000; round++) {
    rocksdb_iterator_t* it = rocksdb_create_iterator(db, ro);
    rocksdb_iter_seek_to_first(it);
    const char* iv = rocksdb_iter_value(it, &len);
    if (len != sizeof base || memcmp(iv, base, len)) {
      if (!bad) {
        printf("Iterator: len=%zu WRONG, first bytes:", len);
        for (int i = 0; i < 16 && i < (int)len; i++) printf(" %02x", (unsigned char)iv[i]);
        printf("\n");
      }
      bad++;
    }
    rocksdb_iter_destroy(it);
  }
  printf("Iterator: %d of 1000 reads returned a wrong value\n", bad);
  rocksdb_close(db);
  return bad ? 1 : 0;
}
```
