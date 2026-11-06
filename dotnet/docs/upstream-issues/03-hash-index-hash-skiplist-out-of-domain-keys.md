# Hash index and hash skip list lose keys that are outside the prefix extractor's domain

**Suggested title:** `kHashSearch index and HashSkipListRep call SliceTransform::Transform on out-of-domain keys; Get returns NotFound for existing keys`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64). The code involved is unchanged on `main` (checked at
`ee13e2a64042d4952efe46dfe90dd3bef11cd351`).

### Expected behavior

The documentation of `ColumnFamilyOptions::prefix_extractor` and `SliceTransform::InDomain` allows keys for which
`InDomain()` is false ("keys with no prefix (out of domain)"). Such keys are not added to or checked against prefix
filters, but they are still ordinary keys: `DB::Get` finds them whatever index type or memtable representation is
configured.

With `prefix_extractor = rocksdb.FixedPrefix.4`, writing the keys `""`, `"a"`, `"ab"`, `"abc"`, `"abcd"`, `"abcde"`,
`"b"` and `"bcdef"` and then calling `Get` on each should find all eight. That should hold:

- after a flush, with `BlockBasedTableOptions::index_type = kHashSearch`;
- in the memtable, with the hash skip list memtable (`memtable=prefix_hash`).

### Actual behavior

Some of the keys shorter than the 4-byte prefix are not found. The results are the same on every run:

```
hash index (SST): Get("") -> NotFound (expected "v")
hash index (SST): Get("ab") -> NotFound (expected "v")
hash index (SST): 2 of 8 keys missing
hash skiplist (memtable): Get("") -> NotFound (expected "v")
hash skiplist (memtable): Get("a") -> NotFound (expected "v")
hash skiplist (memtable): Get("ab") -> NotFound (expected "v")
hash skiplist (memtable): Get("abc") -> NotFound (expected "v")
hash skiplist (memtable): Get("b") -> NotFound (expected "v")
hash skiplist (memtable): 5 of 8 keys missing
```

These code paths call `Transform` without first checking `InDomain`:

- `HashIndexBuilder::OnKeyAdded` (`table/block_based/index_builder.h`), when the SST is written;
- `BlockPrefixIndex::GetBlocks` (`table/block_based/block_prefix_index.cc`), when it is read;
- `HashSkipListRep` insert and lookup (`memtable/hash_skiplist_rep.cc`).

`FixedPrefixTransform::Transform` only has `assert(InDomain(src))`. A release build therefore returns
`Slice(src.data(), 4)` for a shorter key. That reads past the key, and the hash is computed over whatever bytes follow
it, so the insert and the lookup land in different buckets. A debug build would hit the assertion instead.

The other prefix-aware code checks `InDomain` first: the filter readers and builders, the memtable prefix bloom, and
`BlockBasedTable::PrefixRangeMayMatch`.

Possible fixes:

- route out-of-domain keys to a dedicated bucket, or to the binary-search fallback, in the hash index and the hash
  skip list;
- or document that every key must be in domain for these two features, and check it with a `Status` error at write
  time instead of silently corrupting lookups.

The hash linked list memtable (`HashLinkListRep`) may have the same problem; it was not tested.

### Steps to reproduce the behavior

The program below uses only the C API. Build and run it:

```
gcc repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -o repro && ./repro
```

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rocksdb/c.h"
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(2); } } while (0)

static const char* keys[] = {"", "a", "ab", "abc", "abcd", "abcde", "b", "bcdef"};

static int run(const char* path, const char* optstr, int flush, const char* what) {
  char cmd[256];
  snprintf(cmd, sizeof cmd, "rm -rf %s", path);
  system(cmd);
  char* err = NULL;
  rocksdb_options_t* base_opts = rocksdb_options_create();
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_get_options_from_string(base_opts, optstr, o, &err); CHECK(err);
  rocksdb_t* db = rocksdb_open(o, path, &err); CHECK(err);
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  for (size_t i = 0; i < sizeof keys / sizeof *keys; i++) {
    rocksdb_put(db, wo, keys[i], strlen(keys[i]), "v", 1, &err); CHECK(err);
  }
  if (flush) {
    rocksdb_flushoptions_t* fo = rocksdb_flushoptions_create();
    rocksdb_flush(db, fo, &err); CHECK(err);
  }
  rocksdb_readoptions_t* ro = rocksdb_readoptions_create();
  int missing = 0;
  for (size_t i = 0; i < sizeof keys / sizeof *keys; i++) {
    size_t len;
    char* v = rocksdb_get(db, ro, keys[i], strlen(keys[i]), &len, &err); CHECK(err);
    if (!v) { printf("%s: Get(\"%s\") -> NotFound (expected \"v\")\n", what, keys[i]); missing++; }
    free(v);
  }
  printf("%s: %d of %zu keys missing\n", what, missing, sizeof keys / sizeof *keys);
  rocksdb_close(db);
  return missing;
}

int main(void) {
  int m = 0;
  m += run("/tmp/repro_hashidx",
           "create_if_missing=true;prefix_extractor=rocksdb.FixedPrefix.4;"
           "block_based_table_factory={index_type=kHashSearch;block_size=64}",
           1, "hash index (SST)");
  m += run("/tmp/repro_hashskl",
           "create_if_missing=true;prefix_extractor=rocksdb.FixedPrefix.4;"
           "memtable=prefix_hash:1000;allow_concurrent_memtable_write=false",
           0, "hash skiplist (memtable)");
  return m ? 1 : 0;
}
```
