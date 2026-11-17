# Row cache + MergeOperator::ShouldMerge: a lookup that ShouldMerge ends caches a truncated row; later reads skip the rest of the file

**Suggested title:** `row_cache: TableCache::Get / MultiGet insert the replay log of a lookup that MergeOperator::ShouldMerge ended inside the file; a later snapshot read or GetMergeOperands replays the truncated entry and merges an older file's value`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

`DBOptions::row_cache` does not change read results: a point lookup returns the same value with and without it.
A row cache entry for (table file, key) holds the file's entries for the key down to the first one that ends every
lookup (a value or a deletion), so replaying it is equivalent to reading the file, whatever the reader has collected
before reaching the file.

### Actual behavior

When `MergeOperator::ShouldMerge` ends a point lookup inside a table file, the row cache entry inserted for that file
holds only the operands seen up to that point. A later lookup with the same row cache key that reaches the file with
fewer operands collected replays the truncated entry, finds the file "exhausted" in state `kMerge`, and goes on to
older files: it merges with an older file's value (one that the file's own base value overwrote), and the file's older
operands and base value are lost.

On a row cache miss, `TableCache::Get` (`db/table_cache_sync_and_async.h`) records every entry `GetContext::SaveValue`
visits (`GetContext::appendToReplayLog`) and inserts the log whenever it is not empty:

```cpp
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      s = CO_AWAIT(t->Get, options, k, get_context,
                   mutable_cf_options.prefix_extractor.get(), skip_filters);
      get_context->SetReplayLog(nullptr);
...
  // Put the replay log in row cache only if something was found.
  if (!done && s.ok() && row_cache_entry && !row_cache_entry->empty()) {
```

(`TableCache::MultiGet` does the same per key). `GetContext::SaveValue` (`table/get_context.cc`) ends the lookup at a
merge operand when `ShouldMerge` returns true, which depends on the operands collected before this file too (the
memtables' and newer files'):

```cpp
      case kTypeMerge:
        assert(State() == kNotFound || State() == kMerge);
        state_ = kMerge;
        // value_pinner is not set from plain_table_reader.cc for example.
        push_operand(value, value_pinner);
        PERF_COUNTER_ADD(internal_merge_point_lookup_count, 1);

        if (do_merge_ && merge_operator_ != nullptr &&
            merge_operator_->ShouldMerge(
                merge_context_->GetOperandsDirectionBackward())) {
          state_ = kFound;
          const Status s = MergeWithNoBaseValue();
          ...
          return false;
        }
```

The row cache key (`TableCache::CreateRowCacheKeyPrefix`) is the same for every read without a snapshot and for every
snapshot read whose snapshot is above the file's largest seqno ("we treat it the same as there is no snapshot"). A hit
replays the entry (`replayGetContextLog`), and `Version::Get` treats `kMerge` after the file as "keep searching":

```cpp
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        if (get_context.sample()) {
          sample_collapsible_entry_file_read_inc(f->file_metadata);
        }
        break;
```

Reads that reach the file with fewer operands than the read that populated the entry, and so go wrong:

- a snapshot read (snapshot above the file's largest seqno) that does not see newer memtable operands (reproduced
  below, for `Get` and for an entry populated by `MultiGet`; `GetEntity` uses the same `GetContext`);
- `GetMergeOperands` (`do_merge_ = false`, `ShouldMerge` is not consulted): after such a `Get`, it returns
  `[old, b, m1, m2]` instead of `[base, a, b, m1, m2]` in the program's DB (found by reading and in the port; the C
  API has no `GetMergeOperands`);
- by reading: a read after a compaction combined newer operands with `PartialMerge` (fewer operands reach the file),
  and any read with a `ShouldMerge` that does not depend only on the number of operands.

The same happens without `ShouldMerge` when `GetMergeOperandsOptions::continue_cb` ends a `GetMergeOperands` lookup
inside a file (the `continue_cb` branch right below sets `state_ = kFound` and returns false): the truncated log is
cached and a later `Get` replays it (found by reading, not run).

### Steps to reproduce

The C++ program below makes the DB through the C API and only the merge operator in C++
(`rocksdb_options_set_merge_operator` stores the pointer as `std::shared_ptr<MergeOperator>`), so it also runs
against a prebuilt library: `MergeOperator`'s virtual table in 11.12 (checked in the library's relocations) is the one
`include/rocksdb/merge_operator.h` declares on `main`, and the library is built without RTTI.

```cpp
// Row cache + MergeOperator::ShouldMerge: a point lookup that ShouldMerge ends inside a table file caches only the
// entries it has seen so far as that file's row cache entry. A later lookup that needs more of the file (here a
// snapshot read that does not see the newer memtable operands) replays the truncated entry, treats the file as having
// no more entries for the key and reads the older file: it returns a value overwritten long ago.
#include <rocksdb/c.h>
#include <rocksdb/merge_operator.h>

#include <cstdio>
#include <cstdlib>
#include <string>

using namespace ROCKSDB_NAMESPACE;

// String append with ','; ShouldMerge ends a point lookup once it holds 3 operands.
class AppendOp : public MergeOperator {
 public:
  bool FullMerge(const Slice&, const Slice* existing, const std::deque<std::string>& operands, std::string* out,
                 Logger*) const override {
    out->clear();
    if (existing != nullptr) out->assign(existing->data(), existing->size());
    for (const std::string& op : operands) {
      if (!out->empty()) out->push_back(',');
      out->append(op);
    }
    return true;
  }
  bool ShouldMerge(const std::vector<Slice>& operands) const override { return operands.size() >= 3; }
  const char* Name() const override { return "AppendOp"; }
};

static void check(char* err, const char* what) {
  if (err != nullptr) {
    printf("%s: %s\n", what, err);
    exit(1);
  }
}

static void run(bool row_cache, bool multi_get) {
  char* err = nullptr;
  const char* path = "/tmp/up16";
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_options_set_disable_auto_compactions(o, 1);
  // rocksdb_options_set_merge_operator only stores the pointer as std::shared_ptr<MergeOperator>.
  rocksdb_options_set_merge_operator(o, reinterpret_cast<rocksdb_mergeoperator_t*>(new AppendOp()));
  rocksdb_cache_t* cache = rocksdb_cache_create_lru(1 << 20);
  if (row_cache) rocksdb_options_set_row_cache(o, cache);
  rocksdb_destroy_db(o, path, &err);
  check(err, "destroy");
  rocksdb_t* db = rocksdb_open(o, path, &err);
  check(err, "open");
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  rocksdb_readoptions_t* ro = rocksdb_readoptions_create();
  rocksdb_flushoptions_t* fo = rocksdb_flushoptions_create();

  rocksdb_put(db, wo, "k", 1, "old", 3, &err);  // older table file
  check(err, "put");
  rocksdb_flush(db, fo, &err);
  check(err, "flush");
  rocksdb_put(db, wo, "k", 1, "base", 4, &err);  // newer table file: b, a, base
  check(err, "put");
  // A snapshot held during the flush keeps the flush from merging the operands into the base value.
  const rocksdb_snapshot_t* keep = rocksdb_create_snapshot(db);
  rocksdb_merge(db, wo, "k", 1, "a", 1, &err);
  rocksdb_merge(db, wo, "k", 1, "b", 1, &err);
  check(err, "merge");
  rocksdb_flush(db, fo, &err);
  check(err, "flush");
  rocksdb_release_snapshot(db, keep);
  rocksdb_put(db, wo, "other", 5, "x", 1, &err);  // the snapshot's seqno is above the newer file's largest seqno
  check(err, "put");
  const rocksdb_snapshot_t* snap = rocksdb_create_snapshot(db);
  rocksdb_merge(db, wo, "k", 1, "m1", 2, &err);  // memtable operands the snapshot does not see
  rocksdb_merge(db, wo, "k", 1, "m2", 2, &err);
  check(err, "merge");

  const char* mode = !row_cache ? "row cache off         " : multi_get ? "row cache on, MultiGet" : "row cache on, Get     ";
  size_t len = 0;
  char* v = nullptr;
  if (multi_get) {
    const char* keys[1] = {"k"};
    size_t key_lens[1] = {1};
    char* errs[1] = {nullptr};
    rocksdb_multi_get(db, ro, 1, keys, key_lens, &v, &len, errs);
    check(errs[0], "multiget");
  } else {
    v = rocksdb_get(db, ro, "k", 1, &len, &err);
    check(err, "get");
  }
  printf("%s: latest      = %.*s\n", mode, (int)len, v);
  rocksdb_free(v);

  rocksdb_readoptions_t* ros = rocksdb_readoptions_create();
  rocksdb_readoptions_set_snapshot(ros, snap);
  v = rocksdb_get(db, ros, "k", 1, &len, &err);
  check(err, "get");
  printf("%s: at snapshot = %.*s\n", mode, (int)len, v);
  rocksdb_free(v);

  rocksdb_release_snapshot(db, snap);
  rocksdb_readoptions_destroy(ros);
  rocksdb_readoptions_destroy(ro);
  rocksdb_writeoptions_destroy(wo);
  rocksdb_flushoptions_destroy(fo);
  rocksdb_close(db);
  rocksdb_options_destroy(o);
  rocksdb_cache_destroy(cache);
}

int main() {
  run(false, false);
  run(true, false);
  run(true, true);
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`; drop `-fno-rtti`
for a library built with RTTI):

```
g++ -std=c++20 -fno-rtti repro.cc -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
row cache off         : latest      = b,m1,m2
row cache off         : at snapshot = base,a,b
row cache on, Get     : latest      = b,m1,m2
row cache on, Get     : at snapshot = old,b
row cache on, MultiGet: latest      = b,m1,m2
row cache on, MultiGet: at snapshot = old,b
```

The latest read stops at `b` in the newer file (`m2`, `m1`, `b`: three operands) and caches `[b]` as that file's
entry. The snapshot read, with the row cache off, reads `b`, `a` and the base value of the same file (`base,a,b`);
with the row cache on it replays `[b]`, continues with the older file and returns `old,b`: `old` was overwritten by
`base` before the snapshot was taken. Reading at the snapshot first (so the complete entry is cached) gives the
right answer for both reads.

<details>
<summary>The same as a unit test for db/db_merge_operator_test.cc (not run)</summary>

```cpp
TEST_F(DBMergeOperatorTest, RowCacheEntryOfLookupEndedByShouldMerge) {
  class LimitedStringAppendMergeOp : public StringAppendTESTOperator {
   public:
    LimitedStringAppendMergeOp(size_t limit, char delim)
        : StringAppendTESTOperator(delim), limit_(limit) {}
    const char* Name() const override {
      return "DBMergeOperatorTest::LimitedStringAppendMergeOp";
    }
    bool ShouldMerge(const std::vector<Slice>& operands) const override {
      return operands.size() >= limit_;
    }

   private:
    size_t limit_;
  };

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(3, ',');
  options.row_cache = NewLRUCache(1 << 20);
  DestroyAndReopen(options);

  ASSERT_OK(Put("k", "old"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k", "base"));
  const Snapshot* keep = db_->GetSnapshot();
  ASSERT_OK(Merge("k", "a"));
  ASSERT_OK(Merge("k", "b"));
  ASSERT_OK(Flush());
  db_->ReleaseSnapshot(keep);
  ASSERT_OK(Put("other", "x"));
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Merge("k", "m1"));
  ASSERT_OK(Merge("k", "m2"));

  ASSERT_EQ("b,m1,m2", Get("k"));
  ASSERT_EQ("base,a,b", Get("k", snap));  // "old,b" without a fix
  db_->ReleaseSnapshot(snap);
}
```

</details>

### Suggested fix

Do not insert the replay log of a lookup that ended inside the file for a reason other than the file's own entries:
in `GetContext::SaveValue`, when `ShouldMerge` returns true (and in the `continue_cb` branch), discard the log, e.g.

```cpp
          state_ = kFound;
          if (replay_log_ != nullptr) {
            // The lookup ends before the file's last entry for this key: what
            // was recorded is not a complete row cache entry for the file.
            replay_log_->clear();
          }
```

`TableCache::Get` and `TableCache::MultiGet` already insert only a non-empty log. This costs nothing without a row
cache or without a `ShouldMerge` override; with both, the lookups `ShouldMerge` ends inside a file are no longer cached
(each reads the table, as without a row cache), while the other lookups of the key still are. Recording the file's
remaining entries for the key instead would keep those lookups cached, but makes them read past the point where
`ShouldMerge` was meant to stop them.

The managed port (RocksDb.Managed) had the same behavior (`old,b`) and now does this (`GetContext.SaveEntry` clears
the replay log when `ShouldMerge` ends the lookup; its `continue_cb` lookups do not use the row cache). Regression tests
`Audit11RowCacheShouldMergeTests`: the sequence above for every populating / reading path (`Get`, `GetAsync`,
`MultiGet`, `MultiGetAsync`, `GetEntity`), `GetMergeOperands`, the row cache hit / miss counts, and a seeded
randomized differential of a row-cached DB against an uncached one.
