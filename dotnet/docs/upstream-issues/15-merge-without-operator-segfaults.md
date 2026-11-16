# Reading merge operands without a merge operator segfaults: reverse iteration, and Get / MultiGet above an older value

**Suggested title:** `DB opened without merge_operator: DBIter::FindValueForCurrentKey and GetContext::SaveValue only assert merge_operator != nullptr, then TimedFullMerge dereferences it -> SIGSEGV on Prev / SeekForPrev / SeekToLast and on Get / MultiGet of an operand above an older value`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

A DB whose table files hold merge operands (written with a merge operator) can be opened without one: nothing checks
the operator against the OPTIONS file or the `rocksdb.merge.operator` table property. Every read that meets an
operand then fails with InvalidArgument, as it already does for forward iteration (`DBIter::MergeValuesNewToOld`), for
`Get` / `MultiGet` of a key that has only operands (`Version::Get`), for the memtable (`MemTable`'s
`HandleTypeMerge`) and for compaction (`CompactionIterator::NextFromInput`):

```
Invalid argument: merge_operator_ must be set.                    (forward iteration)
Invalid argument: merge_operator is not properly initialized.     (Get / MultiGet, compaction)
```

### Actual behavior

Reverse iteration that reaches a merge operand (`SeekToLast`, `Prev`, `SeekForPrev`), and `Get` / `MultiGet`
(and `GetEntity`, which uses the same `GetContext`) of a key whose operands lie above an older value or deletion of
the key in a table file, call `MergeHelper::TimedFullMerge` with a null operator and the process dies with SIGSEGV. The checks are `assert`s, so a
release build goes on and dereferences the null pointer (`merge_operator->FullMergeV3(...)` in
`MergeHelper::TimedFullMergeCommonImpl`, `db/merge_helper.cc`, which itself only has `assert(merge_operator)`).

Cause, reverse iteration: `DBIter::FindValueForCurrentKey` (`db/db_iter.cc`) collects the operands with only an
`assert`:

```cpp
      case kTypeMerge: {
        assert(merge_operator_ != nullptr);
        merge_context_.PushOperandBack(
            iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
        PERF_COUNTER_ADD(internal_merge_count, 1);
      } break;
```

and then merges without checking, through `MergeWithNoBaseValue` (no older value, or a deletion),
`MergeWithPlainBaseValue` (`kTypeValue` / `kTypeValuePreferredSeqno`), `MergeWithBlobBaseValue` (which calls
`MergeWithPlainBaseValue`) or `MergeWithWideColumnBaseValue`, each of which passes `merge_operator_` straight to
`TimedFullMerge`:

```cpp
bool DBIter::MergeWithNoBaseValue(const Slice& user_key) {
  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  ValueType result_type;
  const Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key, MergeHelper::kNoBaseValue,
```

`DBIter::FindValueForCurrentKeyUsingSeek` (taken after `max_sequential_skip_in_iterations` entries of one key) has
no check at all before calling the same four helpers. The forward direction is safe only because
`DBIter::MergeValuesNewToOld` checks first:

```cpp
bool DBIter::MergeValuesNewToOld() {
  if (!merge_operator_) {
    ROCKS_LOG_ERROR(logger_, "Options::merge_operator is null.");
    status_ = Status::InvalidArgument("merge_operator_ must be set.");
    valid_ = false;
    return false;
  }
```

Cause, point lookups: `Version::Get` (`db/version_set_sync_and_async.h`) checks the operator only after the last file,
when the key's history ended in operands (`if (!merge_operator_) { *status = Status::InvalidArgument("merge_operator
is not properly initialized."); ... }`). When an older value is found while operands are pending,
`GetContext::SaveValue` (`table/get_context.cc`) merges right away:

```cpp
        } else if (State() == kMerge) {
          assert(merge_operator_ != nullptr);
```

followed by `GetContext::MergeWithPlainBaseValue` / `MergeWithWideColumnBaseValue` (and `MergeWithNoBaseValue` for a
deletion in the `kTypeDeletion` case), which pass the null `merge_operator_` to `TimedFullMerge`. The backtraces with
the 11.12 library:

```
#0  rocksdb::MergeHelper::TimedFullMergeImpl(...)
#1  rocksdb::GetContext::MergeWithPlainBaseValue(rocksdb::Slice const&)
#2  rocksdb::GetContext::SaveValue(...)
#3  rocksdb::BlockBasedTable::Get(...)
#4  rocksdb::TableCache::Get(...)
#5  rocksdb::Version::Get(...)

#0  rocksdb::MergeHelper::TimedFullMergeImpl(...)
#1  rocksdb::DBIter::MergeWithNoBaseValue(rocksdb::Slice const&)
#2  rocksdb::DBIter::FindValueForCurrentKey(bool&)
#3  rocksdb::DBIter::PrevInternal()
#4  rocksdb::DBIter::Prev()

#0  rocksdb::MergeHelper::TimedFullMergeImpl(...)
#1  rocksdb::DBIter::MergeWithPlainBaseValue(rocksdb::Slice const&, rocksdb::Slice const&)
#2  rocksdb::DBIter::FindValueForCurrentKey(bool&)
#3  rocksdb::DBIter::PrevInternal()
#4  rocksdb::DBIter::SeekForPrev(rocksdb::Slice const&)
```

The operands have to be in a table file (written by a flush or compaction while the operator was set, or ingested).
The memtable cannot hold any without an operator: `MemTableInserter::MergeCF` (`db/write_batch.cc`) rejects them on
write and on WAL replay,

```cpp
    if (moptions->merge_operator == nullptr) {
      return Status::InvalidArgument(
          "Merge requires `ColumnFamilyOptions::merge_operator != nullptr`");
    }
```

and `DBImpl::ProcessLogRecord` reports the failed batch as a corrupted WAL record, so with the default
`kPointInTimeRecovery` the open succeeds and replay stops at the first Merge record: in the reproduction below the
Merge records and the `Put` written after them (`n`) are gone, and no read crashes. (That an operator-less open of a
WAL holding Merge records silently drops every later write is a separate question; it is shown only to explain why
the crash needs operands in a table file.)

### Steps to reproduce

The program below uses only the C API. With a string-append merge operator it writes `a=1`, `k=base`, `z=2`, then
`Merge(k, o1)`, `Merge(m, x)` (no base value) and `n=3`. In (1) each group is flushed to its own L0 file (so the flush
cannot fold `o1` into `k`'s base value); in (2) the DB is closed with everything in the WAL. Each DB is reopened
without a merge operator and each read runs in a child process.

```c
/* A DB with merge operands in its table files, opened without a merge operator: reverse iteration over an operand,
   and Get / MultiGet of a key whose operands sit above an older value in a table file, crash (SIGSEGV); forward
   iteration and Get of a key with only operands return InvalidArgument. */
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)

/* A string-append merge operator ("base,o1"), used only to write the operands. */
static char* full_merge(void* s, const char* key, size_t key_len, const char* existing, size_t existing_len,
                        const char* const* ops, const size_t* op_lens, int n, unsigned char* success, size_t* len) {
  (void)s; (void)key; (void)key_len;
  size_t total = existing ? existing_len : 0;
  for (int i = 0; i < n; i++) total += op_lens[i] + 1;
  char* out = malloc(total + 1);
  size_t at = 0;
  if (existing) { memcpy(out, existing, existing_len); at = existing_len; }
  for (int i = 0; i < n; i++) {
    if (at) out[at++] = ',';
    memcpy(out + at, ops[i], op_lens[i]); at += op_lens[i];
  }
  *len = at; *success = 1;
  return out;
}
static char* partial_merge(void* s, const char* key, size_t key_len, const char* const* ops, const size_t* op_lens,
                           int n, unsigned char* success, size_t* len) {
  (void)s; (void)key; (void)key_len; (void)ops; (void)op_lens; (void)n; (void)len;
  *success = 0;
  return NULL;
}
static void delete_value(void* s, const char* v, size_t n) { (void)s; (void)n; free((char*)v); }
static const char* name(void* s) { (void)s; return "append"; }

static rocksdb_options_t* options(int with_operator) {
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  if (with_operator)
    rocksdb_options_set_merge_operator(o, rocksdb_mergeoperator_create(NULL, NULL, full_merge, partial_merge,
                                                                       delete_value, name));
  return o;
}

static const char* path;
static rocksdb_t* db;

static void flush(void) {
  char* err = NULL;
  rocksdb_flushoptions_t* f = rocksdb_flushoptions_create();
  rocksdb_flush(db, f, &err); CHECK(err);
  rocksdb_flushoptions_destroy(f);
}

/* With a merge operator: a=1, k=base, z=2, then merge(k, o1), merge(m, x) (m has no base value) and n=3. With
   use_sst, each group is flushed to its own L0 file (so the flush cannot fold o1 into k's base value); without,
   the DB is closed with everything in the WAL. */
static void write_db(int use_sst) {
  char* err = NULL;
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  db = rocksdb_open(options(1), path, &err); CHECK(err);
  rocksdb_put(db, w, "a", 1, "1", 1, &err); CHECK(err);
  rocksdb_put(db, w, "k", 1, "base", 4, &err); CHECK(err);
  rocksdb_put(db, w, "z", 1, "2", 1, &err); CHECK(err);
  if (use_sst) flush();
  rocksdb_merge(db, w, "k", 1, "o1", 2, &err); CHECK(err);
  rocksdb_merge(db, w, "m", 1, "x", 1, &err); CHECK(err);
  rocksdb_put(db, w, "n", 1, "3", 1, &err); CHECK(err);
  if (use_sst) flush();
  rocksdb_close(db);
  rocksdb_writeoptions_destroy(w);
}

static void open_without_operator(void) {
  char* err = NULL;
  db = rocksdb_open(options(0), path, &err);
  printf("  open without a merge operator: %s\n", err ? err : "OK");
  if (err) exit(1);
}

static void print_result(const char* v, size_t len, char* err) {
  if (err) printf("%s", err);
  else if (v) printf("%.*s", (int)len, v);
  else printf("not found");
  free(err);
}

/* "get K", "multiget K", "next" (SeekToFirst + Next), "prev" (SeekToLast + Prev), "seekforprev K". */
static void step(const char* op) {
  char* err = NULL;
  const char* key = strchr(op, ' ') ? strchr(op, ' ') + 1 : "";
  size_t len;
  rocksdb_readoptions_t* r = rocksdb_readoptions_create();
  if (strncmp(op, "get ", 4) == 0) {
    char* v = rocksdb_get(db, r, key, strlen(key), &len, &err);
    print_result(v, len, err);
    free(v);
  } else if (strncmp(op, "multiget ", 9) == 0) {
    size_t key_len = strlen(key);
    char* v;
    rocksdb_multi_get(db, r, 1, &key, &key_len, &v, &len, &err);
    print_result(v, len, err);
    free(v);
  } else {
    rocksdb_iterator_t* it = rocksdb_create_iterator(db, r);
    if (strcmp(op, "next") == 0) rocksdb_iter_seek_to_first(it);
    else if (strcmp(op, "prev") == 0) rocksdb_iter_seek_to_last(it);
    else rocksdb_iter_seek_for_prev(it, key, strlen(key));
    while (rocksdb_iter_valid(it)) {
      size_t kl, vl;
      const char* k = rocksdb_iter_key(it, &kl);
      const char* v = rocksdb_iter_value(it, &vl);
      printf("%.*s=%.*s ", (int)kl, k, (int)vl, v);
      if (strcmp(op, "next") == 0) rocksdb_iter_next(it);
      else if (strcmp(op, "prev") == 0) rocksdb_iter_prev(it);
      else break;
    }
    rocksdb_iter_get_error(it, &err);
    printf("-> %s", err ? err : "OK");
    free(err);
    rocksdb_iter_destroy(it);
  }
  printf("\n");
}

/* Runs each step in a child process and prints the signal that killed it, if any. */
static void run_steps(const char* const* ops) {
  for (; *ops; ops++) {
    printf("  %-14s ", *ops);
    pid_t pid = fork();
    if (pid == 0) { step(*ops); _exit(0); }
    int st = 0;
    waitpid(pid, &st, 0);
    if (WIFSIGNALED(st)) printf("process killed by signal %d\n", WTERMSIG(st));
  }
}

int main(void) {
  static const char* const ops[] = {"get a", "get k", "get m", "get n", "multiget k", "multiget m", "next",
                                    "prev", "seekforprev k", "seekforprev m", NULL};
  setvbuf(stdout, NULL, _IONBF, 0);
  system("rm -rf /tmp/up15 && mkdir -p /tmp/up15");

  printf("(1) merge operands in an SST file, k's base value in an older SST file\n");
  path = "/tmp/up15/sst";
  write_db(1);
  open_without_operator();
  run_steps(ops);
  rocksdb_close(db);

  printf("(2) merge operands only in the WAL\n");
  path = "/tmp/up15/wal";
  write_db(0);
  open_without_operator();
  run_steps(ops);
  rocksdb_close(db);
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=gnu11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
(1) merge operands in an SST file, k's base value in an older SST file
  open without a merge operator: OK
  get a          1
  get k          process killed by signal 11
  get m          Invalid argument: merge_operator is not properly initialized.
  get n          3
  multiget k     process killed by signal 11
  multiget m     Invalid argument: merge_operator is not properly initialized.
  next           a=1 -> Invalid argument: merge_operator_ must be set.
  prev           z=2 n=3 process killed by signal 11
  seekforprev k  process killed by signal 11
  seekforprev m  process killed by signal 11
(2) merge operands only in the WAL
  open without a merge operator: OK
  get a          1
  get k          base
  get m          not found
  get n          not found
  multiget k     base
  multiget m     not found
  next           a=1 k=base z=2 -> OK
  prev           z=2 k=base a=1 -> OK
  seekforprev k  k=base -> OK
  seekforprev m  k=base -> OK
```

In (1), `prev` dies on `m` (no base value, `DBIter::MergeWithNoBaseValue`), `seekforprev k` on `k` (base value in the
older file, `DBIter::MergeWithPlainBaseValue`), and `get k` / `multiget k` in `GetContext::SaveValue` when the older
file's `base` is found; `get m` and forward iteration fail cleanly.

### Suggested fix

Turn the `assert`s into the check the forward path and `Version::Get` already make, so that every path returns
InvalidArgument:

- `DBIter`: check `merge_operator_` where a merge operand is met in `FindValueForCurrentKey` (the `case kTypeMerge:`
  above) and in `FindValueForCurrentKeyUsingSeek` (before the first `PushOperand`), setting `status_` and `valid_ =
  false` like `MergeValuesNewToOld`; or check once at the top of `MergeWithNoBaseValue` / `MergeWithPlainBaseValue` /
  `MergeWithWideColumnBaseValue`, which `SetValueAndColumnsFromMergeResult` already propagates as an iterator error.
- `GetContext`: return `Status::InvalidArgument("merge_operator is not properly initialized.")` from
  `MergeWithNoBaseValue` / `MergeWithPlainBaseValue` / `MergeWithWideColumnBaseValue` when `merge_operator_` is null;
  `SaveValue` already stores a failed merge status in `*read_status` and `Version::Get` / `MultiGet` return it.

(Alternatively, a single check replacing `assert(merge_operator)` in `MergeHelper::TimedFullMergeCommonImpl` covers
all callers.)

The managed port (RocksDb.Managed) returns InvalidArgument in all of these cases: its `DBIter` checks for the operator
before collecting operands in both directions, and its `GetContext` merges only in the final step, which checks first
(compat test `ReopenWithoutOrWithAnotherMergeOperatorMatchesNative`, which matches native everywhere else and asserts
InvalidArgument for the reverse scan that crashes native).
