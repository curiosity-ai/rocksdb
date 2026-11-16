# A TimedPut WriteBatch record whose value is shorter than 8 bytes aborts the process, and the DB can no longer be opened

**Suggested title:** `ReadRecordFromWriteBatch: TimedPut value shorter than 8 bytes -> Slice(size - 8) wraps -> std::length_error abort on write and on every WAL replay`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

A malformed WriteBatch record fails with Corruption ("bad WriteBatch TimedPut"), like every other malformed record
type: `DB::Write` of a batch built from bytes (`WriteBatch(std::string rep)`, `rocksdb_writebatch_create_from`)
returns the error, and a WAL record holding such a batch is handled according to `wal_recovery_mode`.

### Actual behavior

A `kTypeValuePreferredSeqno` (0x18) or `kTypeColumnFamilyValuePreferredSeqno` (0x19) record whose packed value is
shorter than the 8-byte write time aborts the process with `std::length_error`. The batch is written to the WAL before
the memtable insert aborts, so every later `DB::Open` replays it and aborts too, in all four recovery modes (the WAL
record itself is intact, its CRC is valid): the DB cannot be opened again without removing the WAL by hand.

Cause, in `ReadRecordFromWriteBatch` (`db/write_batch.cc`):

```cpp
    case kTypeValuePreferredSeqno: {
      Slice packed_value;
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, &packed_value)) {
        return Status::Corruption("bad WriteBatch TimedPut");
      }
      if (write_unix_time) {
        std::tie(*value, *write_unix_time) =
            ParsePackedValueWithWriteTime(packed_value);
```

and `ParsePackedValueWithWriteTime` (`db/seqno_to_time_mapping.cc`):

```cpp
  return std::make_tuple(Slice(value.data(), value.size() - sizeof(uint64_t)),
                         ParsePackedValueForWriteTime(value));
```

For a value of 3 bytes the size wraps to 2^64 - 5 (and `ParsePackedValueForWriteTime` reads 8 bytes starting 5 bytes
before the value; its size check is an `assert`). On `DB::Write`, `MemTableInserter::TimedPutCF` repacks the value with
`PackValueAndWriteTime` (`buf->assign(value.data(), value.size())`, `std::length_error`); on WAL replay
`WriteBatchInternal::UpdateProtectionInfo`, called from `DBImpl::InitializeWriteBatchForLogRecord`, throws the same way.

### Steps to reproduce

The program below uses only the C API. It writes a 19-byte batch (header, then one 0x18 record with key `k` and the
3-byte value `abc`) with `rocksdb_write`, then reopens the DB. Each step runs in a child process.

```c
/* A WriteBatch TimedPut record whose packed value is shorter than 8 bytes aborts the process (std::length_error). */
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)

static void write_batch(void) {
  char* err = NULL;
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_t* db = rocksdb_open(o, "/tmp/up11", &err); CHECK(err);
  /* Header: sequence (8 bytes) and count 1 (4 bytes); then one kTypeValuePreferredSeqno (0x18) record: key "k",
     packed value "abc" (3 bytes; a valid packed value ends with an 8-byte write time). */
  const char rep[] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0x18, 1, 'k', 3, 'a', 'b', 'c'};
  rocksdb_writebatch_t* b = rocksdb_writebatch_create_from(rep, sizeof rep);
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  rocksdb_write(db, w, b, &err);
  printf("write: %s\n", err ? err : "OK");
  rocksdb_close(db);
}

static void open_db(void) {
  char* err = NULL;
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_t* db = rocksdb_open(o, "/tmp/up11", &err);
  printf("reopen: %s\n", err ? err : "OK");
  if (db) rocksdb_close(db);
}

static void run_in_child(void (*fn)(void), const char* what) {
  pid_t pid = fork();
  if (pid == 0) { fn(); _exit(0); }
  int st = 0;
  waitpid(pid, &st, 0);
  if (WIFSIGNALED(st)) printf("%s: process killed by signal %d\n", what, WTERMSIG(st));
}

int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  system("rm -rf /tmp/up11");
  run_in_child(write_batch, "write");
  run_in_child(open_db, "reopen");
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=gnu11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
terminate called after throwing an instance of 'std::length_error'
  what():  basic_string::_M_replace
write: process killed by signal 6
terminate called after throwing an instance of 'std::length_error'
  what():  basic_string::append
reopen: process killed by signal 6
```

The reopen aborts the same way with `wal_recovery_mode` set to each of the four modes.

### Suggested fix

Check the length where the record is parsed:

```cpp
      if (packed_value.size() < sizeof(uint64_t)) {
        return Status::Corruption("bad WriteBatch TimedPut");
      }
```

(and make the `assert`s in `ParsePackedValueForWriteTime` / `ParsePackedValueForSeqno` / `ParsePackedValueForValue`
real checks, since `DBIter` and `CompactionIterator` also call them on values read from table files).

The managed port (RocksDb.Managed) returns Corruption "bad WriteBatch TimedPut" for such a record, and validates a batch
before writing it to the WAL (regression test `WriteBatchMutationFuzzOnlyCorruptionEscapes`).
