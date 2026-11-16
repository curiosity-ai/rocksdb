# log::Reader drops WAL records without reporting corruption, even with kAbsoluteConsistency

**Suggested title:** `log::Reader returns kBadRecord without ReportCorruption for a zeroed record header mid-log and for a CRC-valid record that fails to decompress; WAL recovery silently loses writes in every recovery mode`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

With `wal_recovery_mode = kAbsoluteConsistency` (and `kTolerateCorruptedTailRecords` for damage that is not at the
tail), a WAL that cannot be replayed completely fails `DB::Open` with Corruption. With `kPointInTimeRecovery`, recovery
stops at the first damaged record and never applies records after a hole.

### Actual behavior

Two kinds of damage make `log::Reader::ReadPhysicalRecord` (`db/log_reader.cc`) discard the rest of the current 32 KiB
block and return `kBadRecord` without setting `drop_size` or calling `ReportCorruption`. `ReadRecord`'s `case
kBadRecord:` reports only when it is inside a fragmented record, so `DB::Open` succeeds in all four recovery modes and
the records are gone:

1. A record header overwritten with zeros (type `kZeroType`, length 0) in the middle of a WAL:

   ```cpp
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      // NOTE: this should never happen in DB written by new RocksDB versions,
      // since we turn off mmap writes to manifest and log files
      buffer_.clear();
      return kBadRecord;
    }
   ```

   Skipping a zero-filled preallocated tail silently is intended. But the check does not look at what follows: the rest
   of the block is discarded even when it holds valid records, and reading then continues with the next block, whose
   records are replayed. In the reproduction, 250 writes in the middle of the WAL are lost and the 146 writes after them
   are applied, with `kAbsoluteConsistency` and with `kPointInTimeRecovery`, whose result is then not a point in time.

2. With `wal_compression = kZSTD`, a record whose CRC is valid but whose compressed payload does not decompress:

   ```cpp
        remaining = uncompress_->Uncompress(
            input, length, uncompressed_buffer_.get(), &uncompressed_size);
        input = nullptr;
        if (remaining < 0) {
          buffer_.clear();
          return kBadRecord;
        }
   ```

   The XXH3 mismatch a few lines further down (`*fragment_checksum != actual_checksum`) also returns `kBadRecord`
   without a report. In the reproduction every write from the damaged record on is lost, and the open succeeds.

`FragmentBufferedReader::TryReadFragment` has the same pattern for both. Recovery compares sequence numbers only in
`kPointInTimeRecovery` after a reported corruption (`MaybeReviseStopReplayForCorruption`), so nothing else notices the
gap.

### Steps to reproduce

The program below uses only the C API. It writes 400 keys (one WAL record each, 100-byte values, so 254 records fill
the first 32 KiB block) and closes without a flush, then (1) zeroes the 7-byte header of the 5th record, or (2) with
`wal_compression = zstd`, overwrites the second half of the 5th record's payload (record 0 is the compression type
record) and recomputes the record's CRC. Each damaged DB is copied and opened in each recovery mode.

```c
/* WAL records lost without any error, in every wal_recovery_mode (kAbsoluteConsistency included):
   (1) a WAL record header overwritten with zeros, (2) a CRC-valid record whose zstd-compressed payload is damaged. */
#include <rocksdb/c.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)
#define N 400

static uint32_t crc32c(const unsigned char* p, size_t n) {
  uint32_t c = 0xffffffff;
  while (n--) { c ^= *p++; for (int k = 0; k < 8; k++) c = (c >> 1) ^ (0x82f63b78 & (0u - (c & 1))); }
  return ~c;
}
static uint32_t mask_crc(uint32_t c) { return ((c >> 15) | (c << 17)) + 0xa282ead8u; }

static unsigned char* read_file(const char* path, long* len) {
  FILE* f = fopen(path, "rb");
  fseek(f, 0, SEEK_END); *len = ftell(f); fseek(f, 0, SEEK_SET);
  unsigned char* b = malloc(*len);
  if (fread(b, 1, *len, f) != (size_t)*len) exit(1);
  fclose(f);
  return b;
}
static void write_file(const char* path, const unsigned char* b, long len) {
  FILE* f = fopen(path, "wb"); fwrite(b, 1, len, f); fclose(f);
}

/* Offset of the n-th physical record of a log that has only legacy (7-byte) headers. */
static long record_offset(const unsigned char* b, long len, int n) {
  long off = 0;
  for (;;) {
    if (32768 - off % 32768 < 7) { off += 32768 - off % 32768; continue; }
    if (off + 7 > len) return -1;
    if (n-- == 0) return off;
    off += 7 + (b[off + 4] | (b[off + 5] << 8));
  }
}

/* Writes keys k000000..k000399 (one WAL record each, 100-byte values) and closes without a flush. */
static void make_db(const char* dir, int zstd) {
  char* err = NULL;
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  if (zstd) rocksdb_options_set_wal_compression(o, rocksdb_zstd_compression);
  rocksdb_t* db = rocksdb_open(o, dir, &err); CHECK(err);
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  char k[16], v[100];
  for (int i = 0; i < N; i++) {
    snprintf(k, sizeof k, "k%06d", i);
    memset(v, 'a' + i % 26, sizeof v);
    rocksdb_put(db, w, k, strlen(k), v, sizeof v, &err); CHECK(err);
  }
  rocksdb_close(db);
  rocksdb_writeoptions_destroy(w);
  rocksdb_options_destroy(o);
}

static const char* wal_of(const char* dir) {
  static char path[256];
  char cmd[300];
  snprintf(cmd, sizeof cmd, "ls %s/*.log", dir);
  FILE* p = popen(cmd, "r");
  if (!fgets(path, sizeof path, p)) exit(1);
  pclose(p);
  path[strcspn(path, "\n")] = 0;
  return path;
}

/* Opens a copy of dir in each recovery mode and prints the status and which keys are present. */
static void open_all_modes(const char* dir, int zstd) {
  static const char* names[] = {"kTolerateCorruptedTailRecords", "kAbsoluteConsistency", "kPointInTimeRecovery",
                                "kSkipAnyCorruptedRecords"};
  for (int mode = 0; mode < 4; mode++) {
    char cmd[1024], copy[256], *err = NULL;
    snprintf(copy, sizeof copy, "%s-copy", dir);
    snprintf(cmd, sizeof cmd, "rm -rf %s && cp -r %s %s", copy, dir, copy);
    system(cmd);
    rocksdb_options_t* o = rocksdb_options_create();
    rocksdb_options_set_wal_recovery_mode(o, mode);
    if (zstd) rocksdb_options_set_wal_compression(o, rocksdb_zstd_compression);
    rocksdb_t* db = rocksdb_open(o, copy, &err);
    printf("  %-30s open: %s", names[mode], err ? err : "OK");
    if (db) {
      rocksdb_readoptions_t* r = rocksdb_readoptions_create();
      int first_missing = -1, last_missing = -1, missing = 0;
      for (int i = 0; i < N; i++) {
        char k[16]; size_t len; char* e = NULL;
        snprintf(k, sizeof k, "k%06d", i);
        char* v = rocksdb_get(db, r, k, strlen(k), &len, &e); CHECK(e);
        if (!v) { missing++; if (first_missing < 0) first_missing = i; last_missing = i; }
        free(v);
      }
      printf(", %d of %d keys missing", missing, N);
      if (missing) printf(" (k%06d..k%06d)", first_missing, last_missing);
      rocksdb_readoptions_destroy(r);
      rocksdb_close(db);
    }
    printf("\n");
    free(err);
    rocksdb_options_destroy(o);
  }
}

int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  system("rm -rf /tmp/up13 && mkdir -p /tmp/up13");
  long len;

  /* (1) Overwrite the 7-byte header of the 5th record (key k000004) with zeros. */
  make_db("/tmp/up13/zeroed", 0);
  const char* wal = wal_of("/tmp/up13/zeroed");
  unsigned char* b = read_file(wal, &len);
  printf("(1) WAL of %ld bytes; header of record 4 (offset %ld) zeroed:\n", len, record_offset(b, len, 4));
  memset(b + record_offset(b, len, 4), 0, 7);
  write_file(wal, b, len);
  free(b);
  open_all_modes("/tmp/up13/zeroed", 0);

  /* (2) wal_compression = zstd. Overwrite the second half of the compressed payload of the 5th record (record 0 is
     the compression type record, so this is key k000003) with 0x55 bytes and recompute the record's CRC. */
  make_db("/tmp/up13/zstd", 1);
  wal = wal_of("/tmp/up13/zstd");
  b = read_file(wal, &len);
  long at = record_offset(b, len, 4);
  int n = b[at + 4] | (b[at + 5] << 8);
  memset(b + at + 7 + n / 2, 0x55, n - n / 2);
  uint32_t crc = mask_crc(crc32c(b + at + 6, 1 + n));
  memcpy(b + at, &crc, 4); /* little-endian host */
  write_file(wal, b, len);
  free(b);
  printf("(2) zstd WAL of %ld bytes; payload of record 4 (offset %ld) damaged, CRC recomputed:\n", len, at);
  open_all_modes("/tmp/up13/zstd", 1);
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=gnu11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
(1) WAL of 51602 bytes; header of record 4 (offset 516) zeroed:
  kTolerateCorruptedTailRecords  open: OK, 250 of 400 keys missing (k000004..k000253)
  kAbsoluteConsistency           open: OK, 250 of 400 keys missing (k000004..k000253)
  kPointInTimeRecovery           open: OK, 250 of 400 keys missing (k000004..k000253)
  kSkipAnyCorruptedRecords       open: OK, 250 of 400 keys missing (k000004..k000253)
(2) zstd WAL of 17819 bytes; payload of record 4 (offset 136) damaged, CRC recomputed:
  kTolerateCorruptedTailRecords  open: OK, 397 of 400 keys missing (k000003..k000399)
  kAbsoluteConsistency           open: OK, 397 of 400 keys missing (k000003..k000399)
  kPointInTimeRecovery           open: OK, 397 of 400 keys missing (k000003..k000399)
  kSkipAnyCorruptedRecords       open: OK, 397 of 400 keys missing (k000003..k000399)
```

### Suggested fix

- Zero record: skip silently only when the rest of the buffer is zero as well (a preallocated tail), and remember that a
  zero region was seen; any record read after it, in this block or a later one, is a hole and is reported
  (`ReportCorruption` with the dropped size, i.e. `kBadRecordChecksum`-like handling).
- Decompression failure and XXH3 mismatch: set `*drop_size = buffer_.size()` and return `kBadRecordChecksum` (or a new
  type that `ReadRecord` reports), so the recovery mode decides, as for a CRC mismatch.

The managed port (RocksDb.Managed) currently matches native in both cases (compat tests
`DamageScenarioRecoversLikeNative("wal-zeroed-record")`, `DamagedCompressedWalRecoversLikeNative("wal-zstd-bad-frame")`).
