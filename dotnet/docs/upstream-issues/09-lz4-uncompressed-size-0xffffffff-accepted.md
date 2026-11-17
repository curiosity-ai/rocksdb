# LZ4: a block whose stored uncompressed size is 0xFFFFFFFF is accepted, and 4 GiB of uninitialized memory is read as the block

**Suggested title:** `LZ4_DecompressBlock: uncompressed size 0xFFFFFFFF becomes int -1 and LZ4's error -1 compares equal, so a corrupt block "decompresses" into uninitialized memory`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

An LZ4 (or LZ4HC) block whose stored uncompressed size does not match its payload fails with Corruption ("Error
decompressing LZ4 data"), whatever the stored size is. That is what happens for 0xFFFFFFFE, 0x7FFFFFF0 or any other
wrong size, and for the same damage with Snappy, zlib and ZSTD.

### Actual behavior

For a stored size of exactly 0xFFFFFFFF the block is accepted: a 4 GiB buffer is allocated, nothing is written into
it, and it is parsed as the uncompressed block. A `Get` of a key in that block returns NotFound for an existing key or
crashes the process (SIGSEGV in `DataBlockHashIndex::Lookup`, called from `DataBlockIter::SeekForGetImpl` /
`BlockBasedTable::Get`), depending on what the uninitialized memory holds.

This needs a block whose checksum is not verified: `ReadOptions::verify_checksums = false`, or a table written with
`checksum = kNoChecksum` read with default options (also reproduced: same results).

Cause, in `LZ4_DecompressBlock` (`util/compression.cc`):

```cpp
  int expected_uncompressed_size = static_cast<int>(args.uncompressed_size);
  ...
  int uncompressed_size = LZ4_decompress_safe_continue(
      stream, args.compressed_data.data(), uncompressed_output,
      static_cast<int>(args.compressed_data.size()),
      expected_uncompressed_size);
  LZ4_freeStreamDecode(stream);

  if (uncompressed_size != expected_uncompressed_size) {
    if (uncompressed_size < 0) {
      return Status::Corruption("Error decompressing LZ4 data");
```

`Decompressor::ExtractUncompressedSize` accepts any varint64 up to `SIZE_MAX`, and `DecompressBlockData`
(`table/format.cc`) allocates `args.uncompressed_size` bytes. 0xFFFFFFFF converts to the output capacity -1; LZ4 rejects
a negative capacity with its error code -1, which is equal to `expected_uncompressed_size`, so the function returns OK
and `DecompressBlockData` returns `BlockContents(std::move(ubuf), args.uncompressed_size)`.

### Steps to reproduce

The program below uses only the C API. It writes 500 keys with LZ4 compression, flushes, overwrites the varint
uncompressed size at the start of the first data block (offset 0 of the table file) with 0xFFFFFFFE and then with
0xFFFFFFFF, and reads the first key with `verify_checksums` on and off. Each read runs in a child process.

```c
/* An LZ4 block whose stored uncompressed size is 0xFFFFFFFF is accepted as a 4 GiB block. */
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)

/* Opens the DB and reads key k000000 without filling the block cache; prints the status or the value. */
static void read_key(const char* dir, int verify) {
  char* err = NULL;
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_compression(o, rocksdb_lz4_compression);
  rocksdb_t* db = rocksdb_open(o, dir, &err); CHECK(err);
  rocksdb_readoptions_t* r = rocksdb_readoptions_create();
  rocksdb_readoptions_set_verify_checksums(r, (unsigned char)verify);
  rocksdb_readoptions_set_fill_cache(r, 0);
  size_t len = 0;
  char* v = rocksdb_get(db, r, "k000000", 7, &len, &err);
  if (err) printf("  verify_checksums=%d: Get: %s\n", verify, err);
  else if (!v) printf("  verify_checksums=%d: Get: NotFound\n", verify);
  else printf("  verify_checksums=%d: Get: OK, %zu bytes\n", verify, len);
  free(err); free(v);
  rocksdb_close(db);
}

int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  char* err = NULL;
  const char* dir = "/tmp/up09";
  system("rm -rf /tmp/up09");
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_options_set_compression(o, rocksdb_lz4_compression);
  rocksdb_t* db = rocksdb_open(o, dir, &err); CHECK(err);
  rocksdb_writeoptions_t* w = rocksdb_writeoptions_create();
  char k[16], v[100];
  memset(v, 'v', sizeof v);
  for (int i = 0; i < 500; i++) {
    snprintf(k, sizeof k, "k%06d", i);
    rocksdb_put(db, w, k, strlen(k), v, sizeof v, &err); CHECK(err);
  }
  rocksdb_flushoptions_t* f = rocksdb_flushoptions_create();
  rocksdb_flush(db, f, &err); CHECK(err);
  rocksdb_close(db);

  /* The first data block starts at offset 0 of the table file; LZ4 blocks begin with the varint32 uncompressed size.
     Overwrite it with the 5-byte varint of 0xFFFFFFFE, then of 0xFFFFFFFF, and read the block each time. */
  FILE* p = popen("ls /tmp/up09/*.sst", "r");
  char sst[256] = {0};
  if (!fgets(sst, sizeof sst, p)) { fprintf(stderr, "no table file\n"); return 1; }
  pclose(p);
  sst[strcspn(sst, "\n")] = 0;
  for (unsigned char low = 0xfe; low != 0; low++) {
    const unsigned char size[5] = {low, 0xff, 0xff, 0xff, 0x0f};
    FILE* fp = fopen(sst, "r+b");
    fwrite(size, 1, sizeof size, fp);
    fclose(fp);
    printf("stored uncompressed size 0xFFFFFF%02X:\n", low);
    for (int verify = 1; verify >= 0; verify--) {
      pid_t pid = fork(); /* the read may crash: run it in a child */
      if (pid == 0) { read_key(dir, verify); _exit(0); }
      int st = 0;
      waitpid(pid, &st, 0);
      if (WIFSIGNALED(st)) printf("  verify_checksums=%d: process killed by signal %d\n", verify, WTERMSIG(st));
    }
  }
  return 0;
}
```

Build and run it on Linux (`<lib dir>` holds `librocksdb.so` 11.12; the program writes under `/tmp`):

```
gcc -std=gnu11 repro.c -I<rocksdb>/include -L<lib dir> -lrocksdb -Wl,-rpath,<lib dir> -o repro && ./repro
```

Output with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385):

```
stored uncompressed size 0xFFFFFFFE:
  verify_checksums=1: Get: Corruption: block checksum mismatch: stored(context removed) = 2381754250, computed = 2219179961, type = 4  in /tmp/up09/000009.sst offset 0 size 318, block_type = Data
  verify_checksums=0: Get: Corruption: Error decompressing LZ4 data
stored uncompressed size 0xFFFFFFFF:
  verify_checksums=1: Get: Corruption: block checksum mismatch: stored(context removed) = 2381754250, computed = 87426379, type = 4  in /tmp/up09/000009.sst offset 0 size 318, block_type = Data
  verify_checksums=0: process killed by signal 11
```

In other runs the last line is `verify_checksums=0: Get: NotFound` (k000000 exists).

### Suggested fix

Reject sizes LZ4 cannot produce before converting them, e.g. at the start of `LZ4_DecompressBlock`:

```cpp
  if (args.uncompressed_size > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) {
    return Status::Corruption("Uncompressed size too large for LZ4");
  }
```

and check `uncompressed_size < 0` before comparing it with the expected size. A cheap bound on the stored size in
`ExtractUncompressedSize` (no codec expands more than a known factor of its input; LZ4's limit is 255x) would also stop
the multi-GiB allocation for any damaged size before the codec runs.

The managed port (RocksDb.Managed) returns Corruption when the stored size exceeds the codec's maximum expansion of the
payload (regression tests `HugeStoredUncompressedSizeIsCorruptionWithoutAllocating`, `HugeUncompressedSizeMatchesNative`).
