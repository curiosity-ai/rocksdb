# A block handle with a huge size in a damaged table file aborts or crashes the process

**Suggested title:** `BlockFetcher: a block handle size of 2^40 throws std::bad_alloc (abort), one that wraps size + kBlockTrailerSize segfaults; expected Corruption`

**Affected versions:** reproduced with RocksDB 11.12.0 (Linux x86-64, `RocksDB` NuGet package 11.12.0.7385). The code
involved is unchanged on `main` (11.14, checked at `a87e2a0e43a79bc397b97803022fe7df24e5e22d`).

### Expected behavior

Opening a table file whose footer (or index) holds a damaged block handle fails with Corruption, as it does for a
handle whose offset is past the end of the file ("truncated block read"). `IngestExternalFile` of such a file, and
`DB::Open` of a DB containing one, return that status.

### Actual behavior

The size of a block handle is used unchecked:

- A size of 2^40 (any size the allocator cannot satisfy): `BlockFetcher::PrepareBufferForBlockFromFile` calls
  `AllocateBlock(block_size_with_trailer_, ...)`, `new char[]` throws `std::bad_alloc`, nothing catches it and the
  process aborts.
- A size of 2^64 - 4: `block_size_with_trailer_(block_size_ + footer.GetBlockTrailerSize())` (`table/block_fetcher.h`)
  wraps around to 1. One byte is read, so the "truncated block read" check (`slice_.size() != block_size_with_trailer_`)
  passes, then `ProcessTrailerIfPresent` calls `VerifyBlockChecksum(footer_, slice_.data(), block_size_, ...)` with
  `block_size_` = 2^64 - 4 and the process crashes with SIGSEGV:

```
#1  rocksdb::ComputeBuiltinChecksum(rocksdb::ChecksumType, char const*, unsigned long)
#2  rocksdb::VerifyBlockChecksum(...)
#3  rocksdb::BlockFetcher::ProcessTrailerIfPresent()
#4  rocksdb::BlockFetcher::ReadBlock(bool)
#5  rocksdb::BlockFetcher::ReadBlockContents()
#6  rocksdb::ReadAndParseBlockFromFile<rocksdb::Block_kMetaIndex>(...)
#7  rocksdb::BlockBasedTable::ReadMetaIndexBlock(...)
#8  rocksdb::BlockBasedTable::Open(...)
...
#15 rocksdb::DBImpl::IngestExternalFiles(...)
```

`BlockHandle::DecodeFrom` (`table/format.cc`) accepts any two varint64s. The same happens when `DB::Open` opens the
table (with the default `max_open_files = -1`): a DB whose table file has a 2^40 metaindex size in its footer aborts in
`DB::Open` (checked with the same library).

### Steps to reproduce

The program below uses only the C API. It writes a table file with `SstFileWriter` (format_version 5, no compression),
rewrites the metaindex handle in the footer (the last 53 bytes: checksum type, metaindex and index handles as varints
padded to 40 bytes, format version, magic number) and ingests the result into a new DB, in a child process per case.

```c
/* A table file whose footer names a block with a huge size aborts or crashes the process instead of failing with
   Corruption. The table is opened by IngestExternalFile. */
#include <rocksdb/c.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(err) do { if (err) { fprintf(stderr, "line %d: %s\n", __LINE__, err); exit(1); } } while (0)

static int put_varint64(unsigned char* p, uint64_t v) {
  int n = 0;
  while (v >= 0x80) { p[n++] = (unsigned char)(v | 0x80); v >>= 7; }
  p[n++] = (unsigned char)v;
  return n;
}
static int get_varint64(const unsigned char* p, uint64_t* v) {
  int n = 0, shift = 0; *v = 0;
  do { *v |= (uint64_t)(p[n] & 0x7f) << shift; shift += 7; } while (p[n++] & 0x80);
  return n;
}

/* Copies src to dst, replacing the metaindex handle of the format_version 5 footer (the last 53 bytes: checksum type,
   metaindex and index handles as varints padded to 40 bytes, format version, magic number). */
static void write_with_metaindex(const char* src, const char* dst, uint64_t offset, uint64_t size) {
  FILE* f = fopen(src, "rb");
  fseek(f, 0, SEEK_END); long len = ftell(f); fseek(f, 0, SEEK_SET);
  unsigned char* buf = malloc(len);
  if (fread(buf, 1, len, f) != (size_t)len) exit(1);
  fclose(f);
  unsigned char* handles = buf + len - 53 + 1;
  uint64_t meta_off, meta_size, index_off, index_size;
  int n = get_varint64(handles, &meta_off);
  n += get_varint64(handles + n, &meta_size);
  n += get_varint64(handles + n, &index_off);
  get_varint64(handles + n, &index_size);
  memset(handles, 0, 40);
  n = put_varint64(handles, offset);
  n += put_varint64(handles + n, size);
  n += put_varint64(handles + n, index_off);
  put_varint64(handles + n, index_size);
  f = fopen(dst, "wb");
  fwrite(buf, 1, len, f);
  fclose(f);
  free(buf);
}

static void ingest(const char* file) {
  char* err = NULL;
  system("rm -rf /tmp/up10/db");
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(o, 1);
  rocksdb_t* db = rocksdb_open(o, "/tmp/up10/db", &err); CHECK(err);
  rocksdb_ingestexternalfileoptions_t* io = rocksdb_ingestexternalfileoptions_create();
  const char* files[1] = {file};
  rocksdb_ingest_external_file(db, files, 1, io, &err);
  printf("ingest: %s\n", err ? err : "OK");
  free(err);
  rocksdb_close(db);
}

int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  char* err = NULL;
  system("rm -rf /tmp/up10 && mkdir -p /tmp/up10");

  /* A small table file, format_version 5, no compression. */
  rocksdb_options_t* o = rocksdb_options_create();
  rocksdb_block_based_table_options_t* bb = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_format_version(bb, 5);
  rocksdb_options_set_block_based_table_factory(o, bb);
  rocksdb_options_set_compression(o, rocksdb_no_compression);
  rocksdb_envoptions_t* eo = rocksdb_envoptions_create();
  rocksdb_sstfilewriter_t* w = rocksdb_sstfilewriter_create(eo, o);
  rocksdb_sstfilewriter_open(w, "/tmp/up10/src.sst", &err); CHECK(err);
  char k[16], v[16];
  for (int i = 0; i < 200; i++) {
    snprintf(k, sizeof k, "k%06d", i); snprintf(v, sizeof v, "v%06d", i);
    rocksdb_sstfilewriter_put(w, k, strlen(k), v, strlen(v), &err); CHECK(err);
  }
  rocksdb_sstfilewriter_finish(w, &err); CHECK(err);
  rocksdb_sstfilewriter_destroy(w);

  struct { const char* name; uint64_t offset, size; } cases[] = {
    {"metaindex offset past the end of the file", 1u << 20, 64},
    {"metaindex size 2^40", 0, (uint64_t)1 << 40},
    {"metaindex size 2^64 - 4 (size + trailer wraps)", 10, UINT64_MAX - 3},
  };
  for (int i = 0; i < 3; i++) {
    write_with_metaindex("/tmp/up10/src.sst", "/tmp/up10/bad.sst", cases[i].offset, cases[i].size);
    printf("%s: ", cases[i].name);
    pid_t pid = fork(); /* run each ingestion in a child: it may abort or crash */
    if (pid == 0) { ingest("/tmp/up10/bad.sst"); _exit(0); }
    int st = 0;
    waitpid(pid, &st, 0);
    if (WIFSIGNALED(st)) printf("process killed by signal %d\n", WTERMSIG(st));
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
metaindex offset past the end of the file: ingest: Corruption: truncated block read from /tmp/up10/bad.sst offset 1048576, expected 69 bytes, got 0
metaindex size 2^40: terminate called after throwing an instance of 'std::bad_alloc'
  what():  std::bad_alloc
process killed by signal 6
metaindex size 2^64 - 4 (size + trailer wraps): process killed by signal 11
```

### Suggested fix

Validate the handle against the file before allocating or reading, for example in `BlockFetcher` (or once in
`BlockBasedTable` where handles are decoded, since the file size is known there): fail with Corruption when
`handle.size() > file_size` or `handle.offset() > file_size - handle.size() - trailer size`, computed without overflow.
At minimum, compute `block_size_with_trailer_` with an overflow check and turn an allocation failure into
`Status::MemoryLimit` / Corruption instead of letting `std::bad_alloc` escape.

The managed port (RocksDb.Managed) returns Corruption "block too large" for both sizes (regression tests
`FooterHandleBeyondTheFileFailsWithNativeStatus`, `FooterHandlesBeyondTheFileMatchNative`).
