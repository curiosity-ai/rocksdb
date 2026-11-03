//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// rocksdb_bench: a db_bench-style benchmark written against the RocksDB C API
// only (include/rocksdb/c.h of the release that matches the librocksdb.so it
// is linked with). It is the "pure native" leg of the three-way comparison in
// dotnet/bench (C native vs rocksdb-sharp vs RocksDb.Managed): it implements
// exactly the same workloads, options, key/value generation, random streams
// and output format as the C# runner in
// dotnet/bench/RocksDb.Managed.Benchmarks/DbBench, so the difference between
// this program and the rocksdb-sharp engine is the cost of the managed
// binding (P/Invoke + marshalling), not of the storage engine.
//
// Keep the following in sync with DbBench/Workload.cs (and the warm-up with
// RunWarmup in DbBench/DbBenchRunner.cs):
//   - SplitMix64 PRNG and the per (benchmark, thread) seed derivation
//   - key format (db_bench GenerateKeyFromInt: 8-byte big-endian + '0' pad)
//   - value buffer (db_bench RandomGenerator / CompressibleString)
//   - histogram bucketing and percentile interpolation
//   - work split across threads and the output line formats

#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

#include "rocksdb/c.h"

// ---------------------------------------------------------------- flags

typedef struct {
  const char* benchmarks;
  int64_t num;
  int64_t reads;  // -1 = num
  int key_size;
  int value_size;
  int threads;
  int batch_size;     // puts per WriteBatch for fill benchmarks (1 = Put)
  int multiget_size;  // keys per MultiGet in multireadrandom
  int seek_nexts;     // Next() calls after each Seek in seekrandom
  int reverse_iterator; // seekrandom: SeekForPrev + Prev() instead of Seek + Next()
  bool disable_wal;
  bool sync;
  int64_t cache_size;
  const char* cache_type;  // lru | hyperclock
  int bloom_bits;
  int64_t write_buffer_size;
  int block_size;
  const char* compression;  // none | snappy | lz4 | zstd
  double compression_ratio;
  int64_t target_file_size_base;
  int64_t max_bytes_for_level_base;
  int level0_file_num_compaction_trigger;
  bool disable_auto_compactions;
  bool enable_blob_files;
  uint64_t min_blob_size;
  int max_background_jobs;
  unsigned max_subcompactions;
  uint64_t compaction_readahead_size;
  bool drop_page_cache;  // compact: evict the SST files from the page cache first
  bool use_existing_db;
  int64_t seed;
  const char* db;
  bool histogram;
  int txn_puts;
  bool warmup;            // warm-up phase on a separate DB (see warmup())
  int64_t warmup_ops;     // -1 = max(50k, num / 10), <= 200k, <= num
  int warmup_min_ms;      // minimum time per repeated warm-up benchmark
} Flags;

static Flags FLAGS = {
    .benchmarks = "fillseq,fillrandom,readrandom",
    .num = 1000000,
    .reads = -1,
    .key_size = 16,
    .value_size = 100,
    .threads = 1,
    .batch_size = 1,
    .multiget_size = 16,
    .seek_nexts = 10,
    .disable_wal = false,
    .sync = false,
    .cache_size = 8 << 20,
    .cache_type = "lru",
    .bloom_bits = 10,
    .write_buffer_size = 64 << 20,
    .block_size = 4096,
    .compression = "none",
    .compression_ratio = 0.5,
    .target_file_size_base = 64 << 20,
    .max_bytes_for_level_base = 256 << 20,
    .level0_file_num_compaction_trigger = 4,
    .disable_auto_compactions = false,
    .enable_blob_files = false,
    .min_blob_size = 0,
    .max_background_jobs = 2,
    .max_subcompactions = 1,
    .compaction_readahead_size = 2 * 1024 * 1024,
    .drop_page_cache = false,
    .use_existing_db = false,
    .seed = 301,
    .db = "/tmp/rocksdb-c-bench",
    .histogram = true,
    .txn_puts = 10,
    .warmup = true,
    .warmup_ops = -1,
    .warmup_min_ms = 1000,
};

static void usage(void) {
  fprintf(stderr,
          "usage: rocksdb_bench [--benchmarks a,b,...] [--num N] [--reads N] "
          "[--key_size 16] [--value_size 100]\n"
          "  [--threads 1] [--batch_size 1] [--multiget_size 16] "
          "[--seek_nexts 10] [--reverse_iterator] [--txn_puts 10] [--disable_wal] [--sync]\n"
          "  [--cache_size BYTES] [--cache_type lru|hyperclock] [--bloom_bits "
          "10] [--write_buffer_size BYTES]\n"
          "  [--block_size 4096] [--compression none|snappy|lz4|zstd] "
          "[--compression_ratio 0.5]\n"
          "  [--target_file_size_base BYTES] [--max_bytes_for_level_base BYTES] "
          "[--level0_file_num_compaction_trigger 4]\n"
          "  [--disable_auto_compactions] [--max_background_jobs 2] [--max_subcompactions 1]\n"
          "  [--compaction_readahead_size 2097152] [--drop_page_cache]\n"
          "  [--enable_blob_files] [--min_blob_size 0]\n"
          "  [--use_existing_db] [--seed 301] [--db PATH] [--histogram 1]\n"
          "  [--warmup true|false] [--warmup_ops N] [--warmup_min_ms 1000]\n"
          "benchmarks: fillseq fillrandom overwrite fillsync readrandom "
          "readmissing readseq readreverse\n"
          "            seekrandom multireadrandom readwhilewriting "
          "deleterandom flush compact\n"
          "            waitforcompaction stats txnoptimistic txnpessimistic\n");
}

static bool parse_bool(const char* v) {
  return strcmp(v, "1") == 0 || strcmp(v, "true") == 0 ||
         strcmp(v, "True") == 0;
}

static void parse_flags(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    const char* a = argv[i];
    if (strncmp(a, "--", 2) != 0) {
      fprintf(stderr, "unexpected argument %s\n", a);
      usage();
      exit(1);
    }
    char name[128];
    const char* value;
    const char* eq = strchr(a, '=');
    if (eq != NULL) {
      size_t n = (size_t)(eq - a - 2);
      if (n >= sizeof(name)) n = sizeof(name) - 1;
      memcpy(name, a + 2, n);
      name[n] = 0;
      value = eq + 1;
    } else {
      snprintf(name, sizeof(name), "%s", a + 2);
      if (i + 1 < argc && strncmp(argv[i + 1], "--", 2) != 0) {
        value = argv[++i];
      } else {
        value = "true";
      }
    }
#define FLAG(n) (strcmp(name, n) == 0)
    if (FLAG("benchmarks")) FLAGS.benchmarks = value;
    else if (FLAG("num")) FLAGS.num = strtoll(value, NULL, 10);
    else if (FLAG("reads")) FLAGS.reads = strtoll(value, NULL, 10);
    else if (FLAG("key_size")) FLAGS.key_size = atoi(value);
    else if (FLAG("value_size")) FLAGS.value_size = atoi(value);
    else if (FLAG("threads")) FLAGS.threads = atoi(value);
    else if (FLAG("batch_size")) FLAGS.batch_size = atoi(value);
    else if (FLAG("multiget_size")) FLAGS.multiget_size = atoi(value);
    else if (FLAG("seek_nexts")) FLAGS.seek_nexts = atoi(value);
    else if (FLAG("reverse_iterator")) FLAGS.reverse_iterator = parse_bool(value);
    else if (FLAG("disable_wal")) FLAGS.disable_wal = parse_bool(value);
    else if (FLAG("sync")) FLAGS.sync = parse_bool(value);
    else if (FLAG("cache_size")) FLAGS.cache_size = strtoll(value, NULL, 10);
    else if (FLAG("cache_type")) FLAGS.cache_type = value;
    else if (FLAG("bloom_bits")) FLAGS.bloom_bits = atoi(value);
    else if (FLAG("write_buffer_size"))
      FLAGS.write_buffer_size = strtoll(value, NULL, 10);
    else if (FLAG("block_size")) FLAGS.block_size = atoi(value);
    else if (FLAG("compression") || FLAG("compression_type"))
      FLAGS.compression = value;
    else if (FLAG("compression_ratio"))
      FLAGS.compression_ratio = strtod(value, NULL);
    else if (FLAG("target_file_size_base"))
      FLAGS.target_file_size_base = strtoll(value, NULL, 10);
    else if (FLAG("max_bytes_for_level_base"))
      FLAGS.max_bytes_for_level_base = strtoll(value, NULL, 10);
    else if (FLAG("level0_file_num_compaction_trigger"))
      FLAGS.level0_file_num_compaction_trigger = atoi(value);
    else if (FLAG("disable_auto_compactions"))
      FLAGS.disable_auto_compactions = parse_bool(value);
    else if (FLAG("enable_blob_files"))
      FLAGS.enable_blob_files = parse_bool(value);
    else if (FLAG("min_blob_size"))
      FLAGS.min_blob_size = strtoull(value, NULL, 10);
    else if (FLAG("max_background_jobs"))
      FLAGS.max_background_jobs = atoi(value);
    else if (FLAG("compaction_readahead_size"))
      FLAGS.compaction_readahead_size = strtoull(value, NULL, 10);
    else if (FLAG("drop_page_cache"))
      FLAGS.drop_page_cache = parse_bool(value);
    else if (FLAG("max_subcompactions"))
      FLAGS.max_subcompactions = (unsigned)strtoul(value, NULL, 10);
    else if (FLAG("use_existing_db")) FLAGS.use_existing_db = parse_bool(value);
    else if (FLAG("seed")) FLAGS.seed = strtoll(value, NULL, 10);
    else if (FLAG("db")) FLAGS.db = value;
    else if (FLAG("histogram")) FLAGS.histogram = parse_bool(value);
    else if (FLAG("txn_puts")) FLAGS.txn_puts = atoi(value);
    else if (FLAG("warmup")) FLAGS.warmup = parse_bool(value);
    else if (FLAG("warmup_ops")) FLAGS.warmup_ops = strtoll(value, NULL, 10);
    else if (FLAG("warmup_min_ms")) FLAGS.warmup_min_ms = atoi(value);
    else if (FLAG("engine") || FLAG("async")) { /* accepted for runner parity */ }
    else {
      fprintf(stderr, "unknown flag --%s\n", name);
      usage();
      exit(1);
    }
#undef FLAG
  }
  if (FLAGS.key_size < 8) {
    fprintf(stderr, "--key_size must be >= 8\n");
    exit(1);
  }
  if (FLAGS.threads < 1) FLAGS.threads = 1;
  if (FLAGS.batch_size < 1) FLAGS.batch_size = 1;
  if (FLAGS.multiget_size < 1) FLAGS.multiget_size = 1;
  if (FLAGS.txn_puts < 1) FLAGS.txn_puts = 1;
  if (FLAGS.reads < 0) FLAGS.reads = FLAGS.num;
  if (FLAGS.warmup_min_ms < 0) FLAGS.warmup_min_ms = 0;
  if (FLAGS.warmup_ops <= 0) {
    // Same default as DbBenchRunner.DefaultWarmupOps.
    int64_t w = FLAGS.num / 10 > 50000 ? FLAGS.num / 10 : 50000;
    if (w > 200000) w = 200000;
    if (w > FLAGS.num) w = FLAGS.num;
    FLAGS.warmup_ops = w > 0 ? w : 1;
  }
}

// ---------------------------------------------------------------- workload

// SplitMix64. The C# runner uses the identical generator (Workload.cs).
static inline uint64_t rng_next(uint64_t* s) {
  uint64_t z = (*s += 0x9E3779B97F4A7C15ULL);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

static inline uint64_t rng_seed(int64_t seed, int bench_index, int tid) {
  return (uint64_t)seed * 1000003ULL + (uint64_t)bench_index * 1009ULL +
         (uint64_t)tid;
}

static inline uint64_t rng_uniform(uint64_t* s, uint64_t n) {
  return rng_next(s) % n;
}

// db_bench GenerateKeyFromInt (no prefix): 8-byte big-endian number, padded
// with '0' characters up to key_size.
static inline void make_key(char* key, int key_size, uint64_t v) {
  for (int i = 0; i < 8; i++) key[i] = (char)((v >> ((7 - i) * 8)) & 0xFF);
  if (key_size > 8) memset(key + 8, '0', (size_t)(key_size - 8));
}

// db_bench RandomGenerator: >= max(1 MiB, value_size) bytes built from
// 100-byte pieces, each a random printable string of 100*ratio bytes repeated
// to fill the piece (test::CompressibleString). Values are consecutive
// windows of this buffer.
static char* g_values;
static size_t g_values_len;

static void build_value_buffer(void) {
  size_t target = 1048576;
  if ((size_t)FLAGS.value_size > target) target = (size_t)FLAGS.value_size;
  size_t cap = target + 100;
  g_values = (char*)malloc(cap);
  uint64_t s = (uint64_t)FLAGS.seed;
  int raw_len = (int)(100 * FLAGS.compression_ratio);
  if (raw_len < 1) raw_len = 1;
  if (raw_len > 100) raw_len = 100;
  char raw[100];
  g_values_len = 0;
  while (g_values_len < target) {
    for (int i = 0; i < raw_len; i++) raw[i] = (char)(' ' + rng_uniform(&s, 95));
    for (int i = 0; i < 100; i++) g_values[g_values_len + i] = raw[i % raw_len];
    g_values_len += 100;
  }
}

typedef struct {
  size_t pos;
} ValueGen;

static inline const char* next_value(ValueGen* g, int len) {
  if (g->pos + (size_t)len > g_values_len) g->pos = 0;
  const char* v = g_values + g->pos;
  g->pos += (size_t)len;
  return v;
}

// ---------------------------------------------------------------- histogram

// Log-linear buckets: values < 128 ns are exact; above, 64 sub-buckets per
// power of two (<= 1.6% relative error). Identical to Histogram.cs.
#define HIST_BUCKETS 3776

typedef struct {
  uint64_t counts[HIST_BUCKETS];
  uint64_t count;
  uint64_t min;
  uint64_t max;
  double sum;
} Histogram;

static void hist_init(Histogram* h) {
  memset(h, 0, sizeof(*h));
  h->min = UINT64_MAX;
}

static inline int hist_index(uint64_t v) {
  if (v < 128) return (int)v;
  int msb = 63 - __builtin_clzll(v);
  int shift = msb - 6;
  return 128 + (shift - 1) * 64 + (int)((v >> shift) - 64);
}

static inline void hist_bounds(int idx, double* lo, double* hi) {
  if (idx < 128) {
    *lo = idx;
    *hi = idx + 1;
    return;
  }
  int shift = (idx - 128) / 64 + 1;
  uint64_t sub = (uint64_t)((idx - 128) % 64 + 64);
  *lo = (double)(sub << shift);
  *hi = (double)((sub + 1) << shift);
}

static inline void hist_add(Histogram* h, uint64_t ns) {
  h->counts[hist_index(ns)]++;
  h->count++;
  if (ns < h->min) h->min = ns;
  if (ns > h->max) h->max = ns;
  h->sum += (double)ns;
}

static void hist_merge(Histogram* dst, const Histogram* src) {
  for (int i = 0; i < HIST_BUCKETS; i++) dst->counts[i] += src->counts[i];
  dst->count += src->count;
  if (src->min < dst->min) dst->min = src->min;
  if (src->max > dst->max) dst->max = src->max;
  dst->sum += src->sum;
}

// Percentile in nanoseconds, interpolated within the bucket like db_bench.
static double hist_percentile(const Histogram* h, double p) {
  if (h->count == 0) return 0;
  double threshold = (double)h->count * (p / 100.0);
  double cumulative = 0;
  for (int b = 0; b < HIST_BUCKETS; b++) {
    if (h->counts[b] == 0) continue;
    double c = (double)h->counts[b];
    cumulative += c;
    if (cumulative >= threshold) {
      double lo, hi;
      hist_bounds(b, &lo, &hi);
      double pos = (threshold - (cumulative - c)) / c;
      double r = lo + (hi - lo) * pos;
      if (r < (double)h->min) r = (double)h->min;
      if (r > (double)h->max) r = (double)h->max;
      return r;
    }
  }
  return (double)h->max;
}

// ---------------------------------------------------------------- engine

static rocksdb_t* g_db;
static rocksdb_options_t* g_options;
static rocksdb_block_based_table_options_t* g_table_options;
static rocksdb_cache_t* g_cache;
static rocksdb_writeoptions_t* g_wo;
static rocksdb_writeoptions_t* g_wo_sync;
static rocksdb_readoptions_t* g_ro;
static rocksdb_column_family_handle_t* g_default_cf;

static void check(char* err, const char* what) {
  if (err != NULL) {
    fprintf(stderr, "%s failed: %s\n", what, err);
    exit(3);
  }
}

static int compression_from_name(const char* n) {
  if (strcmp(n, "none") == 0 || strcmp(n, "no") == 0)
    return rocksdb_no_compression;
  if (strcmp(n, "snappy") == 0) return rocksdb_snappy_compression;
  if (strcmp(n, "lz4") == 0) return rocksdb_lz4_compression;
  if (strcmp(n, "zstd") == 0) return rocksdb_zstd_compression;
  fprintf(stderr, "unsupported --compression %s (none|snappy|lz4|zstd)\n", n);
  exit(1);
}

static void build_options(void) {
  g_table_options = rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_size(g_table_options,
                                             (size_t)FLAGS.block_size);
  rocksdb_block_based_options_set_format_version(g_table_options, 6);
  if (FLAGS.cache_size > 0) {
    if (strcmp(FLAGS.cache_type, "hyperclock") == 0) {
      // estimated_entry_charge 0 = auto-resizing HyperClock
      g_cache = rocksdb_cache_create_hyper_clock((size_t)FLAGS.cache_size, 0);
    } else {
      g_cache = rocksdb_cache_create_lru((size_t)FLAGS.cache_size);
    }
    rocksdb_block_based_options_set_block_cache(g_table_options, g_cache);
  } else {
    rocksdb_block_based_options_set_no_block_cache(g_table_options, 1);
  }
  if (FLAGS.bloom_bits > 0) {
    rocksdb_block_based_options_set_filter_policy(
        g_table_options,
        rocksdb_filterpolicy_create_bloom_full((double)FLAGS.bloom_bits));
  }
  g_options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(g_options, 1);
  rocksdb_options_set_compression(g_options,
                                  compression_from_name(FLAGS.compression));
  rocksdb_options_set_write_buffer_size(g_options,
                                        (size_t)FLAGS.write_buffer_size);
  rocksdb_options_set_target_file_size_base(
      g_options, (uint64_t)FLAGS.target_file_size_base);
  rocksdb_options_set_max_bytes_for_level_base(
      g_options, (uint64_t)FLAGS.max_bytes_for_level_base);
  rocksdb_options_set_level0_file_num_compaction_trigger(
      g_options, FLAGS.level0_file_num_compaction_trigger);
  rocksdb_options_set_disable_auto_compactions(g_options,
                                               FLAGS.disable_auto_compactions);
  rocksdb_options_set_enable_blob_files(g_options, FLAGS.enable_blob_files);
  rocksdb_options_set_min_blob_size(g_options, FLAGS.min_blob_size);
  rocksdb_options_set_max_background_jobs(g_options,
                                          FLAGS.max_background_jobs);
  rocksdb_options_compaction_readahead_size(g_options,
                                            (size_t)FLAGS.compaction_readahead_size);
  rocksdb_options_set_max_subcompactions(g_options, FLAGS.max_subcompactions);
  rocksdb_options_set_block_based_table_factory(g_options, g_table_options);

  g_wo = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(g_wo, FLAGS.sync);
  rocksdb_writeoptions_disable_WAL(g_wo, FLAGS.disable_wal);
  g_wo_sync = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(g_wo_sync, 1);
  g_ro = rocksdb_readoptions_create();
}

static int rm_cb(const char* path, const struct stat* sb, int flag,
                 struct FTW* ftw) {
  (void)sb;
  (void)flag;
  (void)ftw;
  return remove(path);
}

static void close_db(void) {
  if (g_db != NULL) {
    rocksdb_column_family_handle_destroy(g_default_cf);
    rocksdb_close(g_db);
    g_db = NULL;
  }
}

static void open_db(bool fresh) {
  close_db();
  if (fresh) nftw(FLAGS.db, rm_cb, 64, FTW_DEPTH | FTW_PHYS);
  mkdir(FLAGS.db, 0755);
  char* err = NULL;
  g_db = rocksdb_open(g_options, FLAGS.db, &err);
  check(err, "rocksdb_open");
  g_default_cf = rocksdb_get_default_column_family_handle(g_db);
}

// ---------------------------------------------------------------- benchmarks

typedef enum {
  B_FILLSEQ,
  B_FILLRANDOM,
  B_OVERWRITE,
  B_FILLSYNC,
  B_READRANDOM,
  B_READMISSING,
  B_READSEQ,
  B_READREVERSE,
  B_SEEKRANDOM,
  B_MULTIREADRANDOM,
  B_READWHILEWRITING,
  B_DELETERANDOM,
  B_FLUSH,
  B_COMPACT,
  B_WAITFORCOMPACTION,
  B_STATS,
  B_TXNOPTIMISTIC,
  B_TXNPESSIMISTIC,
  B_UNKNOWN
} BenchId;

static const char* kBenchNames[] = {
    "fillseq",     "fillrandom",   "overwrite",       "fillsync",
    "readrandom",  "readmissing",  "readseq",         "readreverse",
    "seekrandom",  "multireadrandom", "readwhilewriting", "deleterandom",
    "flush",       "compact",      "waitforcompaction", "stats",
    "txnoptimistic", "txnpessimistic"};

static BenchId bench_from_name(const char* n) {
  for (int i = 0; i < B_UNKNOWN; i++)
    if (strcmp(n, kBenchNames[i]) == 0) return (BenchId)i;
  return B_UNKNOWN;
}

static inline uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// User bytes (key + value) written by the fill/overwrite benchmarks and the
// readwhilewriting writer since the DB was created; the denominator of the
// write amplification printed by the stats benchmark.
static int64_t g_user_bytes_written;

typedef struct {
  BenchId bench;
  int bench_index;
  int64_t total_ops;
  pthread_barrier_t barrier;
  atomic_int readers_done;
  atomic_llong writer_ops;
} Shared;

typedef struct {
  int tid;
  Shared* shared;
  Histogram hist;
  int64_t ops;
  int64_t bytes;
  int64_t found;
  uint64_t start_ns;
  uint64_t end_ns;
} ThreadState;

// Number of operations thread tid performs out of total, and the index of its
// first operation (used for fillseq key ranges).
static inline int64_t thread_ops(int64_t total, int threads, int tid) {
  return total / threads + (tid < total % threads ? 1 : 0);
}

static inline int64_t thread_first(int64_t total, int threads, int tid) {
  int64_t base = total / threads, rem = total % threads;
  return base * tid + (tid < rem ? tid : rem);
}

static void do_writes(ThreadState* t, BenchId b, int64_t n, uint64_t* rng,
                      const rocksdb_writeoptions_t* wo) {
  char key[256];
  ValueGen vg = {0};
  int ks = FLAGS.key_size, vs = FLAGS.value_size;
  int64_t first = thread_first(t->shared->total_ops, FLAGS.threads, t->tid);
  rocksdb_writebatch_t* batch =
      FLAGS.batch_size > 1 ? rocksdb_writebatch_create() : NULL;
  int64_t i = 0;
  while (i < n) {
    uint64_t t0 = now_ns();
    char* err = NULL;
    if (batch == NULL) {
      uint64_t k = b == B_FILLSEQ ? (uint64_t)(first + i)
                                  : rng_uniform(rng, (uint64_t)FLAGS.num);
      make_key(key, ks, k);
      rocksdb_put(g_db, wo, key, (size_t)ks, next_value(&vg, vs), (size_t)vs,
                  &err);
      i++;
    } else {
      rocksdb_writebatch_clear(batch);
      for (int j = 0; j < FLAGS.batch_size && i < n; j++, i++) {
        uint64_t k = b == B_FILLSEQ ? (uint64_t)(first + i)
                                    : rng_uniform(rng, (uint64_t)FLAGS.num);
        make_key(key, ks, k);
        rocksdb_writebatch_put(batch, key, (size_t)ks, next_value(&vg, vs),
                               (size_t)vs);
      }
      rocksdb_write(g_db, wo, batch, &err);
    }
    check(err, "write");
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
  }
  if (batch != NULL) rocksdb_writebatch_destroy(batch);
  t->ops = n;
  t->bytes = n * (int64_t)(ks + vs);
}

static void do_reads(ThreadState* t, BenchId b, int64_t n, uint64_t* rng) {
  char key[256];
  int ks = FLAGS.key_size;
  for (int64_t i = 0; i < n; i++) {
    make_key(key, ks, rng_uniform(rng, (uint64_t)FLAGS.num));
    if (b == B_READMISSING) key[ks - 1] = '.';
    uint64_t t0 = now_ns();
    char* err = NULL;
    size_t vlen = 0;
    char* v = rocksdb_get(g_db, g_ro, key, (size_t)ks, &vlen, &err);
    check(err, "get");
    if (v != NULL) {
      t->found++;
      t->bytes += (int64_t)(ks + vlen);
      rocksdb_free(v);
    }
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
  }
  t->ops = n;
}

static void do_deletes(ThreadState* t, int64_t n, uint64_t* rng) {
  char key[256];
  int ks = FLAGS.key_size;
  for (int64_t i = 0; i < n; i++) {
    make_key(key, ks, rng_uniform(rng, (uint64_t)FLAGS.num));
    uint64_t t0 = now_ns();
    char* err = NULL;
    rocksdb_delete(g_db, g_wo, key, (size_t)ks, &err);
    check(err, "delete");
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
  }
  t->ops = n;
}

static void do_scan(ThreadState* t, bool reverse, int64_t n) {
  rocksdb_iterator_t* it = rocksdb_create_iterator(g_db, g_ro);
  if (reverse) rocksdb_iter_seek_to_last(it);
  else rocksdb_iter_seek_to_first(it);
  for (int64_t i = 0; i < n; i++) {
    uint64_t t0 = now_ns();
    if (!rocksdb_iter_valid(it)) {
      if (reverse) rocksdb_iter_seek_to_last(it);
      else rocksdb_iter_seek_to_first(it);
      if (!rocksdb_iter_valid(it)) break;  // empty DB
    }
    size_t kl, vl;
    rocksdb_iter_key(it, &kl);
    rocksdb_iter_value(it, &vl);
    t->bytes += (int64_t)(kl + vl);
    t->found++;
    if (reverse) rocksdb_iter_prev(it);
    else rocksdb_iter_next(it);
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
  }
  char* err = NULL;
  rocksdb_iter_get_error(it, &err);
  check(err, "iterator");
  rocksdb_iter_destroy(it);
  t->ops = n;
}

static void do_seeks(ThreadState* t, int64_t n, uint64_t* rng) {
  char key[256];
  int ks = FLAGS.key_size;
  rocksdb_iterator_t* it = rocksdb_create_iterator(g_db, g_ro);
  for (int64_t i = 0; i < n; i++) {
    make_key(key, ks, rng_uniform(rng, (uint64_t)FLAGS.num));
    uint64_t t0 = now_ns();
    if (FLAGS.reverse_iterator) rocksdb_iter_seek_for_prev(it, key, (size_t)ks);
    else rocksdb_iter_seek(it, key, (size_t)ks);
    if (rocksdb_iter_valid(it)) t->found++;
    for (int j = 0; j < FLAGS.seek_nexts && rocksdb_iter_valid(it); j++) {
      size_t kl, vl;
      rocksdb_iter_key(it, &kl);
      rocksdb_iter_value(it, &vl);
      t->bytes += (int64_t)(kl + vl);
      if (FLAGS.reverse_iterator) rocksdb_iter_prev(it);
      else rocksdb_iter_next(it);
    }
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
  }
  rocksdb_iter_destroy(it);
  t->ops = n;
}

static void do_multiget(ThreadState* t, int64_t n, uint64_t* rng) {
  int m = FLAGS.multiget_size, ks = FLAGS.key_size;
  char* keybuf = (char*)malloc((size_t)m * (size_t)ks);
  const char** keys = (const char**)malloc(sizeof(char*) * (size_t)m);
  size_t* sizes = (size_t*)malloc(sizeof(size_t) * (size_t)m);
  rocksdb_pinnableslice_t** values =
      (rocksdb_pinnableslice_t**)malloc(sizeof(void*) * (size_t)m);
  char** errs = (char**)malloc(sizeof(char*) * (size_t)m);
  for (int j = 0; j < m; j++) {
    keys[j] = keybuf + (size_t)j * (size_t)ks;
    sizes[j] = (size_t)ks;
  }
  int64_t i = 0;
  while (i < n) {
    int cnt = (int)((n - i) < m ? (n - i) : m);
    for (int j = 0; j < cnt; j++)
      make_key(keybuf + (size_t)j * (size_t)ks, ks,
               rng_uniform(rng, (uint64_t)FLAGS.num));
    uint64_t t0 = now_ns();
    rocksdb_batched_multi_get_cf(g_db, g_ro, g_default_cf, (size_t)cnt, keys,
                                 sizes, values, errs, false);
    for (int j = 0; j < cnt; j++) {
      check(errs[j], "multiget");
      if (values[j] != NULL) {
        size_t vl;
        rocksdb_pinnableslice_value(values[j], &vl);
        t->found++;
        t->bytes += (int64_t)(ks + vl);
        rocksdb_pinnableslice_destroy(values[j]);
      }
    }
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
    i += cnt;
  }
  free(keybuf);
  free(keys);
  free(sizes);
  free(values);
  free(errs);
  t->ops = n;
}

// ---------------------------------------------------------------- transactions

// txnoptimistic / txnpessimistic run against their own fresh transactional DB
// (<db>-<benchmark>): OptimisticTransactionDB or TransactionDB (WriteCommitted).
static rocksdb_optimistictransactiondb_t* g_otxn_db;
static rocksdb_transactiondb_t* g_txn_db;
static rocksdb_transactiondb_options_t* g_txn_db_options;
static rocksdb_transaction_options_t* g_txn_options;
static rocksdb_optimistictransaction_options_t* g_otxn_options;

static void open_txn_db(BenchId b, const char* name) {
  char path[1024];
  snprintf(path, sizeof(path), "%s-%s", FLAGS.db, name);
  nftw(path, rm_cb, 64, FTW_DEPTH | FTW_PHYS);
  mkdir(path, 0755);
  char* err = NULL;
  if (b == B_TXNOPTIMISTIC) {
    g_otxn_options = rocksdb_optimistictransaction_options_create();
    g_otxn_db = rocksdb_optimistictransactiondb_open(g_options, path, &err);
    check(err, "rocksdb_optimistictransactiondb_open");
  } else {
    g_txn_db_options = rocksdb_transactiondb_options_create();
    g_txn_options = rocksdb_transaction_options_create();
    g_txn_db = rocksdb_transactiondb_open(g_options, g_txn_db_options, path, &err);
    check(err, "rocksdb_transactiondb_open");
  }
}

static void close_txn_db(void) {
  if (g_otxn_db != NULL) {
    rocksdb_optimistictransactiondb_close(g_otxn_db);
    rocksdb_optimistictransaction_options_destroy(g_otxn_options);
    g_otxn_db = NULL;
  }
  if (g_txn_db != NULL) {
    rocksdb_transactiondb_close(g_txn_db);
    rocksdb_transaction_options_destroy(g_txn_options);
    rocksdb_transactiondb_options_destroy(g_txn_db_options);
    g_txn_db = NULL;
  }
}

static atomic_llong g_txn_retries;

// One op = one transaction of --txn_puts random puts; conflicts (Busy /
// TryAgain / TimedOut) are rolled back and retried.
static void do_txns(ThreadState* t, BenchId b, int64_t n, uint64_t* rng) {
  int ks = FLAGS.key_size, vs = FLAGS.value_size, puts = FLAGS.txn_puts;
  char* keys = (char*)malloc((size_t)ks * (size_t)puts);
  ValueGen vg = {0};
  rocksdb_transaction_t* txn = NULL;
  int64_t retries = 0;
  for (int64_t i = 0; i < n; i++) {
    for (int j = 0; j < puts; j++)
      make_key(keys + (size_t)j * (size_t)ks, ks,
               rng_uniform(rng, (uint64_t)FLAGS.num));
    const char* v = next_value(&vg, vs);
    uint64_t t0 = now_ns();
    for (;;) {
      txn = b == B_TXNOPTIMISTIC
                ? rocksdb_optimistictransaction_begin(g_otxn_db, g_wo,
                                                      g_otxn_options, txn)
                : rocksdb_transaction_begin(g_txn_db, g_wo, g_txn_options, txn);
      char* err = NULL;
      for (int j = 0; j < puts && err == NULL; j++)
        rocksdb_transaction_put(txn, keys + (size_t)j * (size_t)ks, (size_t)ks,
                                v, (size_t)vs, &err);
      if (err == NULL) rocksdb_transaction_commit(txn, &err);
      if (err == NULL) break;
      rocksdb_free(err);
      err = NULL;
      rocksdb_transaction_rollback(txn, &err);
      if (err != NULL) rocksdb_free(err);
      retries++;
    }
    if (FLAGS.histogram) hist_add(&t->hist, now_ns() - t0);
  }
  if (txn != NULL) rocksdb_transaction_destroy(txn);
  free(keys);
  atomic_fetch_add(&g_txn_retries, retries);
  t->ops = n;
  t->bytes = n * puts * (ks + vs);
}

// readwhilewriting: the writer (tid == threads) writes random keys until all
// readers are done.
static void do_background_writer(ThreadState* t, uint64_t* rng) {
  char key[256];
  ValueGen vg = {0};
  int ks = FLAGS.key_size, vs = FLAGS.value_size;
  int64_t n = 0;
  while (atomic_load_explicit(&t->shared->readers_done,
                              memory_order_relaxed) < FLAGS.threads) {
    make_key(key, ks, rng_uniform(rng, (uint64_t)FLAGS.num));
    char* err = NULL;
    rocksdb_put(g_db, g_wo, key, (size_t)ks, next_value(&vg, vs), (size_t)vs,
                &err);
    check(err, "put");
    n++;
  }
  atomic_store(&t->shared->writer_ops, n);
}

static void* thread_main(void* arg) {
  ThreadState* t = (ThreadState*)arg;
  Shared* s = t->shared;
  uint64_t rng = rng_seed(FLAGS.seed, s->bench_index, t->tid);
  int64_t n = thread_ops(s->total_ops, FLAGS.threads, t->tid);
  pthread_barrier_wait(&s->barrier);
  t->start_ns = now_ns();
  switch (s->bench) {
    case B_FILLSEQ:
    case B_FILLRANDOM:
    case B_OVERWRITE:
      do_writes(t, s->bench, n, &rng, g_wo);
      break;
    case B_FILLSYNC:
      do_writes(t, s->bench, n, &rng, g_wo_sync);
      break;
    case B_READRANDOM:
    case B_READMISSING:
      do_reads(t, s->bench, n, &rng);
      break;
    case B_READWHILEWRITING:
      if (t->tid == FLAGS.threads) {
        do_background_writer(t, &rng);
      } else {
        do_reads(t, B_READRANDOM, n, &rng);
        atomic_fetch_add(&s->readers_done, 1);
      }
      break;
    case B_READSEQ:
      do_scan(t, false, n);
      break;
    case B_READREVERSE:
      do_scan(t, true, n);
      break;
    case B_SEEKRANDOM:
      do_seeks(t, n, &rng);
      break;
    case B_MULTIREADRANDOM:
      do_multiget(t, n, &rng);
      break;
    case B_DELETERANDOM:
      do_deletes(t, n, &rng);
      break;
    case B_TXNOPTIMISTIC:
    case B_TXNPESSIMISTIC:
      do_txns(t, s->bench, n, &rng);
      break;
    default:
      break;
  }
  t->end_ns = now_ns();
  return NULL;
}

static const char* kEngineName = "c-native";

// Warm-up state: report() prints nothing while g_quiet is set, and
// run_threaded() runs g_ops_override ops instead of --num / --reads when >= 0.
static bool g_quiet;
static int64_t g_ops_override = -1;

static void report(const char* name, int64_t ops, double seconds,
                   int64_t bytes, int64_t found, bool show_found,
                   const Histogram* h, const char* extra) {
  if (g_quiet) return;
  double micros_per_op = ops > 0 ? seconds * 1e6 / (double)ops : 0;
  double ops_per_sec = seconds > 0 ? (double)ops / seconds : 0;
  double mbs = seconds > 0 ? (double)bytes / 1048576.0 / seconds : 0;
  double p50 = 0, p99 = 0, p999 = 0;
  if (h != NULL && h->count > 0) {
    p50 = hist_percentile(h, 50) / 1000.0;
    p99 = hist_percentile(h, 99) / 1000.0;
    p999 = hist_percentile(h, 99.9) / 1000.0;
  }
  printf("%-17s : %11.3f micros/op %10.0f ops/sec %9.3f seconds %10" PRId64
         " operations; %8.1f MB/s",
         name, micros_per_op, ops_per_sec, seconds, ops, mbs);
  if (h != NULL && h->count > 0)
    printf(" | p50 %.3f p99 %.3f p99.9 %.3f us", p50, p99, p999);
  if (show_found) printf(" (%" PRId64 " of %" PRId64 " found)", found, ops);
  if (extra != NULL && extra[0] != 0) printf(" %s", extra);
  printf("\n");
  // Machine-readable line; the C# runner prints the same columns and fills
  // the allocation/GC/JIT columns.
  printf("RESULT,%s,%s,%d,%" PRId64 ",%.6f,%.4f,%.1f,%.2f,%.4f,%.4f,%.4f,%" PRId64
         ",,,,,,\n",
         kEngineName, name, FLAGS.threads, ops, seconds, micros_per_op,
         ops_per_sec, mbs, p50, p99, p999, found);
  fflush(stdout);
}

static void run_threaded(BenchId b, int bench_index, const char* name) {
  Shared s;
  memset(&s, 0, sizeof(s));
  s.bench = b;
  s.bench_index = bench_index;
  bool is_read = b == B_READRANDOM || b == B_READMISSING || b == B_READSEQ ||
                 b == B_READREVERSE || b == B_SEEKRANDOM ||
                 b == B_MULTIREADRANDOM || b == B_READWHILEWRITING;
  int64_t nominal = g_ops_override >= 0 ? g_ops_override
                    : is_read           ? FLAGS.reads
                                        : FLAGS.num;
  switch (b) {
    case B_FILLSYNC:
      s.total_ops = nominal / 1000 > 0 ? nominal / 1000 : 1;
      break;
    case B_TXNOPTIMISTIC:
    case B_TXNPESSIMISTIC:
      // one op = one transaction (as many puts in total as fillrandom)
      s.total_ops = nominal / FLAGS.txn_puts > 0 ? nominal / FLAGS.txn_puts : 1;
      atomic_store(&g_txn_retries, 0);
      break;
    default:
      s.total_ops = nominal;
      break;
  }
  int nthreads = FLAGS.threads + (b == B_READWHILEWRITING ? 1 : 0);
  atomic_init(&s.readers_done, 0);
  atomic_init(&s.writer_ops, 0);
  pthread_barrier_init(&s.barrier, NULL, (unsigned)nthreads);
  ThreadState* ts = (ThreadState*)calloc((size_t)nthreads, sizeof(ThreadState));
  pthread_t* th = (pthread_t*)calloc((size_t)nthreads, sizeof(pthread_t));
  for (int i = 0; i < nthreads; i++) {
    ts[i].tid = i;
    ts[i].shared = &s;
    hist_init(&ts[i].hist);
    pthread_create(&th[i], NULL, thread_main, &ts[i]);
  }
  for (int i = 0; i < nthreads; i++) pthread_join(th[i], NULL);
  pthread_barrier_destroy(&s.barrier);

  Histogram* merged = (Histogram*)malloc(sizeof(Histogram));
  hist_init(merged);
  uint64_t start = UINT64_MAX, end = 0;
  int64_t ops = 0, bytes = 0, found = 0;
  for (int i = 0; i < FLAGS.threads; i++) {  // readers only for rww
    hist_merge(merged, &ts[i].hist);
    if (ts[i].start_ns < start) start = ts[i].start_ns;
    if (ts[i].end_ns > end) end = ts[i].end_ns;
    ops += ts[i].ops;
    bytes += ts[i].bytes;
    found += ts[i].found;
  }
  bool show_found = b == B_READRANDOM || b == B_READMISSING ||
                    b == B_MULTIREADRANDOM || b == B_READWHILEWRITING ||
                    b == B_SEEKRANDOM;
  char extra[96] = "";
  if (b == B_READWHILEWRITING)
    snprintf(extra, sizeof(extra), "(writer: %lld puts)",
             (long long)atomic_load(&s.writer_ops));
  if (b == B_TXNOPTIMISTIC || b == B_TXNPESSIMISTIC)
    snprintf(extra, sizeof(extra), "(%d puts/txn, %lld retries)",
             FLAGS.txn_puts, (long long)atomic_load(&g_txn_retries));
  if (b == B_FILLSEQ || b == B_FILLRANDOM || b == B_OVERWRITE ||
      b == B_FILLSYNC)
    g_user_bytes_written += bytes;
  else if (b == B_READWHILEWRITING)
    g_user_bytes_written += (int64_t)atomic_load(&s.writer_ops) *
                            (FLAGS.key_size + FLAGS.value_size);
  report(name, ops, (double)(end - start) / 1e9, bytes, found, show_found,
         merged, extra);
  free(merged);
  free(ts);
  free(th);
}

// fsync + POSIX_FADV_DONTNEED for every *.sst file of the DB (--drop_page_cache).
static void drop_page_cache(void) {
  DIR* d = opendir(FLAGS.db);
  if (d == NULL) return;
  struct dirent* e;
  char path[4096];
  while ((e = readdir(d)) != NULL) {
    size_t n = strlen(e->d_name);
    if (n < 4 || strcmp(e->d_name + n - 4, ".sst") != 0) continue;
    snprintf(path, sizeof(path), "%s/%s", FLAGS.db, e->d_name);
    int fd = open(path, O_RDWR);
    if (fd < 0) continue;
    fsync(fd);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);
  }
  closedir(d);
}

static void run_single(BenchId b, const char* name) {
  if (b == B_COMPACT && FLAGS.drop_page_cache) drop_page_cache();
  uint64_t t0 = now_ns();
  char* err = NULL;
  if (b == B_FLUSH) {
    rocksdb_flushoptions_t* fo = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(fo, 1);
    rocksdb_flush(g_db, fo, &err);
    rocksdb_flushoptions_destroy(fo);
    check(err, "flush");
  } else if (b == B_COMPACT) {
    rocksdb_compact_range(g_db, NULL, 0, NULL, 0);
  } else {
    rocksdb_wait_for_compact_options_t* wo =
        rocksdb_wait_for_compact_options_create();
    rocksdb_wait_for_compact(g_db, wo, &err);
    rocksdb_wait_for_compact_options_destroy(wo);
    check(err, "wait_for_compact");
  }
  double seconds = (double)(now_ns() - t0) / 1e9;
  report(name, 1, seconds, 0, 0, false, NULL, "");
}

// The stats benchmark; same output as PrintStats in DbBenchRunner.cs.
static void print_stats(void) {
  char levels[256] = "";
  size_t len = 0;
  for (int l = 0; l < 7; l++) {
    char prop[64];
    snprintf(prop, sizeof(prop), "rocksdb.num-files-at-level%d", l);
    char* v = rocksdb_property_value(g_db, prop);
    len += (size_t)snprintf(levels + len, sizeof(levels) - len, "%s%s",
                            l > 0 ? " " : "", v != NULL ? v : "?");
    rocksdb_free(v);
  }
  printf("%-17s : files per level [%s]\n", "stats", levels);
  double written = -1;
  char* stats = rocksdb_property_value(g_db, "rocksdb.stats");
  static const char kMarker[] = "Cumulative compaction: ";
  const char* line = stats != NULL ? strstr(stats, kMarker) : NULL;
  if (line != NULL) {
    const char* nl = strchr(line, '\n');
    int n = nl != NULL ? (int)(nl - line) : (int)strlen(line);
    char* end = NULL;
    double gb = strtod(line + sizeof(kMarker) - 1, &end);
    if (end != NULL && strncmp(end, " GB write", 9) == 0)
      written = gb * 1073741824.0;
    printf("%.*s\n", n, line);
  }
  rocksdb_free(stats);
  char* pending =
      rocksdb_property_value(g_db, "rocksdb.estimate-pending-compaction-bytes");
  int64_t user = g_user_bytes_written;
  double amp = written >= 0 && user > 0 ? written / (double)user : 0;
  if (written < 0) written = 0;
  printf("%-17s : user %.1f MB, flush+compaction written %.1f MB, write-amp "
         "%.2f, pending compaction %s bytes\n",
         "stats", (double)user / 1048576.0, written / 1048576.0, amp,
         pending != NULL ? pending : "?");
  printf("STATS,%s,%s,%" PRId64 ",%.0f,%.2f,%s\n", kEngineName, levels, user,
         written, amp, pending != NULL ? pending : "?");
  rocksdb_free(pending);
  fflush(stdout);
}

// ---------------------------------------------------------------- warm-up

// Same constants and algorithm as RunWarmup in DbBench/DbBenchRunner.cs. The C
// leg has no JIT to warm up; it runs the identical warm-up workload so that all
// three legs are measured under the same page cache / allocator / file system
// conditions.
enum {
  kWarmupMinReps = 32,
  kWarmupMaxMsPerBenchmark = 30000,
  kWarmupFlushCompactionCycles = 4,
  kWarmupFlushesPerCycle = 16,
  kSettleMs = 200,
};

static void sleep_ms(int ms) {
  struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
  while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
  }
}

static void destroy_options(void) {
  rocksdb_writeoptions_destroy(g_wo);
  rocksdb_writeoptions_destroy(g_wo_sync);
  rocksdb_readoptions_destroy(g_ro);
  rocksdb_options_destroy(g_options);
  rocksdb_block_based_options_destroy(g_table_options);
  if (g_cache != NULL) rocksdb_cache_destroy(g_cache);
  g_cache = NULL;
}

// Plan step: a benchmark, or B_UNKNOWN = close and reopen the DB, then pause
// (the C# runner waits for the JIT to quiesce there).
typedef struct {
  BenchId bench;
  bool repeat;
} WarmupStep;

// Runs every requested benchmark on <db>-warmup (same options, key range
// --warmup_ops), then deletes it and rebuilds the options (fresh block cache):
//   - fillrandom first when the list does not start with a fill;
//   - repeated benchmarks (all but flush/compact/waitforcompaction; stats is
//     skipped) run repetitions of warmup_ops / 32 ops until >= 32
//     repetitions, >= warmup_ops ops and >= warmup_min_ms ms;
//   - flush / compact / waitforcompaction run once where they appear;
//   - 4 cycles of: reopen, 16 x (overwrite of warmup_ops / 50 keys + flush),
//     waitforcompaction, compact.
static void warmup(const BenchId* benches, int count) {
  static char path[1024];
  Flags saved = FLAGS;
  snprintf(path, sizeof(path), "%s-warmup", saved.db);
  FLAGS.db = path;
  FLAGS.num = saved.warmup_ops;
  FLAGS.reads = saved.warmup_ops;
  FLAGS.use_existing_db = false;

  int cap = count + 1 +
            kWarmupFlushCompactionCycles * (3 + 2 * kWarmupFlushesPerCycle);
  WarmupStep* plan = (WarmupStep*)calloc((size_t)cap, sizeof(WarmupStep));
  int n = 0;
  if (count == 0 || !(benches[0] == B_FILLSEQ || benches[0] == B_FILLRANDOM ||
                      benches[0] == B_FILLSYNC))
    plan[n++] = (WarmupStep){B_FILLRANDOM, true};
  for (int i = 0; i < count; i++) {
    BenchId b = benches[i];
    if (b == B_STATS) continue;
    plan[n++] = (WarmupStep){
        b, !(b == B_FLUSH || b == B_COMPACT || b == B_WAITFORCOMPACTION)};
  }
  for (int c = 0; c < kWarmupFlushCompactionCycles; c++) {
    plan[n++] = (WarmupStep){B_UNKNOWN, false};
    for (int i = 0; i < kWarmupFlushesPerCycle; i++) {
      plan[n++] = (WarmupStep){B_OVERWRITE, false};
      plan[n++] = (WarmupStep){B_FLUSH, false};
    }
    plan[n++] = (WarmupStep){B_WAITFORCOMPACTION, false};
    plan[n++] = (WarmupStep){B_COMPACT, false};
  }

  printf("%-17s : start\n", "warmup");
  fflush(stdout);
  int64_t saved_user_bytes = g_user_bytes_written;
  int64_t chunk = FLAGS.num / kWarmupMinReps > 0 ? FLAGS.num / kWarmupMinReps : 1;
  uint64_t total0 = now_ns();
  g_quiet = true;
  for (int index = 0; index < n; index++) {
    BenchId b = plan[index].bench;
    if (b == B_UNKNOWN) {
      open_db(false);
      sleep_ms(100);
      printf("%-17s : reopen\n", "warmup");
      fflush(stdout);
      continue;
    }
    const char* name = kBenchNames[b];
    bool fresh = b == B_FILLSEQ || b == B_FILLRANDOM || b == B_FILLSYNC;
    if (g_db == NULL || fresh) open_db(true);
    bool txn = b == B_TXNOPTIMISTIC || b == B_TXNPESSIMISTIC;
    if (txn) open_txn_db(b, name);
    uint64_t t0 = now_ns();
    int reps = 0;
    int64_t ops = 0;
    if (!plan[index].repeat) {
      g_ops_override = FLAGS.num / 50 > 0 ? FLAGS.num / 50 : 1;
      if (b == B_FLUSH || b == B_COMPACT || b == B_WAITFORCOMPACTION)
        run_single(b, name);
      else
        run_threaded(b, index, name);
      reps = 1;
      ops = g_ops_override;
    } else {
      g_ops_override = chunk;
      for (;;) {
        uint64_t ms = (now_ns() - t0) / 1000000;
        if (ms >= kWarmupMaxMsPerBenchmark) break;
        if (reps >= kWarmupMinReps && ops >= FLAGS.num &&
            ms >= (uint64_t)FLAGS.warmup_min_ms)
          break;
        // A different random key stream per repetition (same derivation as
        // the C# runner).
        run_threaded(b, index + 1000 * (reps + 1), name);
        reps++;
        ops += chunk;
      }
    }
    if (txn) {
      close_txn_db();
      char txn_path[1100];
      snprintf(txn_path, sizeof(txn_path), "%s-%s", FLAGS.db, name);
      nftw(txn_path, rm_cb, 64, FTW_DEPTH | FTW_PHYS);
    }
    printf("%-17s : %s %d reps %" PRId64 " ops %.3f s\n", "warmup", name, reps,
           ops, (double)(now_ns() - t0) / 1e9);
    fflush(stdout);
  }
  g_quiet = false;
  g_ops_override = -1;
  close_db();
  nftw(FLAGS.db, rm_cb, 64, FTW_DEPTH | FTW_PHYS);
  free(plan);
  FLAGS = saved;
  g_user_bytes_written = saved_user_bytes;
  // Fresh options and block cache for the measured run.
  destroy_options();
  build_options();
  printf("%-17s : done in %.1f s\n", "warmup", (double)(now_ns() - total0) / 1e9);
  fflush(stdout);
}

static void print_header(void) {
  struct utsname u;
  uname(&u);
  printf("Engine:      %s (RocksDB C API)\n", kEngineName);
  printf("Keys:        %d bytes each\n", FLAGS.key_size);
  printf("Values:      %d bytes each (%.2f compression ratio)\n",
         FLAGS.value_size, FLAGS.compression_ratio);
  printf("Entries:     %" PRId64 " (reads %" PRId64 ")\n", FLAGS.num,
         FLAGS.reads);
  printf("Threads:     %d\n", FLAGS.threads);
  printf("Options:     compression=%s cache=%s:%" PRId64
         " bloom_bits=%d write_buffer_size=%" PRId64
         " block_size=%d batch_size=%d multiget_size=%d seek_nexts=%d "
         "disable_wal=%d sync=%d\n",
         FLAGS.compression, FLAGS.cache_type, FLAGS.cache_size,
         FLAGS.bloom_bits, FLAGS.write_buffer_size, FLAGS.block_size,
         FLAGS.batch_size, FLAGS.multiget_size, FLAGS.seek_nexts,
         FLAGS.disable_wal, FLAGS.sync);
  printf("Compaction:  target_file_size_base=%" PRId64
         " max_bytes_for_level_base=%" PRId64
         " level0_file_num_compaction_trigger=%d disable_auto_compactions=%d "
         "max_background_jobs=%d\n",
         FLAGS.target_file_size_base, FLAGS.max_bytes_for_level_base,
         FLAGS.level0_file_num_compaction_trigger,
         FLAGS.disable_auto_compactions, FLAGS.max_background_jobs);
  printf("Blob files:  enable_blob_files=%d min_blob_size=%" PRIu64 "\n",
         FLAGS.enable_blob_files, FLAGS.min_blob_size);
  printf("DB path:     %s\n", FLAGS.db);
  if (FLAGS.warmup)
    printf("Warm-up:     on (%" PRId64
           " ops per benchmark, >= %d repetitions and >= %d ms, then %d x %d "
           "flushes + compactions; separate DB %s-warmup)\n",
           FLAGS.warmup_ops, kWarmupMinReps, FLAGS.warmup_min_ms,
           kWarmupFlushCompactionCycles, kWarmupFlushesPerCycle, FLAGS.db);
  else
    printf("Warm-up:     off\n");
  printf("Kernel:      %s %s\n", u.sysname, u.release);
  printf("------------------------------------------------\n");
  fflush(stdout);
}

int main(int argc, char** argv) {
  parse_flags(argc, argv);
  build_value_buffer();
  build_options();
  print_header();

  // Parse the whole list first (the warm-up runs it before the measurement).
  char* list = strdup(FLAGS.benchmarks);
  int count = 0;
  BenchId* benches = (BenchId*)calloc(strlen(FLAGS.benchmarks) + 1, sizeof(BenchId));
  for (char* save = NULL, *name = strtok_r(list, ",", &save); name != NULL;
       name = strtok_r(NULL, ",", &save)) {
    while (*name == ' ') name++;
    BenchId b = bench_from_name(name);
    if (b == B_UNKNOWN) {
      fprintf(stderr, "unknown benchmark %s\n", name);
      return 1;
    }
    benches[count++] = b;
  }
  free(list);
  if (FLAGS.warmup) warmup(benches, count);

  for (int index = 0; index < count; index++) {
    BenchId b = benches[index];
    const char* name = kBenchNames[b];
    bool fresh = !FLAGS.use_existing_db &&
                 (b == B_FILLSEQ || b == B_FILLRANDOM || b == B_FILLSYNC);
    if (g_db == NULL || fresh) {
      open_db(fresh);
      if (fresh) g_user_bytes_written = 0;
    }
    if (b == B_STATS) {
      print_stats();
      continue;
    }
    // Settle pause, as long as the C# runner's (which waits for the JIT).
    if (FLAGS.warmup) sleep_ms(kSettleMs);
    if (b == B_FLUSH || b == B_COMPACT || b == B_WAITFORCOMPACTION)
      run_single(b, name);
    else if (b == B_TXNOPTIMISTIC || b == B_TXNPESSIMISTIC) {
      open_txn_db(b, name);
      run_threaded(b, index, name);
      close_txn_db();
    }
    else run_threaded(b, index, name);
  }
  free(benches);
  close_db();
  destroy_options();
  free(g_values);
  return 0;
}
