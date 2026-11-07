#!/usr/bin/env bash
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).
#
# Three-way RocksDB benchmark matrix: C native (bench/native-c, RocksDB C API) vs rocksdb-sharp (NuGet RocksDB,
# same librocksdb.so) vs RocksDb.Managed (sync and async APIs). Builds everything in Release, runs every
# configuration of the matrix on every engine (each engine in its own process, on a fresh DB directory), and writes
#   bench/results/<date>-<host>.md       (comparison tables, LSM shape / write-amp, machine info)
#   bench/results/<date>-<host>.csv      (raw RESULT rows)
#   bench/results/<date>-<host>.lsm.csv  (raw STATS rows of the compaction-heavy configuration)
#
# Usage: bench/run-all.sh            (from dotnet/ or anywhere)
# Every knob is an environment variable:
#   NUM=1000000            entries for fill benchmarks (100-byte values)
#   NUM_LARGE=$NUM         entries for the large-value variant
#   READS=$NUM             read operations for read benchmarks (total across threads)
#   READS_LARGE=$READS     read operations for the large-value variant (default: READS, else NUM_LARGE)
#   VALUE_SIZES="100 1024" value sizes; sizes > 100 use NUM_LARGE
#   THREADS_LIST="1 4"     --threads values
#   COMPRESSIONS="none lz4"
#   ENGINES=c-native,rocksdb-sharp,managed,managed-async
#   BENCHMARKS=fillseq,fillrandom,overwrite,flush,readrandom,readmissing,seekrandom,readseq,readreverse,multireadrandom,readwhilewriting,compact,deleterandom,fillsync
#   DB_BASE=/tmp/rocksdb-bench   base directory for the (fresh, deleted afterwards) DB directories
#   OUT_DIR=bench/results        TFM=net10.0     EXTRA_ARGS="--cache_size 8388608 ..." (passed to every engine)
#   OUT_NAME=<date>-<host>       base name of the output files
#   NOTES="free text added to the report"
#   WARMUP=true                  warm-up phase in every engine (all three legs, same workload): every benchmark of the
#                                list runs on a separate temporary DB first, so the .NET engines are measured with
#                                JIT-compiled, tiered-up code (see "Warm-up" in bench/README.md); false = cold process
#   WARMUP_OPS=                  --warmup_ops (default max(50k, num/10), <= 200k, <= num)
#   WARMUP_MIN_MS=               --warmup_min_ms (default 1000)
#   REPEATS=1                    repetitions: every engine x configuration x benchmark runs REPEATS times, each on a fresh
#                                DB; repetition r runs all configurations (engines A B C D in turn) before repetition r+1
#                                starts, so drift spreads over all engines. With REPEATS > 1 the CSV rows carry the
#                                repetition number and the report shows medians, min-max, CV and bootstrap CIs.
#   LOAD_MAX=                    before each repetition, wait (up to LOAD_WAIT_MAX_S=900 s) until the 1-minute load
#                                average is <= LOAD_MAX (e.g. 1.5); the load average at the start of every repetition is
#                                recorded in the report
#   RUN_MATRIX=1                 run the matrix above
#   RUN_COMPACTION=1             also run one compaction-heavy configuration (label "compaction"): small memtables and
#                                SST files so the LSM tree gets several levels; reports LSM shape + write amplification
#   COMPACTION_NUM=$NUM  COMPACTION_THREADS=1  COMPACTION_COMPRESSION=none  COMPACTION_VALUE_SIZE=100
#   COMPACTION_BENCHMARKS=fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom
#   COMPACTION_ARGS="--write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216"
#   RUN_BLOB=0                   run one integrated BlobDB configuration (label "blob"): values in blob files
#   BLOB_NUM=$NUM  BLOB_VALUE_SIZE=1024  BLOB_THREADS=1  BLOB_COMPRESSION=none
#   BLOB_BENCHMARKS=fillrandom,overwrite,flush,waitforcompaction,stats,readrandom,multireadrandom,seekrandom,readseq,compact,readrandom
#   BLOB_ARGS="--enable_blob_files 1 --min_blob_size 512 --write_buffer_size 16777216"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOTNET_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DOTNET_DIR}"

if [[ -x /root/.dotnet/dotnet && -z "${DOTNET_ROOT:-}" ]]; then
  export DOTNET_ROOT=/root/.dotnet
  export PATH="/root/.dotnet:${PATH}"
fi
export DOTNET_NOLOGO=1 DOTNET_CLI_TELEMETRY_OPTOUT=1

NUM="${NUM:-1000000}"
NUM_LARGE="${NUM_LARGE:-${NUM}}"
READS="${READS:-}"
READS_LARGE="${READS_LARGE:-${READS}}"
VALUE_SIZES="${VALUE_SIZES:-100 1024}"
THREADS_LIST="${THREADS_LIST:-1 4}"
COMPRESSIONS="${COMPRESSIONS:-none lz4}"
ENGINES="${ENGINES:-c-native,rocksdb-sharp,managed,managed-async}"
BENCHMARKS="${BENCHMARKS:-fillseq,fillrandom,overwrite,flush,readrandom,readmissing,seekrandom,readseq,readreverse,multireadrandom,readwhilewriting,compact,deleterandom,fillsync}"
DB_BASE="${DB_BASE:-/tmp/rocksdb-bench}"
OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/results}"
TFM="${TFM:-net10.0}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
WARMUP="${WARMUP:-true}"
WARMUP_ARGS="--warmup ${WARMUP}"
if [[ -n "${WARMUP_OPS:-}" ]]; then WARMUP_ARGS="${WARMUP_ARGS} --warmup_ops ${WARMUP_OPS}"; fi
if [[ -n "${WARMUP_MIN_MS:-}" ]]; then WARMUP_ARGS="${WARMUP_ARGS} --warmup_min_ms ${WARMUP_MIN_MS}"; fi
REPEATS="${REPEATS:-1}"
LOAD_MAX="${LOAD_MAX:-}"
LOAD_WAIT_MAX_S="${LOAD_WAIT_MAX_S:-900}"
RUN_MATRIX="${RUN_MATRIX:-1}"
RUN_COMPACTION="${RUN_COMPACTION:-1}"
COMPACTION_NUM="${COMPACTION_NUM:-${NUM}}"
COMPACTION_THREADS="${COMPACTION_THREADS:-1}"
COMPACTION_COMPRESSION="${COMPACTION_COMPRESSION:-none}"
COMPACTION_VALUE_SIZE="${COMPACTION_VALUE_SIZE:-100}"
COMPACTION_BENCHMARKS="${COMPACTION_BENCHMARKS:-fillrandom,overwrite,waitforcompaction,stats,readrandom,seekrandom}"
COMPACTION_ARGS="${COMPACTION_ARGS:---write_buffer_size 4194304 --target_file_size_base 4194304 --max_bytes_for_level_base 16777216}"
RUN_BLOB="${RUN_BLOB:-0}"
BLOB_NUM="${BLOB_NUM:-${NUM}}"
BLOB_VALUE_SIZE="${BLOB_VALUE_SIZE:-1024}"
BLOB_THREADS="${BLOB_THREADS:-1}"
BLOB_COMPRESSION="${BLOB_COMPRESSION:-none}"
BLOB_BENCHMARKS="${BLOB_BENCHMARKS:-fillrandom,overwrite,flush,waitforcompaction,stats,readrandom,multireadrandom,seekrandom,readseq,compact,readrandom}"
BLOB_ARGS="${BLOB_ARGS:---enable_blob_files 1 --min_blob_size 512 --write_buffer_size 16777216}"

echo "== building (Release)"
dotnet build -c Release bench/RocksDb.Managed.Benchmarks -f "${TFM}" -v quiet -nologo | tail -3
make -C bench/native-c --no-print-directory
BENCH_DLL="${DOTNET_DIR}/bench/RocksDb.Managed.Benchmarks/bin/Release/${TFM}/RocksDb.Managed.Benchmarks.dll"
RUNNER=(dotnet "${BENCH_DLL}")

mkdir -p "${OUT_DIR}" "${DB_BASE}"
STAMP="${OUT_NAME:-$(date +%Y-%m-%d)-$(hostname -s)}"
CSV="${OUT_DIR}/${STAMP}.csv"
LSM_CSV="${OUT_DIR}/${STAMP}.lsm.csv"
MD="${OUT_DIR}/${STAMP}.md"
MACHINE="$(mktemp)"
NOTES_FILE="$(mktemp)"
trap 'rm -f "${MACHINE}" "${NOTES_FILE}"' EXIT
rm -f "${CSV}" "${LSM_CSV}"

PKG_VERSION="$(sed -n 's:.*<NativeRocksDbPackageVersion>\(.*\)</NativeRocksDbPackageVersion>.*:\1:p' Directory.Build.props)"
LIBDIR="$(make -C bench/native-c --no-print-directory print-libdir)"
SONAME="$(readelf -d "${LIBDIR}/librocksdb.so" 2>/dev/null | sed -n 's/.*Library soname: \[\(.*\)\]/\1/p')"
{
  echo "Date: $(date -u '+%Y-%m-%d %H:%M UTC'), host $(hostname -s)"
  echo "CPU: $(lscpu | sed -n 's/^Model name:[[:space:]]*//p'), $(nproc) cores ($(lscpu | sed -n 's/^Hypervisor vendor:[[:space:]]*//p' | sed 's/^/VM: /'))"
  echo "Memory: $(free -g | awk '/^Mem:/{print $2 " GiB"}')"
  echo "Kernel: $(uname -sr)"
  echo ".NET: SDK $(dotnet --version); benchmarks run on ${TFM} ($("${RUNNER[@]}" machine-info | sed -n 's/^.NET: //p'))"
  echo "librocksdb: NuGet RocksDB ${PKG_VERSION} (${SONAME}), used by both c-native and rocksdb-sharp"
  echo "C compiler: $(${CC:-cc} --version | head -1)"
  echo "Load average at start: $(cut -d' ' -f1-3 /proc/loadavg)"
} > "${MACHINE}"

START=$(date +%s)
LOADS=""
LOAD_SAMPLES=()
sample_load() { LOAD_SAMPLES+=("$(cut -d' ' -f1 /proc/loadavg)"); }
for (( rep = 1; rep <= REPEATS; rep++ )); do
  REP_ARGS=""
  if (( REPEATS > 1 )); then REP_ARGS="--rep ${rep}"; echo "== repetition ${rep}/${REPEATS}"; fi
  if [[ -n "${LOAD_MAX}" ]]; then
    waited=0
    while awk -v l="$(cut -d' ' -f1 /proc/loadavg)" -v m="${LOAD_MAX}" 'BEGIN { exit !(l > m) }' && (( waited < LOAD_WAIT_MAX_S )); do
      sleep 10; waited=$(( waited + 10 ))
    done
    if (( waited > 0 )); then echo "   waited ${waited} s for load <= ${LOAD_MAX}"; fi
  fi
  LOADS="${LOADS}${LOADS:+; }rep ${rep} $(date -u +%H:%M) $(cut -d' ' -f1-3 /proc/loadavg)"
  if [[ "${RUN_MATRIX}" == 1 ]]; then
  for vs in ${VALUE_SIZES}; do
    num="${NUM}"
    reads="${READS:-${num}}"
    if (( vs > 100 )); then num="${NUM_LARGE}"; reads="${READS_LARGE:-${num}}"; fi
    for comp in ${COMPRESSIONS}; do
      for th in ${THREADS_LIST}; do
        echo "== value_size=${vs} num=${num} reads=${reads} compression=${comp} threads=${th}"
        sample_load
        # shellcheck disable=SC2086
        "${RUNNER[@]}" compare --engines "${ENGINES}" --db_base "${DB_BASE}" --csv "${CSV}" ${REP_ARGS} -- \
          --benchmarks "${BENCHMARKS}" --num "${num}" --reads "${reads}" --value_size "${vs}" \
          --threads "${th}" --compression "${comp}" ${WARMUP_ARGS} ${EXTRA_ARGS} \
          | grep -E '^(====|[a-z]+ +: )' | grep -vE '^warmup +: ([a-z]+ [0-9]+ reps|reopen|start)' || true
      done
    done
  done
  fi
  if [[ "${RUN_COMPACTION}" == 1 ]]; then
    echo "== compaction: num=${COMPACTION_NUM} value_size=${COMPACTION_VALUE_SIZE} compression=${COMPACTION_COMPRESSION} threads=${COMPACTION_THREADS} ${COMPACTION_ARGS}"
    sample_load
    # shellcheck disable=SC2086
    "${RUNNER[@]}" compare --engines "${ENGINES}" --db_base "${DB_BASE}" --csv "${CSV}" ${REP_ARGS} --label compaction -- \
      --benchmarks "${COMPACTION_BENCHMARKS}" --num "${COMPACTION_NUM}" --value_size "${COMPACTION_VALUE_SIZE}" \
      --threads "${COMPACTION_THREADS}" --compression "${COMPACTION_COMPRESSION}" ${COMPACTION_ARGS} ${WARMUP_ARGS} ${EXTRA_ARGS} \
      | grep -E '^(====|[a-z]+ +: |Cumulative compaction)' | grep -vE '^warmup +: ([a-z]+ [0-9]+ reps|reopen|start)' || true
  fi
  if [[ "${RUN_BLOB}" == 1 ]]; then
    echo "== blob: num=${BLOB_NUM} value_size=${BLOB_VALUE_SIZE} compression=${BLOB_COMPRESSION} threads=${BLOB_THREADS} ${BLOB_ARGS}"
    sample_load
    # shellcheck disable=SC2086
    "${RUNNER[@]}" compare --engines "${ENGINES}" --db_base "${DB_BASE}" --csv "${CSV}" ${REP_ARGS} --label blob -- \
      --benchmarks "${BLOB_BENCHMARKS}" --num "${BLOB_NUM}" --value_size "${BLOB_VALUE_SIZE}" \
      --threads "${BLOB_THREADS}" --compression "${BLOB_COMPRESSION}" ${BLOB_ARGS} ${WARMUP_ARGS} ${EXTRA_ARGS} \
      | grep -E '^(====|[a-z]+ +: |Cumulative compaction)' | grep -vE '^warmup +: ([a-z]+ [0-9]+ reps|reopen|start)' || true
  fi
done
END=$(date +%s)
if (( REPEATS > 1 )); then echo "Repetitions: ${REPEATS}; UTC time and load average (1/5/15 min) at the start of each: ${LOADS}" >> "${MACHINE}"; fi
LOAD_SUMMARY="$(printf '%s\n' "${LOAD_SAMPLES[@]}" | sort -g | awk '{ v[NR] = $1 } END { printf "min %s, median %s, max %s", v[1], v[int((NR + 1) / 2)], v[NR] }')"
echo "1-minute load average before each of the ${#LOAD_SAMPLES[@]} engine-set runs: ${LOAD_SUMMARY}" >> "${MACHINE}"
echo "Load average at end: $(cut -d' ' -f1-3 /proc/loadavg); total runtime $(( (END - START) / 60 )) min $(( (END - START) % 60 )) s" >> "${MACHINE}"

{
  if [[ "${RUN_MATRIX}" == 1 ]]; then
  echo "Matrix: value sizes [${VALUE_SIZES}] (num=${NUM}, reads=${READS:-num}; large values num=${NUM_LARGE}, reads=${READS_LARGE:-num}), threads [${THREADS_LIST}],"
  echo "compression [${COMPRESSIONS}], benchmarks \`${BENCHMARKS}\`. Common options: 16-byte keys, values 50% compressible,"
  echo "8 MiB LRU block cache, 10-bit full bloom filter, 64 MiB write buffer, 4 KiB blocks, format_version 6, WAL on, no sync"
  echo "(fillsync: num/1000 synced puts). Fill benchmarks start from a fresh DB; the others run on the DB left by the previous step."
  echo "Ops are split across threads; readwhilewriting = N reader threads + 1 writer. Each engine runs in its own process."
  fi
  if [[ "${RUN_COMPACTION}" == 1 ]]; then
    if [[ "${RUN_MATRIX}" == 1 ]]; then echo; fi
    echo "Compaction-heavy configuration (label \`compaction\`): num=${COMPACTION_NUM}, value ${COMPACTION_VALUE_SIZE} B, threads ${COMPACTION_THREADS},"
    echo "compression ${COMPACTION_COMPRESSION}, \`${COMPACTION_ARGS}\`; otherwise 16-byte keys, values 50% compressible, 8 MiB LRU"
    echo "block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6, WAL on, no sync, and the engines' default leveled"
    echo "compaction (dynamic level bytes, level0_file_num_compaction_trigger 4, 2 background jobs). Benchmarks \`${COMPACTION_BENCHMARKS}\`."
    echo "fillrandom and overwrite run with background flushes and compactions; waitforcompaction waits until none is pending."
  fi
  if [[ "${RUN_BLOB}" == 1 ]]; then
    if [[ "${RUN_MATRIX}" == 1 || "${RUN_COMPACTION}" == 1 ]]; then echo; fi
    echo "Integrated BlobDB configuration (label \`blob\`): num=${BLOB_NUM}, value ${BLOB_VALUE_SIZE} B, threads ${BLOB_THREADS},"
    echo "compression ${BLOB_COMPRESSION}, \`${BLOB_ARGS}\` (values of at least min_blob_size bytes are written to blob files by"
    echo "flush and compaction; reads resolve blob indexes through the blob file readers; no blob cache, no blob GC); otherwise"
    echo "16-byte keys, values 50% compressible, 8 MiB LRU block cache, 10-bit full bloom filter, 4 KiB blocks, format_version 6,"
    echo "WAL on, no sync. Benchmarks \`${BLOB_BENCHMARKS}\`."
  fi
  echo
  if [[ "${WARMUP}" == true || "${WARMUP}" == 1 ]]; then
    echo "Warm-up on for every engine (\`${WARMUP_ARGS}\`): before the measured run, each process runs every benchmark of the list"
    echo "on a separate temporary DB (same options, max(50k, num/10) <= 200k keys unless --warmup_ops; repeated benchmarks for >= 32"
    echo "repetitions, >= warmup_ops ops and >= 1 s each) plus 3 reopen + flush/compaction cycles, deletes it, and (.NET) waits for"
    echo "the JIT to quiesce; every measured benchmark starts after a settle pause (JIT quiet for 200 ms; the C leg sleeps 200 ms)."
    echo "The measured run starts on a fresh DB and block cache. The jit columns show the methods compiled during each measurement."
  else
    echo "Warm-up off: every engine is measured from a cold process (the .NET engines include JIT compilation and tier-0 code)."
  fi
  if [[ -n "${NOTES:-}" ]]; then echo; echo "${NOTES}"; fi
} > "${NOTES_FILE}"

"${RUNNER[@]}" report --csv "${CSV}" --out "${MD}" --machine "${MACHINE}" --notes "${NOTES_FILE}"
echo "== wrote ${MD} and ${CSV}"
