#!/usr/bin/env bash
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).
#
# Three-way RocksDB benchmark matrix: C native (bench/native-c, RocksDB C API) vs rocksdb-sharp (NuGet RocksDB,
# same librocksdb.so) vs RocksDb.Managed (sync and async APIs). Builds everything in Release, runs every
# configuration of the matrix on every engine (each engine in its own process, on a fresh DB directory), and writes
#   bench/results/<date>-<host>.md   (comparison tables + machine info)
#   bench/results/<date>-<host>.csv  (raw RESULT rows)
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
#   NOTES="free text added to the report"
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

echo "== building (Release)"
dotnet build -c Release bench/RocksDb.Managed.Benchmarks -f "${TFM}" -v quiet -nologo | tail -3
make -C bench/native-c --no-print-directory
BENCH_DLL="${DOTNET_DIR}/bench/RocksDb.Managed.Benchmarks/bin/Release/${TFM}/RocksDb.Managed.Benchmarks.dll"
RUNNER=(dotnet "${BENCH_DLL}")

mkdir -p "${OUT_DIR}" "${DB_BASE}"
STAMP="$(date +%Y-%m-%d)-$(hostname -s)"
CSV="${OUT_DIR}/${STAMP}.csv"
MD="${OUT_DIR}/${STAMP}.md"
MACHINE="$(mktemp)"
NOTES_FILE="$(mktemp)"
trap 'rm -f "${MACHINE}" "${NOTES_FILE}"' EXIT
rm -f "${CSV}"

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
for vs in ${VALUE_SIZES}; do
  num="${NUM}"
  reads="${READS:-${num}}"
  if (( vs > 100 )); then num="${NUM_LARGE}"; reads="${READS_LARGE:-${num}}"; fi
  for comp in ${COMPRESSIONS}; do
    for th in ${THREADS_LIST}; do
      echo "== value_size=${vs} num=${num} reads=${reads} compression=${comp} threads=${th}"
      # shellcheck disable=SC2086
      "${RUNNER[@]}" compare --engines "${ENGINES}" --db_base "${DB_BASE}" --csv "${CSV}" -- \
        --benchmarks "${BENCHMARKS}" --num "${num}" --reads "${reads}" --value_size "${vs}" \
        --threads "${th}" --compression "${comp}" ${EXTRA_ARGS} \
        | grep -E '^(====|[a-z]+ +: )' || true
    done
  done
done
END=$(date +%s)
echo "Load average at end: $(cut -d' ' -f1-3 /proc/loadavg); total runtime $(( (END - START) / 60 )) min $(( (END - START) % 60 )) s" >> "${MACHINE}"

{
  echo "Matrix: value sizes [${VALUE_SIZES}] (num=${NUM}, reads=${READS:-num}; large values num=${NUM_LARGE}, reads=${READS_LARGE:-num}), threads [${THREADS_LIST}],"
  echo "compression [${COMPRESSIONS}], benchmarks \`${BENCHMARKS}\`. Common options: 16-byte keys, values 50% compressible,"
  echo "8 MiB LRU block cache, 10-bit full bloom filter, 64 MiB write buffer, 4 KiB blocks, format_version 6, WAL on, no sync"
  echo "(fillsync: num/1000 synced puts). Fill benchmarks start from a fresh DB; the others run on the DB left by the previous step."
  echo "Ops are split across threads; readwhilewriting = N reader threads + 1 writer. Each engine runs in its own process."
  if [[ -n "${NOTES:-}" ]]; then echo; echo "${NOTES}"; fi
} > "${NOTES_FILE}"

"${RUNNER[@]}" report --csv "${CSV}" --out "${MD}" --machine "${MACHINE}" --notes "${NOTES_FILE}"
echo "== wrote ${MD} and ${CSV}"
