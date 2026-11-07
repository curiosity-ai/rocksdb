#!/usr/bin/env bash
# Long-running Jepsen-style soak test of RocksDb.Managed (see docs/soak.md).
#
#   tools/soak.sh --dir /data/soak --duration 72h          # run (or resume) a soak in /data/soak
#   tools/soak.sh --dir /data/soak --smoke                 # 2-minute CI smoke run (short cycles)
#   tools/soak.sh report --dir /data/soak                  # summary of a run directory
#   tools/soak.sh selftest                                 # prove that every check catches an injected bug
#
# Builds tools/RocksDb.Managed.Soak in Release once, then runs the built executable (FRAMEWORK=net11.0 to use .NET 11).
# Any first argument that does not start with "--" is a sub-command (run, report, selftest, export-history, ...).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project="$here/RocksDb.Managed.Soak"
framework="${FRAMEWORK:-net10.0}"

if [[ $# -gt 0 && "$1" != --* ]]; then
  cmd="$1"
  shift
else
  cmd="run"
fi

dotnet build "$project" -c Release -f "$framework" -v quiet -nologo >/dev/null
exec "$project/bin/Release/$framework/RocksDb.Managed.Soak" "$cmd" "$@"
