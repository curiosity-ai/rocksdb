#!/usr/bin/env bash
# Long-running Jepsen-style soak test of the Raft cluster mode (see docs/cluster-soak.md).
#
#   tools/cluster-soak.sh --dir /data/csoak --nodes 3 --duration 72h   # run (or resume) a cluster soak
#   tools/cluster-soak.sh --dir /tmp/csoak-smoke --smoke               # ~2.5-minute smoke run (short epochs)
#   tools/cluster-soak.sh report --dir /data/csoak                     # summary of a run directory
#   tools/cluster-soak.sh selftest                                     # prove that every check catches an injected bug
#   tools/cluster-soak.sh export-history --dir /data/csoak --format edn --out /data/csoak/edn
#
# Builds tools/RocksDb.Managed.ClusterSoak in Release once, then runs the built executable (FRAMEWORK=net11.0 to use
# .NET 11). Any first argument that does not start with "--" is a sub-command (run, report, selftest, export-history).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project="$here/RocksDb.Managed.ClusterSoak"
framework="${FRAMEWORK:-net10.0}"

if [[ $# -gt 0 && "$1" != --* ]]; then
  cmd="$1"
  shift
else
  cmd="run"
fi

dotnet build "$project" -c Release -f "$framework" -v quiet -nologo >/dev/null
exec "$project/bin/Release/$framework/RocksDb.Managed.ClusterSoak" "$cmd" "$@"
