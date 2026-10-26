#!/usr/bin/env bash
# Lane Buildkite trusted (issue #562): controllo «readiness» sulla PR indicata dall'ambiente controllato.
# Nessun argomento (l'agent trusted gira con no-command-eval); contratto completo in scripts/ci/trusted_pr_guard.py
# e docs/ci_trusted_lane.md. Exit 0 solo con verdetto PASS, 1 in ogni altro caso.
set -euo pipefail
set +x                                   # mai tracciare: il token passa dall'ambiente del processo figlio
if [ "$#" -ne 0 ]; then
  echo "[readiness] nessun argomento ammesso: i dati arrivano solo dall'ambiente controllato" >&2
  exit 1
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$root"
exec python3 -I "$root/scripts/ci/trusted_pr_guard.py" readiness
