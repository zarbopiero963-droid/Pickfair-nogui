#!/usr/bin/env bash
# Lane Buildkite trusted (issue #562): controllo «overlap» sulla PR indicata dall'ambiente controllato.
# Nessun argomento (l'agent trusted gira con no-command-eval); contratto completo in scripts/ci/trusted_pr_guard.py
# e docs/ci_trusted_lane.md. Exit 0 solo con verdetto PASS, 1 in ogni altro caso (anche upload del
# risultato fallito su Buildkite).
set -euo pipefail
set +x                                   # mai tracciare: il token passa dall'ambiente del processo figlio
if [ "$#" -ne 0 ]; then
  echo "[overlap] nessun argomento ammesso: i dati arrivano solo dall'ambiente controllato" >&2
  exit 1
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$root"
rc=0
python3 -I "$root/scripts/ci/trusted_pr_guard.py" overlap || rc=$?
# Su Buildkite il risultato (anche FAIL) diventa l'artifact che il dispatcher scarica: senza, lo status sarebbe
# `error` «risultato del guard assente» (D2, #562). Nel workflow Actions `BUILDKITE` non c'e' e non si carica nulla.
if [ -n "${BUILDKITE:-}" ]; then
  result="${PF_RESULT_DIR:-.trusted-guard}/overlap.json"
  if [ -f "$result" ] && ! buildkite-agent artifact upload "$result"; then
    echo "[overlap] upload del risultato fallito: $result" >&2
    rc=1
  fi
fi
exit "$rc"
