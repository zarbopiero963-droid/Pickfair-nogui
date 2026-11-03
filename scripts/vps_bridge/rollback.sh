#!/usr/bin/env bash
# VPS Autonomy Bridge — rollback (WP5, preparato offline, NON eseguito). Default DRY-RUN.
# Riporta "current" alla release "previous" (o la disinstalla se non c'e' una precedente), verifica gli sha, ferma
# solo le unita' del ponte. Non tocca MAI pf-orch/certd T186, kill switch, pf-live, credenziali.
set -euo pipefail
MODE="${1:-}"
PF_BRIDGE_HOME="${PF_BRIDGE_HOME:-/opt/pf-bridge}"
run() { if [ "$MODE" = "--apply" ]; then "$@"; else printf '[dry-run]'; printf ' %q' "$@"; printf '\n'; fi; }
if [ "$MODE" = "--apply" ]; then [ "$(id -u)" -eq 0 ] || { echo "[rollback] serve root" >&2; exit 1; }; fi
run systemctl disable --now pf-bridge-ctl.timer
run systemctl stop 'pf-bridge-verify@*.service' 'pf-bridge-deploy@*.service' 'pf-bridge-rollback@*.service'
prev="$(readlink -f "$PF_BRIDGE_HOME/previous" 2>/dev/null || true)"
if [ -n "$prev" ] && [ -d "$prev" ]; then
  sha="$(basename "$prev")"
  ( cd "$prev" && sha256sum -c --quiet SHA256SUMS ) || { echo "[rollback] release precedente alterata: STOP" >&2; exit 1; }
  run ln -sfn "$prev" "$PF_BRIDGE_HOME/current.new"
  run mv -Tf "$PF_BRIDGE_HOME/current.new" "$PF_BRIDGE_HOME/current"
  echo "[rollback] current -> $sha (timer lasciato DISABILITATO: riattivazione solo con GO)"
else
  run rm -f "$PF_BRIDGE_HOME/current"
  for unit in pf-bridge-ctl.service pf-bridge-ctl.timer pf-bridge-verify@.service pf-bridge-deploy@.service pf-bridge-rollback@.service; do
    run rm -f "/etc/systemd/system/$unit"
  done
  run systemctl daemon-reload
  echo "[rollback] nessuna release precedente: ponte disinstallato (utenti e audit conservati per verifica)"
fi
