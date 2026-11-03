#!/usr/bin/env bash
# VPS Autonomy Bridge — installazione IDEMPOTENTE (WP5, preparata offline, NON eseguita).
# Default = DRY-RUN: stampa cosa farebbe e non tocca nulla. Per applicare servono TUTTE:
#   --apply  +  file GO firmato dall'owner (PF_GO_FILE) che contiene "GO-INSTALL <sha256-archivio>"  +  preflight OK.
# Non tocca MAI: kill switch, pf-orch/certd installati (T186), pf-live, credenziali Betfair, chiavi esistenti.
# Nessuna porta in ingresso: il controllore interroga GitHub in uscita (timer), non ascolta.
set -euo pipefail
ARCHIVE="${1:?uso: install.sh ARCHIVIO.tgz SHA256 [--apply]}"
SHA="${2:?sha256 atteso del pacchetto}"
MODE="${3:-}"
PF_BRIDGE_HOME="${PF_BRIDGE_HOME:-/opt/pf-bridge}"
PF_BRIDGE_ETC="${PF_BRIDGE_ETC:-/etc/pf-bridge}"
PF_BRIDGE_STATE="${PF_BRIDGE_STATE:-/var/lib/pf-bridge}"
PF_GO_FILE="${PF_GO_FILE:-$PF_BRIDGE_ETC/GO-INSTALL}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROLES=(pf-bridge-ctl pf-bridge-verify pf-bridge-deploy pf-bridge-rollback)   # il runner resta il pool pf-sbxj0..7 di T186

run() { if [ "$MODE" = "--apply" ]; then "$@"; else printf '[dry-run]'; printf ' %q' "$@"; printf '\n'; fi; }

[[ "$SHA" =~ ^[0-9a-f]{64}$ ]] || { echo "[install] sha256 malformato" >&2; exit 1; }
python3 -I "$HERE/verify_artifact.py" "$ARCHIVE" "$SHA"                      # integrita' e contenuto: fail-closed
if [ "$MODE" = "--apply" ]; then
  [ "$(id -u)" -eq 0 ] || { echo "[install] serve root" >&2; exit 1; }
  grep -qx "GO-INSTALL $SHA" "$PF_GO_FILE" 2>/dev/null || { echo "[install] GO-INSTALL per questo sha assente: STOP" >&2; exit 1; }
  bash "$HERE/preflight.sh" || { echo "[install] preflight fallito: STOP" >&2; exit 1; }
fi
REL="$PF_BRIDGE_HOME/releases/$SHA"
for u in "${ROLES[@]}"; do
  getent passwd "$u" >/dev/null || run useradd --system --no-create-home --shell /usr/sbin/nologin "$u"
done
run install -d -m 0755 -o root -g root "$PF_BRIDGE_HOME/releases"
run install -d -m 0750 -o root -g pf-bridge-ctl "$PF_BRIDGE_ETC"
run install -d -m 0700 -o pf-bridge-ctl -g pf-bridge-ctl "$PF_BRIDGE_STATE" "$PF_BRIDGE_STATE/audit"
if [ ! -d "$REL" ]; then
  run install -d -m 0755 -o root -g root "$REL"
  run tar -xzf "$ARCHIVE" -C "$REL" --no-same-owner --no-same-permissions
  run chmod -R go-w "$REL"
fi
prev="$(readlink -f "$PF_BRIDGE_HOME/current" 2>/dev/null || true)"
[ -n "$prev" ] && [ "$prev" != "$REL" ] && run ln -sfn "$prev" "$PF_BRIDGE_HOME/previous"
run ln -sfn "$REL" "$PF_BRIDGE_HOME/current.new"
run mv -Tf "$PF_BRIDGE_HOME/current.new" "$PF_BRIDGE_HOME/current"          # switch atomico
for unit in pf-bridge-ctl.service pf-bridge-ctl.timer pf-bridge-verify@.service pf-bridge-deploy@.service pf-bridge-rollback@.service; do
  run install -m 0644 -o root -g root "$REL/systemd/$unit" "/etc/systemd/system/$unit"
done
run systemctl daemon-reload
# Attivazione NON inclusa: il timer del controllore si abilita solo con un GO separato (GO-ACTIVATE).
echo "[install] completato: modo=${MODE:-dry-run} release=$SHA. Timer NON abilitato, serve GO-ACTIVATE."
