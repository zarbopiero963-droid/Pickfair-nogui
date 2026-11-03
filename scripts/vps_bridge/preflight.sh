#!/usr/bin/env bash
# VPS Autonomy Bridge — preflight in SOLA LETTURA (WP5, preparato offline, NON eseguito).
# Non modifica nulla: niente kill, stop, chown, scritture. Exit 0 solo se tutte le precondizioni valgono.
# Uso (Grok o owner da console/ConnectBot, solo dopo GO-INSTALL): sudo bash preflight.sh
set -euo pipefail
PF_ORCH_HOME="${PF_ORCH_HOME:-/opt/pf-orch}"
PF_ORCH_ETC="${PF_ORCH_ETC:-/etc/pf-orch}"
PF_BRIDGE_HOME="${PF_BRIDGE_HOME:-/opt/pf-bridge}"
EXPECTED_ORCH_SHA="${EXPECTED_ORCH_SHA:-c45ba1d8f78092d26b88584915a4f161f42681f84d6bd54bca41ec40f8c354ab}"   # 1.4.3 (T186)
EXPECTED_CERTD_SHA="${EXPECTED_CERTD_SHA:-80886a45d7d222cb10c21a747bc424a84667e424ecb3890deba4cc3558f05512}"
fail=0
ok()   { printf '[preflight] OK   %s\n' "$1"; }
bad()  { printf '[preflight] FAIL %s\n' "$1"; fail=1; }

[ "$(id -u)" -eq 0 ] && ok "eseguito come root (necessario solo per leggere i percorsi protetti)" || bad "serve root per leggere i percorsi protetti"
v="$(systemctl --version 2>/dev/null | awk 'NR==1{print $2}')"; [ -n "$v" ] && [ "$v" -ge 250 ] && ok "systemd $v" || bad "systemd >= 250 richiesto (trovato: ${v:-nessuno})"
[ -d /sys/fs/cgroup/system.slice ] && ok "cgroup v2" || bad "cgroup v2 assente"
for ks in KILL_SWITCH CERTD_KILL_SWITCH CERTD_E2E_KILL_SWITCH; do
  [ -e "$PF_ORCH_ETC/$ks" ] && ok "kill switch presente: $ks" || bad "kill switch mancante: $ks (il ponte non si installa senza)"
done
s="$(sha256sum "$PF_ORCH_HOME/orch.py" 2>/dev/null | cut -c1-64 || true)"
[ "$s" = "$EXPECTED_ORCH_SHA" ] && ok "orch.py = 1.4.3 T186" || bad "orch.py non e' la 1.4.3 T186 attesa (sha=${s:-assente}): STOP, nessuna sostituzione"
s="$(sha256sum "$PF_ORCH_HOME/certd.py" 2>/dev/null | cut -c1-64 || true)"
[ "$s" = "$EXPECTED_CERTD_SHA" ] && ok "certd.py atteso" || bad "certd.py inatteso (sha=${s:-assente})"
active="$(systemctl list-units --no-legend --state=active 'pf-sbx-*' 2>/dev/null | wc -l)"
[ "$active" -eq 0 ] && ok "nessun job sandbox attivo" || bad "$active job sandbox attivi: attendere una finestra libera"
systemctl is-active --quiet pf-live.service 2>/dev/null && bad "pf-live attivo: LIVE vietato" || ok "pf-live non attivo"
free_kb="$(df -Pk /opt | awk 'NR==2{print $4}')"; [ "${free_kb:-0}" -ge 1048576 ] && ok "spazio libero su /opt >= 1 GiB" || bad "spazio libero su /opt insufficiente"
ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE ':(8443|9443)$' && bad "porta di ingresso inattesa in ascolto" || ok "nessuna nuova porta in ingresso"
[ -e "$PF_BRIDGE_HOME/current" ] && ok "installazione precedente rilevata (aggiornamento idempotente)" || ok "prima installazione"
for u in pf-bridge-ctl pf-bridge-verify pf-bridge-deploy pf-bridge-rollback; do
  getent passwd "$u" >/dev/null && ok "utente gia' presente: $u" || ok "utente da creare: $u"
done
exit "$fail"
