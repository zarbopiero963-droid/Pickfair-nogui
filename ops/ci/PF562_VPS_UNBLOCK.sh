#!/usr/bin/env bash
# PF562_VPS_UNBLOCK.sh — sblocco della lane Buildkite di Pickfair (#562), da eseguire come root sulla VPS della CI.
#
# Cosa fa (solo infrastruttura CI, niente Pickfair applicativo, niente Betfair, nessun segreto nei log):
#   1. preflight fail-closed: root, Linux x86_64, VPS giusta (agent Buildkite, coda, unit pf-dispatch), Python 3.11 della
#      lane con tkinter;
#   2. appimagetool: binario (file locale o download dal canale ufficiale) accettato SOLO se il suo sha256 coincide con
#      quello passato a mano (--appimagetool-sha256, ricavato da una fonte indipendente dal binario); mai eseguito prima
#      della verifica; installato root:root 0555 con appimagetool.sha256 0444, non scrivibile dal job;
#   3. dispatcher trusted: backup di script, configurazione e archivio SQLite, poi aggiornamento dello script installato
#      con `scripts/pr_trusted_dispatcher.py` del commit di `main` passato a mano (--main-sha, verificato == origin/main).
#      Configurazione, credenziali, timeout, concorrenza e unit systemd NON vengono toccati;
#   4. verifiche: check-config, un solo processo del dispatcher, 52 step del generatore di main = FULL_SUITE_STEP_KEYS
#      del dispatcher, i 3 step M2 obbligatori, appimagetool eseguibile ma non scrivibile dal job;
#   5. report con SHA, hash, backup e comando di rollback.
#
# Default: SOLO preflight e piano (nessuna modifica). Le modifiche richiedono --apply.
#
# Uso:
#   sudo bash ops/ci/PF562_VPS_UNBLOCK.sh --main-sha <40 hex> --appimagetool-sha256 <64 hex> \
#        [--appimagetool-file /percorso/appimagetool-x86_64.AppImage] [--apply]
#   sudo bash ops/ci/PF562_VPS_UNBLOCK.sh --rollback /root/pf562-unblock-<timestamp>
#
# Codici di uscita: 0 OK; 1 preflight o verifica fallita (niente applicato, oppure rollback automatico eseguito);
# 2 uso errato; 3 rollback automatico fallito: intervento manuale (vedi report).
set -euo pipefail
umask 077

REPO_URL="${PF562_REPO_URL:-https://github.com/zarbopiero963-droid/Pickfair-nogui.git}"
# release fissa 1.9.1 (opzione B, owner su #562): sha256 atteso ed4ce84f0d9caff66f50bcca6ff6f35aae54ce8135408b3fa33abfc3cb384eb0,
# sempre passato a mano con --appimagetool-sha256 e verificato prima di installare
APPIMAGETOOL_URL="https://github.com/AppImage/appimagetool/releases/download/1.9.1/appimagetool-x86_64.AppImage"
# percorsi della lane: le variabili PF562_* servono SOLO ai test automatici (tests/scripts/test_pf562_vps_unblock.py)
TOOLS_DIR="${PF562_TOOLS_DIR:-/var/lib/buildkite/tools}"
LANE_PYTHON="${PF562_LANE_PYTHON:-/var/lib/buildkite/tools/py311/bin/python3.11}"
AGENT_CFG="${PF562_AGENT_CFG:-/etc/buildkite-agent/buildkite-agent.cfg}"
AGENT_USER="${PF562_AGENT_USER:-buildkite-agent}"
AGENT_NAME="${PF562_AGENT_NAME:-pickfair-dedicated-vps-01}"
AGENT_QUEUE="${PF562_AGENT_QUEUE:-pickfair-dedicated}"
DISPATCH_UNIT="${PF562_DISPATCH_UNIT:-pf-dispatch.service}"
BACKUP_BASE="${PF562_BACKUP_BASE:-/root}"
PY_MINOR="${PF562_PY_MINOR:-11}"            # la lane usa Python 3.11
INSTALL_OWNER="${PF562_INSTALL_OWNER:-root}" # proprietario di appimagetool (root; altro solo nei test non-root)
INSTALL_GROUP="${PF562_INSTALL_GROUP:-root}"
EXPECTED_STEPS=52
M2_STEPS=(w5-lockfile-fresh w5-build-linux-appimage w5-telegram-live-sim)

MAIN_SHA="" AIT_SHA="" AIT_FILE="" APPLY=0 ROLLBACK_DIR=""
log()  { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die()  { log "FERMO: $*"; exit 1; }
usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --main-sha) MAIN_SHA="${2:-}"; shift 2 ;;
    --appimagetool-sha256) AIT_SHA="${2:-}"; shift 2 ;;
    --appimagetool-file) AIT_FILE="${2:-}"; shift 2 ;;
    --apply) APPLY=1; shift ;;
    --rollback) ROLLBACK_DIR="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) log "argomento sconosciuto: $1"; exit 2 ;;
  esac
done

# ------------------------------------------------------------------------------------------------- preflight comune
preflight_host() {
  [ "$(id -u)" -eq 0 ] || die "serve root"
  [ "$(uname -s)" = "Linux" ] || die "sistema non Linux: $(uname -s)"
  [ "$(uname -m)" = "x86_64" ] || die "architettura non x86_64: $(uname -m)"
  command -v systemctl >/dev/null || die "systemd assente"
  command -v sha256sum >/dev/null || die "sha256sum assente"
  command -v git >/dev/null || die "git assente"
  id "$AGENT_USER" >/dev/null 2>&1 || die "utente $AGENT_USER assente: non e' la VPS della lane"
  [ -r "$AGENT_CFG" ] || die "configurazione dell'agent assente: $AGENT_CFG"
  # identita' della VPS: nome e coda dell'agent (mai stampato il token dell'agent)
  grep -Eq "^[[:space:]]*name[[:space:]]*=[[:space:]]*\"?${AGENT_NAME}" "$AGENT_CFG" \
    || die "agent diverso da ${AGENT_NAME}: VPS sbagliata"
  grep -Eq "queue=${AGENT_QUEUE}([,\"]|$)" "$AGENT_CFG" || die "coda ${AGENT_QUEUE} non configurata: VPS sbagliata"
  systemctl cat "$DISPATCH_UNIT" >/dev/null 2>&1 || die "unit ${DISPATCH_UNIT} assente: VPS sbagliata"
  [ -x "$LANE_PYTHON" ] || die "Python della lane assente: $LANE_PYTHON"
  "$LANE_PYTHON" -c "import sys; assert sys.version_info[:2] == (3, ${PY_MINOR})" || die "il Python della lane non e' 3.${PY_MINOR}"
  "$LANE_PYTHON" -c 'import tkinter; print("Tk", tkinter.TkVersion)' >/dev/null 2>&1 || die "tkinter assente nel Python della lane"
  local bad
  if bad="$(agent_can_replace "$TOOLS_DIR/appimagetool")"; then
    die "$bad e' scrivibile da $AGENT_USER: il job potrebbe sostituire appimagetool (correggere i permessi, poi rilanciare)"
  fi
  log "preflight host OK: $(uname -s) $(uname -m), agent ${AGENT_NAME} coda ${AGENT_QUEUE}, unit ${DISPATCH_UNIT}, Python 3.${PY_MINOR} + tkinter"
}

# percorso dello script e della configurazione del dispatcher, letti da ExecStart: mai indovinati
dispatcher_paths() {
  local exec
  exec="$(systemctl show -p ExecStart --value "$DISPATCH_UNIT" 2>/dev/null)" || die "ExecStart di ${DISPATCH_UNIT} illeggibile"
  DISP_SCRIPT="$(printf '%s\n' "$exec" | grep -oE '/[^ ;]*pr_trusted_dispatcher\.py' | head -1 || true)"
  DISP_CONFIG="$(printf '%s\n' "$exec" | grep -oE -- '--config[ =]/[^ ;]+' | head -1 | sed -E 's/^--config[ =]//' || true)"
  [ -n "${DISP_SCRIPT:-}" ] && [ -f "$DISP_SCRIPT" ] || die "script del dispatcher non determinabile da ExecStart"
  [ -n "${DISP_CONFIG:-}" ] && [ -f "$DISP_CONFIG" ] || die "configurazione del dispatcher non determinabile da ExecStart"
  DISP_DB="$("$LANE_PYTHON" -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["state_db"])' "$DISP_CONFIG")" \
    || die "state_db illeggibile dalla configurazione"
  [ -f "$DISP_DB" ] || die "archivio del dispatcher assente: $DISP_DB"
}

dispatcher_count() { pgrep -fc 'pr_trusted_dispatcher\.py.* serve' || true; }

# il job (utente dell'agent) puo' sostituire `path`? Vale per il file e per OGNI directory sopra (Sol su #587): una
# directory scrivibile permette di rinominare il figlio e rimpiazzare binario e checksum. Una directory sticky di un
# altro utente (/tmp) non lo permette. Stampa il primo percorso pericoloso; nessuno => exit 1.
agent_can_replace() {
  local p="$1" mode owner
  while :; do
    if [ -e "$p" ] && runuser -u "$AGENT_USER" -- test -w "$p"; then
      mode="$(stat -c %a "$p")"; owner="$(stat -c %U "$p")"
      if ! { [ -d "$p" ] && [ $(( 8#$mode & 01000 )) -ne 0 ] && [ "$owner" != "$AGENT_USER" ]; }; then
        printf '%s\n' "$p"; return 0
      fi
    fi
    [ "$p" = "/" ] && return 1
    p="$(dirname "$p")"
  done
}

# ---------------------------------------------------------------------------------------------------------- rollback
do_rollback() {
  local dir="$1" rc=0
  [ -d "$dir" ] && [ -f "$dir/manifest.sha256" ] || { log "backup non valido: $dir"; return 3; }
  ( cd "$dir" && sha256sum -c --quiet manifest.sha256 ) || { log "backup alterato (manifest)"; return 3; }
  # shellcheck disable=SC1091
  . "$dir/paths.env"
  systemctl stop "$DISPATCH_UNIT" || rc=3
  install -o "$DISP_OWNER" -g "$DISP_GROUP" -m "$DISP_MODE" "$dir/dispatcher.py" "$DISP_SCRIPT" || rc=3
  if [ -f "$dir/appimagetool.tgz" ]; then
    rm -rf "$TOOLS_DIR/appimagetool" && tar -C "$TOOLS_DIR" -xzf "$dir/appimagetool.tgz" || rc=3
  elif [ -f "$dir/appimagetool.absent" ]; then
    rm -rf "$TOOLS_DIR/appimagetool" || rc=3
  fi
  systemctl start "$DISPATCH_UNIT" || rc=3
  sleep 5
  [ "$(dispatcher_count)" = "1" ] || { log "dopo il rollback il dispatcher non e' attivo una sola volta"; rc=3; }
  [ "$(sha256sum "$DISP_SCRIPT" | awk '{print $1}')" = "$(sha256sum "$dir/dispatcher.py" | awk '{print $1}')" ] || rc=3
  log "rollback da $dir: $([ $rc -eq 0 ] && echo OK || echo 'NON COMPLETO')"
  # la configurazione e l'archivio non vengono mai modificati da questo script: restano nel backup, non ripristinati
  return $rc
}

if [ -n "$ROLLBACK_DIR" ]; then
  preflight_host
  do_rollback "$ROLLBACK_DIR" && exit 0 || exit 3
fi

# ------------------------------------------------------------------------------------------------- argomenti e piano
[[ "$MAIN_SHA" =~ ^[0-9a-f]{40}$ ]] || { log "--main-sha: servono 40 caratteri esadecimali"; exit 2; }
[[ "$AIT_SHA" =~ ^[0-9a-f]{64}$ ]] || { log "--appimagetool-sha256: servono 64 caratteri esadecimali"; exit 2; }
preflight_host
dispatcher_paths

WORK="$(mktemp -d "$BACKUP_BASE/pf562-work.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

# main trusted: lo SHA passato deve essere l'head attuale di main (mai codice di una PR)
REMOTE_MAIN="$(git ls-remote "$REPO_URL" refs/heads/main | awk '{print $1}')"
[ "$REMOTE_MAIN" = "$MAIN_SHA" ] || die "--main-sha ${MAIN_SHA} diverso da origin/main ${REMOTE_MAIN:-?}"
git -C "$WORK" init -q repo
git -C "$WORK/repo" fetch -q --depth 1 "$REPO_URL" "$MAIN_SHA" || die "fetch di main@${MAIN_SHA} fallito"
[ "$(git -C "$WORK/repo" rev-parse FETCH_HEAD)" = "$MAIN_SHA" ] || die "commit scaricato diverso da ${MAIN_SHA}"
git -C "$WORK/repo" show "$MAIN_SHA:scripts/pr_trusted_dispatcher.py" > "$WORK/dispatcher.py"
git -C "$WORK/repo" show "$MAIN_SHA:scripts/pr_full_suite_pipeline.py" > "$WORK/pipeline.py"

# 52 step del generatore di main == step obbligatori del dispatcher di main, M2 compresi
"$LANE_PYTHON" -I - "$WORK/dispatcher.py" "$WORK/pipeline.py" "$EXPECTED_STEPS" "${M2_STEPS[@]}" <<'PY' || die "step del generatore e del dispatcher non coerenti"
import importlib.util, json, subprocess, sys
disp, pipe, expected, m2 = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4:]
spec = importlib.util.spec_from_file_location("pf_dispatcher_main", disp)
mod = importlib.util.module_from_spec(spec); sys.modules[spec.name] = mod; spec.loader.exec_module(mod)
keys = set(mod.FULL_SUITE_STEP_KEYS)
steps = json.loads(subprocess.run([sys.executable, "-I", pipe], capture_output=True, text=True, check=True).stdout)
gen = {s["key"] for s in steps["steps"]}
assert len(gen) == expected == len(keys), (len(gen), len(keys), expected)
assert gen == keys, sorted(gen ^ keys)
assert all(k in keys for k in m2), m2
print(f"step: {len(gen)} generati = {len(keys)} obbligatori, M2 obbligatori: {', '.join(m2)}")
PY

# appimagetool: candidato verificato PRIMA di qualunque esecuzione o installazione
AIT_DIR="$TOOLS_DIR/appimagetool"
AIT_CAND="$WORK/appimagetool"
if [ -n "$AIT_FILE" ]; then
  [ -f "$AIT_FILE" ] || die "--appimagetool-file inesistente"
  cp "$AIT_FILE" "$AIT_CAND"
else
  command -v curl >/dev/null || die "curl assente"
  curl -fsSL --proto '=https' --tlsv1.2 -o "$AIT_CAND" "$APPIMAGETOOL_URL" || die "download di appimagetool fallito"
fi
GOT_SHA="$(sha256sum "$AIT_CAND" | awk '{print $1}')"
[ "$GOT_SHA" = "$AIT_SHA" ] || die "sha256 di appimagetool ${GOT_SHA} diverso dall'atteso ${AIT_SHA}: non installato"
[ "$(head -c 4 "$AIT_CAND" | od -An -tx1 | tr -d ' \n')" = "7f454c46" ] || die "appimagetool non e' un ELF"
ALREADY_AIT=0
if [ -f "$AIT_DIR/appimagetool" ] && [ "$(sha256sum "$AIT_DIR/appimagetool" | awk '{print $1}')" = "$AIT_SHA" ] \
   && [ "$(awk '{print $1; exit}' "$AIT_DIR/appimagetool.sha256" 2>/dev/null)" = "$AIT_SHA" ] \
   && [ "$(stat -c '%U:%G %a' "$AIT_DIR")" = "$INSTALL_OWNER:$INSTALL_GROUP 755" ] \
   && [ "$(stat -c '%U:%G %a' "$AIT_DIR/appimagetool")" = "$INSTALL_OWNER:$INSTALL_GROUP 555" ] \
   && [ "$(stat -c '%U:%G %a' "$AIT_DIR/appimagetool.sha256")" = "$INSTALL_OWNER:$INSTALL_GROUP 444" ]; then
  # riuso solo con proprietario e permessi attesi; altrimenti si reinstalla e si normalizza (Codex su #587)
  ALREADY_AIT=1
fi

CUR_DISP_SHA="$(sha256sum "$DISP_SCRIPT" | awk '{print $1}')"
NEW_DISP_SHA="$(sha256sum "$WORK/dispatcher.py" | awk '{print $1}')"
ALREADY_DISP=$([ "$CUR_DISP_SHA" = "$NEW_DISP_SHA" ] && echo 1 || echo 0)

log "PIANO: main ${MAIN_SHA}; dispatcher ${DISP_SCRIPT} ${CUR_DISP_SHA} -> ${NEW_DISP_SHA} $([ $ALREADY_DISP = 1 ] && echo '(gia aggiornato)')"
log "PIANO: appimagetool ${AIT_SHA} in ${AIT_DIR} $([ $ALREADY_AIT = 1 ] && echo '(gia installato)')"
log "PIANO: configurazione ${DISP_CONFIG} e archivio ${DISP_DB}: solo backup, nessuna modifica"
if [ "$APPLY" -ne 1 ]; then
  log "DRY-RUN: nessuna modifica applicata. Rilanciare con --apply."
  exit 0
fi

# -------------------------------------------------------------------------------------------------------- backup
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BK="$BACKUP_BASE/pf562-unblock-$TS"
install -d -m 0700 "$BK"
cp -p "$DISP_SCRIPT" "$BK/dispatcher.py"
cp -p "$DISP_CONFIG" "$BK/config.json"
"$LANE_PYTHON" -I -c 'import sqlite3,sys; s=sqlite3.connect(sys.argv[1]); d=sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()' \
  "$DISP_DB" "$BK/dispatcher.db" || die "backup dell'archivio fallito"
if [ -d "$AIT_DIR" ]; then tar -C "$TOOLS_DIR" -czf "$BK/appimagetool.tgz" appimagetool; else : > "$BK/appimagetool.absent"; fi
{
  printf 'DISP_SCRIPT=%q\nDISP_CONFIG=%q\nDISP_DB=%q\n' "$DISP_SCRIPT" "$DISP_CONFIG" "$DISP_DB"
  printf 'DISP_OWNER=%q\nDISP_GROUP=%q\nDISP_MODE=%q\n' "$(stat -c %U "$DISP_SCRIPT")" "$(stat -c %G "$DISP_SCRIPT")" "$(stat -c %a "$DISP_SCRIPT")"
} > "$BK/paths.env"
( cd "$BK" && sha256sum ./* > manifest.sha256 )
log "backup: $BK"

fail_and_rollback() {
  trap - ERR
  log "FERMO: $*. Rollback automatico da $BK"
  if do_rollback "$BK"; then exit 1; else log "ROLLBACK NON COMPLETO: intervento manuale"; exit 3; fi
}

# da qui ogni comando che fallisce (anche una scrittura) riporta allo stato del backup (Sol su #587)
set -E
trap 'fail_and_rollback "errore non previsto alla riga $LINENO"' ERR

# ------------------------------------------------------------------------------------------------- appimagetool
if [ "$ALREADY_AIT" -ne 1 ]; then
  install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 "$AIT_DIR"
  chown "$INSTALL_OWNER:$INSTALL_GROUP" "$AIT_DIR"; chmod 0755 "$AIT_DIR"   # install -d non corregge una dir esistente
  install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0555 "$AIT_CAND" "$AIT_DIR/appimagetool.new"
  mv -f "$AIT_DIR/appimagetool.new" "$AIT_DIR/appimagetool"
  printf '%s  appimagetool\n' "$AIT_SHA" > "$AIT_DIR/appimagetool.sha256.new"
  chown "$INSTALL_OWNER:$INSTALL_GROUP" "$AIT_DIR/appimagetool.sha256.new"; chmod 0444 "$AIT_DIR/appimagetool.sha256.new"
  mv -f "$AIT_DIR/appimagetool.sha256.new" "$AIT_DIR/appimagetool.sha256"
fi
( cd "$AIT_DIR" && sha256sum -c --quiet appimagetool.sha256 ) || fail_and_rollback "verifica sha256 di appimagetool installato"
for f in "$AIT_DIR/appimagetool" "$AIT_DIR/appimagetool.sha256"; do
  if bad="$(agent_can_replace "$f")"; then fail_and_rollback "$bad scrivibile da $AGENT_USER"; fi
done
runuser -u "$AGENT_USER" -- test -x "$AIT_DIR/appimagetool" || fail_and_rollback "appimagetool non eseguibile da $AGENT_USER"

# ------------------------------------------------------------------------------------------------- dispatcher
if [ "$ALREADY_DISP" -ne 1 ]; then
  # check-config col NUOVO script sulla configurazione esistente, prima di sostituire niente
  "$LANE_PYTHON" -I "$WORK/dispatcher.py" check-config --config "$DISP_CONFIG" >/dev/null \
    || fail_and_rollback "il dispatcher di main rifiuta la configurazione installata"
  install -o "$(stat -c %U "$DISP_SCRIPT")" -g "$(stat -c %G "$DISP_SCRIPT")" -m "$(stat -c %a "$DISP_SCRIPT")" \
    "$WORK/dispatcher.py" "$DISP_SCRIPT.new"
  mv -f "$DISP_SCRIPT.new" "$DISP_SCRIPT"
  systemctl restart "$DISPATCH_UNIT" || fail_and_rollback "riavvio di ${DISPATCH_UNIT} fallito"
  sleep 10
fi
systemctl is-active --quiet "$DISPATCH_UNIT" || fail_and_rollback "${DISPATCH_UNIT} non attivo"
[ "$(dispatcher_count)" = "1" ] || fail_and_rollback "processi del dispatcher: $(dispatcher_count) (atteso 1)"
[ "$(sha256sum "$DISP_SCRIPT" | awk '{print $1}')" = "$NEW_DISP_SHA" ] || fail_and_rollback "script installato diverso da main"
# smoke non finanziario: configurazione e journal recente senza errori di avvio (solo conteggi, mai il contenuto)
"$LANE_PYTHON" -I "$DISP_SCRIPT" check-config --config "$DISP_CONFIG" >/dev/null || fail_and_rollback "check-config dopo l'aggiornamento"
ERRS="$(journalctl -u "$DISPATCH_UNIT" --since '-2 min' --no-pager -o cat 2>/dev/null | grep -ciE 'traceback|NOT CONFIGURED' || true)"
[ "${ERRS:-0}" = "0" ] || fail_and_rollback "errori di avvio nel journal del dispatcher (${ERRS})"

# ---------------------------------------------------------------------------------------------------------- report
REPORT="$BK/report.txt"
{
  echo "PF562_VPS_UNBLOCK report $TS"
  echo "main: $MAIN_SHA"
  echo "dispatcher: $DISP_SCRIPT  prima $CUR_DISP_SHA  dopo $NEW_DISP_SHA  processi: $(dispatcher_count)"
  echo "configurazione: $DISP_CONFIG (invariata)  archivio: $DISP_DB (invariato, copia nel backup)"
  echo "appimagetool: $AIT_DIR/appimagetool  sha256 $AIT_SHA  $(stat -c '%U:%G %a' "$AIT_DIR/appimagetool")"
  echo "step: $EXPECTED_STEPS (M2 obbligatori: ${M2_STEPS[*]})"
  echo "backup: $BK  (manifest.sha256)"
  echo "rollback: sudo bash $0 --rollback $BK"
} > "$REPORT"
chmod 0600 "$REPORT"
cat "$REPORT"
log "OK. Prossimo passo: push della PR #587 (nuova full-suite a 52 step); nessun rerun manuale necessario."
