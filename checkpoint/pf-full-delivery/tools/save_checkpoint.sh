#!/usr/bin/env bash
# Salvataggio DURABLE del checkpoint (mandato PF-ROSERPINA-CONTINUOUS-DELIVERY-04, punto 4.4/4.5).
# Uso: save_checkpoint.sh <worktree_checkpoint> <commit_message>
# - aggiornamento ottimistico: il push parte solo se il remoto e' ancora sul commit da cui si e' partiti
#   (nessun force-push; se un altro agente ha avanzato lo stato, ci si ferma e si riconcilia);
# - manifest con SHA-256 di ogni artefatto;
# - readback remoto: il checkpoint e' DURABLE solo se STATE.json e MANIFEST rilettti dal remoto coincidono.
set -euo pipefail
WT="$1"; MSG="$2"
BR="agent-state/pf-full-delivery"
DIR="checkpoint/pf-full-delivery"
cd "$WT"
git fetch -q origin "$BR"
BASE=$(git rev-parse HEAD)
REMOTE=$(git rev-parse "origin/$BR")
if [ "$BASE" != "$REMOTE" ]; then
  echo "CONFLITTO: il remoto ($REMOTE) e' avanzato rispetto alla base locale ($BASE). Leggere e riconciliare, nessun push." >&2
  exit 3
fi
( cd "$DIR" && find . -type f ! -name MANIFEST.sha256 -print0 | sort -z | xargs -0 sha256sum > MANIFEST.sha256 )
git add -A "$DIR"
git commit -q -m "$MSG"
git push -q origin "HEAD:$BR"
git fetch -q origin "$BR"
NEW=$(git rev-parse HEAD)
[ "$(git rev-parse "origin/$BR")" = "$NEW" ] || { echo "READBACK FAIL: remoto != locale" >&2; exit 4; }
for f in STATE.json MANIFEST.sha256 HANDOFF.md; do
  diff <(git show "origin/$BR:$DIR/$f") "$DIR/$f" >/dev/null || { echo "READBACK FAIL: $f" >&2; exit 5; }
done
echo "DURABLE $NEW (precedente $BASE)"
