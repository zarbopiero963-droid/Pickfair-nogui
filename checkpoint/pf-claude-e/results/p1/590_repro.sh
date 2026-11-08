#!/bin/bash
# $1 = old|new ; esegue il guard 200 volte con TG_BOT_TOKEN + 400 variabili grandi; conta i mancati
big=$(head -c 200 /dev/zero | tr '\0' x)
for i in $(seq 1 400); do export V_$i=$big; done
export TG_BOT_TOKEN=x
miss=0
for i in $(seq 1 200); do
  if [ "$1" = old ]; then
    bash -c 'set -euo pipefail; if env | grep -q "^TG_"; then exit 1; fi; exit 0'
  else
    bash -c 'set -euo pipefail; if compgen -e TG_ >/dev/null; then exit 1; fi; exit 0'
  fi
  [ $? -eq 0 ] && miss=$((miss+1))
done
echo "$1: guard mancato $miss/200"
