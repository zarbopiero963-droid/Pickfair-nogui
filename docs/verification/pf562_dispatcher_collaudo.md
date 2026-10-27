# PF-562 — collaudo del dispatcher trusted: matrice delle prove

Registro della certificazione della #562 (dispatcher trusted a polling, `scripts/pr_trusted_dispatcher.py`).
Questo file è il contenuto della **PR di collaudo** (blocco B1 del rapporto Grok, #562 c.6450558425): una PR senza
modifiche funzionali, che serve solo a dare al dispatcher una PR reale da osservare (bozza, pronta, nuovo head,
chiusura e riapertura, rerun). Gli esiti si registrano sulla #562; qui resta la mappa.

**Classi di prova** (mandato owner PF-562-SYSTEMD-CREDENTIAL-D1-FIX-01, §7):

| Classe | Significato | Chi |
|---|---|---|
| A | offline / mock: finto GitHub e finto Buildkite (`tests/scripts/test_pr_trusted_dispatcher.py`), mutation gate | Claude |
| B | DRY-RUN sulla VPS: `check-config --check-secrets` nel contesto systemd, `serve --dry-run` (nessuna build, nessuno status) | Grok |
| C | end-to-end reale: build Buildkite e status GitHub sullo SHA corretto | Grok, **solo con GO owner separato** |

Esiti ammessi: PASS, FAIL, BLOCKED, NOT_RUN, PARTIAL, sempre con evidenza verificabile (SHA, timestamp, comandi,
output con i segreti redatti). A e B non sostituiscono C. #562 resta **NOT CERTIFIED** finché i requisiti C
obbligatori non sono dimostrati.

## Stato di partenza

| Voce | Valore |
|---|---|
| Codice in collaudo | `main` = `d1599187a1b1ead3c666b250e46e64eeba918427` (#572, D1) |
| Classe A | PASS su `d159918`: suite `tests/scripts` + `tests/guardrails` 3491 passed, mutation gate `trusted_dispatcher` 90/90 |
| D1 (credenziale systemd 255 `0440 root:root` via ACL) | corretto in #572; da riverificare in B (P5–P6) |

## Prove B (DRY-RUN VPS) che usano questa PR

Riferimento: checklist di `docs/trusted_dispatcher.md` e rapporto Grok c.6450558425.

| ID | Prova | Azione sulla PR di collaudo | Atteso in DRY-RUN |
|---|---|---|---|
| DR02 | lettura delle PR aperte | nessuna (la PR è aperta) | la PR compare nel polling |
| DR03 | bozza | la PR nasce in bozza | nessun job (bozza non valutabile) |
| DR04 | pronta per la review | bozza → pronta | job `dry_run` per i due guard sull'head |
| DR05 | nuovo head | push di un commit su questo file | job del vecchio head sostituiti, nuovi job sull'head nuovo |
| DR06 | chiusura e riapertura | chiusura, poi riapertura | chiusura dopo due giri senza la PR; riapertura = transizione `reopened` |
| DR09 | rerun autorizzato | commento `/pf-trusted-rerun <sha40>` dell'owner | riga `reruns` accettata, nessuna build |
| DR10 | rerun non autorizzato | commento con SHA vecchio o di un altro autore | riga `reruns` rifiutata |

In tutte: **zero** build Buildkite e **zero** status GitHub, archivio reale non creato.

## Prove C

Tutte **NOT_RUN** fino a un GO owner separato per build reali e pubblicazione degli status.
