# PF-ISSUE-CLEANUP-01 — Piano di pulizia (Fase 1, in attesa di approvazione owner)

Agente: Claude E. Base: `main` @ `a09d8d6` (08/11/2026). 82 issue aperte; nessuna PR aperta (#609 e #610 mergiate).
Nessuna issue toccata in Fase 1. Matrice completa: `MATRIX.md` / `MATRIX.json` in questo checkpoint.

## Esito
| Classe | N. | Issue |
|---|---|---|
| KEEP | 50 | 11 registri (#351 #426 #453 #461 #489 #491 #506 #562 #582 #586 #588) + 39 issue di lavoro |
| SUPERSEDED | 11 | #305 #328 #329 #330 #331 #332 #337 #338 #367 #412 #459 |
| DUPLICATE | 1 | #456 → #455 |
| CLOSE_VERIFIED | 4 | #528 (P3), #550 (P2); #536 (P1), #590 (P1) |
| CONSOLIDATE | 6 | #290 #301 #302 #312 → #461; #394 → #416; #509 → #562 |
| NEEDS_REVIEW | 10 | #274 #374 #398 #454 #455 #457 #514 #565 #578 #605 |

## Vincolo da sciogliere prima di chiudere qualsiasi issue superata
#489 («nessuna chiusura automatica per consolidamento») e #453 c.5653916051 §C («non chiudere le fonti
raggruppate») vietano le chiusure per consolidamento senza decisione owner. **L'approvazione di questo piano
vale come decisione owner esplicita SOLO per le issue elencate nel lotto approvato**; tutto il resto resta aperto.

## Lotto 1 — chiudibile in autonomia dopo l'approvazione (13 issue)
Ogni chiusura: commento conclusivo con motivo, sostituta, PR/commit, file:riga su main, poi `close`
(`completed` per CLOSE_VERIFIED, `duplicate` per DUPLICATE, `not_planned` per SUPERSEDED).
- CLOSE_VERIFIED: #528 (harness `tests/f026/child_f026.py:249-261,310-328`), #550 (PR #552, `scripts/pr_autonomy_policy.py:1001`, test 161 passed).
- DUPLICATE: #456 → #455.
- SUPERSEDED (report/roadmap storici; requisiti validi in issue aperte): #305→#461, #328→#453, #329→#293,
  #330→#453, #331→#453 (+#290), #332→#335, #337→#453, #338→#453, #367→#351, #412→#426 D19, #459→#453 X07.

## Lotto 2 — CLOSE_VERIFIED P1: serve un sì esplicito
- #536 runtime lock: PR #537 merged (lock OS + inode + lease con fencing, `core/runtime_authority.py`); limiti residui
  (Windows MANUAL_ONLY, TOCTOU) tracciati in #453/#506; LIVE in #351.
- #590 guard Telegram fail-open: PR #592 merged, `scripts/pr_full_suite_pipeline.py:321` usa `compgen -e TG_`; test con ambiente grande PASS su a09d8d6.

## Lotto 3 — CONSOLIDATE: trasferimento esplicito poi chiusura (decisione richiesta)
Requisiti trovati SOLO nelle issue sorgenti (da copiare come commento nella destinazione prima di chiudere):
- #290/#301 → #461 PR08/PR09: CS_OTHER/HT espanso in 3 selection_id con fail-closed, helper best-price per selection_id, mapping punteggio→selection_id, esempi e2e (Over 2.5, dutching Risultato Esatto).
- #302 → #461: CANCEL_UNMATCHED come comando standard, CLOSE_EVENT come cashout di tutte le gambe dell'evento, % fissa/dal messaggio/range con SKIP, ordine bet→azioni.
- #312 → #461 PR42: banco forced-SIM (flag esplicito prima del chokepoint, fail-closed senza SimulationBroker, zero ordini reali, isolamento bankroll).
- #394 → #416: nota «Riferimenti visivi», procedura segnaposto screenshot, sezioni handoff mancanti, cartella unica screenshot.
- #509 → #562 §8: custodia credenziali 12/10, solo workflow_dispatch, test negativi place/cancel/replace/update, matrice test con Xvfb, runner effimero.

## Domande owner (NEEDS_REVIEW)
1. **#274** (titolo «NON chiudere»): chiudere come SUPERSEDED da #453, dopo aver riportato in #453 i 11 test TOP30 Parte 2 e i 6 follow-up cashout?
2. **#374**: chiudere come storica? D-9 (stake dal messaggio), D-11 (tastiere Bot API da Pickfair-MarketAPI), D-12 (CS_OTHER 3 gambe) sono vigenti e vanno annotate in #426/#461?
3. **#454 / #455 / #457**: la regola #453 §C vale ancora? Se no, chiudere #454 e #455 come SUPERSEDED (#453/#461). #457 contiene P0 aperti (tracciati in PR28/PR29/PR37): proposta KEEP fino alla chiusura di quelle schede.
4. **#398**: abbandonare (not_planned, P11 + sospensione P41) o mantenere con budget Sol/Grok?
5. **#514**: le divergenze residue (PF-C22-01, freshness Delayed/Live, migrazione pre-v9) vanno spostate in #586/#506 e #514 chiusa, o #514 resta il registro canonico?
6. **#565**: basta l'evidenza documentale (integration_id 15368 + PR successive) o serve il test negativo con identità separata?
7. **#578**: dopo t257 riscrivere il corpo (kit recovery, pulizia VPS MatchPilot, revoca PAT) o chiudere spostando le attività in #562?
8. **#605**: un EXTERNAL_PENDING/NOT_RUN deve restare scheda PF-DEF, o va tracciato in #586 e la card chiusa?
9. **Conflitto rilevato (non da chiudere)**: #309 e #426 D10 prevedono «superamento tetto = CAP» per i follower, in contrasto con la decisione owner 2 e PF-EXEC-04 del 17/10 (mai ridimensionamento). Da confermare quale prevale.

## Cosa NON si tocca
Registri (#586, #588, #461, #426, #453, #351, #491, #489, #506, #582, #562), P0/P1, Gate A, certificazioni, sicurezza
finanziaria. Nessuna modifica a codice, workflow, VPS o LIVE. Nessuna seconda roadmap. Nessuna interferenza con la
corsia di Claude B (nessuna PR aperta; coda dichiarata in #586: F-R06 → pfckpt → BB-1).

## Risultato atteso
Dopo Lotto 1: 69 aperte. Con Lotti 2–3 e le risposte alle domande: fino a ~55, ciascuna con lavoro reale.

## Limiti dell'audit
- Clone a profondità 50: la provenienza delle PR più vecchie è verificata dalla ricerca GitHub e dallo stato dei file su main, non da `git log`.
- Per le issue con centinaia di commenti (registri) gli auditor hanno letto le pagine pertinenti, non ogni commento.
- Nessun test eseguito oltre a quelli mirati citati (#550, #590, #611).
