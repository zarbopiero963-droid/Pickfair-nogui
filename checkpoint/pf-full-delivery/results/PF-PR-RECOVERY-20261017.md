# PF-PR-RECOVERY-20261017 — Rapporto di riconciliazione delle PR #505 / #508 / #512

Agente: Claude Code (sessione `session_01Wrefu68dTUWD1PXe8PTZjb`). Data: 2026-10-17. Modalità: audit reale del codice,
produzione in sola lettura dopo la messa in sicurezza del lotto già pronto. Nessun merge, nessuna chiusura, nessun
deploy, nessun LIVE, nessuna spesa, nessuna cancellazione. Gate A #506 NON PASS, Gate B non certificato, PR #505
protetta, LIVE NO-GO: invariati.

## A. Stato GitHub (riverificato con `git ls-remote` e API, 17/10 ~16:10 UTC)

| Oggetto | SHA / stato | Note |
|---|---|---|
| `main` | `028cee9074c3ab6c2a8014449a1897ae8faebc93` | invariata rispetto al controllo precedente |
| PR #505 `feat/issue461-pr28d-absolute-caps` | `618884e6c0aa82342bb98ad7c81a4cf8963f1fa4`, OPEN, 9 commit, 22 file | protetta, non toccata; Merge readiness FAIL (09/10), Sol/Grok verdi sul range, 22 thread |
| PR #508 `docs/od01-review-bf2-d03` | `a7a6d946eb11bb09df10136568b427a7cfe3dc38`, OPEN, 10 commit, 37 file | solo `docs/review/od01/`; ultima attività 12/10 15:34 |
| PR #512 `ccr-bcc81683-ud0hgc` | **`339cd244a8336d6d4ddd1d4880d588efccb0c2e1`**, OPEN, 15 commit, 68 file | era `043e34c` al controllo precedente: un push (lotto Codex-fix + FASE 4) prima del congelamento |

Nota sul push di `339cd24`: eseguito prima della lettura del mandato PF-PR-RECOVERY-20261017, come messa in
sicurezza di lavoro già verificato localmente (8 rilievi Codex su `8ffb4f3` con test RED→GREEN, riconciliazione
issue FASE 4). Dopo quel push il codice di produzione non è stato più modificato. Nessuna delle tre PR è stata
chiusa o mergiata.

CI della #512 su `339cd24` (37 check): verdi tutti i test/guard/parity/stateful; **rossi**: `overlap-check`
(tre PR aperte: blocco di governance, c.6236096085) e `Merge readiness` (UNSTABLE, 1 check bloccante, 11 thread
attivi). Review: Sol `043e34c…339cd24` nessun bloccante (review parziale, `simulation_broker.py` oltre budget);
Grok 2 bloccanti triagiati con prova (c.6240897866: nessun percorso di recovery ripiazza un ordine; due
GUARDRAIL_GAP a bassa severità in direzione fail-closed). Codex: 8 finding su `8ffb4f3` **corretti in `339cd24`**
con risposta in thread; 6 finding su `043e34c` triagiati (4 CURRENT_DEFECT → PATCH_REQUIRED, 1 GUARDRAIL_GAP
basso, 1 preesistente DEFERRED_BY_POLICY); su `339cd24` Codex in usage-limit. Label `manual-review-required`
presente (AGENTS.md/CLAUDE.md nel diff: merge manuale owner).

Sovrapposizioni dei file (calcolate su `origin/main...head`): #512∩#505 = 7 file (gli stessi sette del mandato);
#512∩#508 = 0; #505∩#508 = 0.

## B. Stato reale del codice sull'HEAD `339cd24` della #512

Suite completa eseguita davvero: `PYTHONPATH=netguard DISPLAY=:99 venv312/bin/python -m pytest -q -p no:cacheprovider tests`
(Python 3.12.3, Linux 6.18, rete non-loopback negata, Xvfb :99): **7720 passed, 14 skipped, 2 xfailed, 0 failed**
(18 min 16 s). Skip: 7 Telegram LIVE (credenziali assenti, LIVE vietato), 7 guardrail/ambiente; xfail: F033 (cap evento
canonico, dipende da #505) e recorder A2. Nessun test escluso.

| Area | Stato | Prove / limiti |
|---|---|---|
| Microstake nativo (SIM) | IMPLEMENTED + TESTED (SIM) | `core/microstake_safe.py`, `core/microstake_sim_port.py`, `core/trading_engine.py::_route_native_microstake`, `simulation_broker.cancel_orders(sizeReduction)`; 17+23+14+11 test dedicati + 53 Codex; Dutching 1,91 RED→GREEN. Stake richiesto = eseguito (rifiuto esplicito, mai sostituzione); cap transitorio = min(fondi disponibili, headroom esposizione totale); AMBIGUOUS fail-closed; nessun retry; recovery mai ripiazza. **Aperti**: resume() non ritenta la recovery Microstake (Codex 4303374299), precisione 2 decimali non validata (4303374312), snapshot fuori lock (Grok). |
| Microstake LIVE | MISSING (dichiarato) | nessun adapter Betfair: `MICROSTAKE_EXECUTOR_UNAVAILABLE`, fail-closed. Non LIVE-ready. |
| Money Management | IMPLEMENTED + TESTED | nessun arrotondamento al minimo (`stake_sotto_min_stake`), cap LAY su liability, F026v2/F032. **Decisione owner aperta**: stake calcolato sopra `max_single_bet` (oggi BACK ridimensionato al cap, LAY rifiutato). I cap assoluti (ordine 1 €, mercato 2 €, evento 2 €, drawdown 10 €) vivono SOLO nella #505. |
| Dutching | PARTIAL | gambe sotto 2 € eseguite via Microstake (test identità 3 ordini); PF-EXEC-02 (parziale: ribilancio/copertura/sospensione) PLANNED, non implementato. |
| Cashout | PARTIAL | hedge sotto 2 € via stesso esecutore (`OrderRouter._place_native_microstake`), LIVE senza esecutore = errore esplicito; `customer_ref` del cashout = `source` (non unico): finding aperto; PF-EXEC-03 (chiusura protettiva prioritaria) non implementato oltre l'esistente. |
| Recovery / riavvio | IMPLEMENTED + TESTED (SIM) | saghe pendenti riconciliate prima di F018, righe non attribuite ⇒ PAUSED, tavoli occupati riacquisiscono dedupe, betId annidato, tavoli surplus RSR-12, write failure fail-closed. **Aperto**: `resume()` (sopra); aggregazione per-bet dei settlement LIVE (4303297587, design). |
| Best Execution dinamico | MISSING (PLANNED) | PF-BEX-01 registrato in tracciabilità, nessun codice. PF-OD12-04 definitiva registrata, nessun codice. |
| PF-EXEC-01..06 | 01/02/03/05 PLANNED; 04 IMPLEMENTED (fondi insufficienti = blocco, cap transitorio); 06 IMPLEMENTED (riavvio/riconciliazione/STOP) con finding aperto | registro `docs/traceability/requirements.json` (19 requisiti: 9 TESTED, 3 IMPLEMENTED, 6 PLANNED, 1 NOT VERIFIED, 0 CERTIFIED) |
| Roserpina (41 parametri) | TESTED 36, NOT VERIFIED 1 (RSR-20 solo LIVE), BLOCKED 4 (RSR-38..41) | campagna 1684 scenari su `180f405`; F032/RSR-12 su `82f499c`; dossier `docs/roserpina41_parameter_dossier.md` |

## C. Confronto #512 contro #505 sui sette file comuni

Metodo: `git merge-tree --write-tree pr505 339cd24` (prova a tre vie), diff per funzione (AST) e **albero di
merge scratch** (`f63cf3a`, mai pubblicato) su cui sono stati eseguiti i test di entrambe le PR.

| File | Conflitto Git | Funzioni toccate da entrambe | Esito semantico |
|---|---|---|---|
| `.guardrails/allowed_scope.json` | **SÌ (testuale)** | registro task | banale: entry diverse (`issue461-pr28d-absolute-caps` vs `issue461-pr28-microstake-native-02`); si tengono entrambe |
| `CHANGELOG.md` | **SÌ (testuale)** | — | banale: due sezioni, si tengono entrambe |
| `core/money_management.py` | no | `calculate` | **compatibile**: #505 cambia `signal_side(signal, default="BACK")` e aggiunge il rifiuto lato in `evaluate_next_trade_after_settlement`; #512 aggiunge `stake_sotto_min_stake` prima del clamp e la crescita F032. Nessuna riga in comune. |
| `core/runtime_controller.py` | no | `_on_signal_received`, `_evaluate_and_maybe_submit_auto_next_trade`, `_release_if_terminal` | **compatibile per struttura**: #505 avvolge le prime due in `_serial` con lock/fence; le modifiche #512 restano nel corpo serializzato. `_release_if_terminal`: #505 non rilascia su `QUICK_BET_FILLED`; #512 rilascia solo il tavolo che porta l'`event_key` (F023) e trattiene su AMBIGUOUS (F024): il merge compone entrambe le condizioni. |
| `mini_gui.py` | no | `_build_roserpina_tab` | compatibile: #505 aggiunge 4 campi cap assoluti; #512 riordina le checkbox su più righe e cabla il provider Microstake del cashout. Handoff design da riallineare al merge. |
| `docs/design/design_handoff.md` | no | — | documentale, da riallineare |
| `tests/integration/test_issue461_pr26a_customer_ref_provenance.py` | no | nessuna | #505 aggiunge `side`; #512 aggiunge `exposure` e `list_current_orders` al servizio finto. Compatibili. |

**Risultato dei test sull'albero di merge scratch** (test di #505 + test Microstake/F026/F029/core di #512):
`848 passed, 31 failed, 1 xfailed`. **Tutti i 31 fallimenti** sono in `tests/f026/test_f026_financial.py` e hanno una
sola causa, verificata eseguendo il child: `SIGNAL_REJECTED max_order_exposure_exceeded:limit=1.0€`. I cap assoluti
della #505 (default 1 € per ordine) rifiutano gli stake degli scenari F026 (≥ 1 €, scritti senza cap assoluti).
Non è un conflitto di codice: è una **incompatibilità di contratto finanziario** fra le due PR che va decisa, non
"risolta" scegliendo una versione del file.

Rischi finanziari e decisioni che ne derivano (owner):

1. **Cap ordine 1 € (#505) + Microstake (#512)**: ogni ordine Roserpina sotto 2 € passerebbe sempre dalla saga
   (stub temporaneo da 2 €). Lo stub è limitato da fondi disponibili e headroom totale, **non** dai cap per
   ordine/mercato/evento della #505. Serve una decisione: l'importo temporaneo della procedura è soggetto ai cap
   assoluti (⇒ con cap ordine 1 € il Microstake non può mai partire) oppure solo a fondi + esposizione totale
   (come la rettifica 17/10 descrive). Fino alla decisione: integrazione BLOCCATA.
2. **Scenari F026 e campagna Roserpina**: vanno rieseguiti con i cap assoluti configurati esplicitamente dopo
   l'integrazione; i numeri attuali (1684 scenari, 7720 test) valgono solo senza #505.
3. **Lato dell'ordine**: #505 valida strict nel runtime e ripristina il default BACK nel calcolatore puro; #512
   valida il lato prima del routing Microstake (`INVALID_SIDE`). Coerenti, ma il test side-parity va rieseguito
   sull'albero integrato.
4. **Fence cashout (#505) × saga Microstake (#512)**: una CASHOUT barrier invalida gli ingressi in ammissione; una
   saga già in corso (stub piazzato) non è coperta dalla fence: da verificare con test dedicato all'integrazione
   (PF-EXEC-03).

Attribuzione: cap assoluti, fence, aggregazione identità canonica (F033) → **#505**; Microstake, MM senza
arrotondamento, recovery F018/RSR-12, continuità, tracciabilità → **#512**; riallineamento degli scenari F026 ai cap
assoluti, test fence×saga, stub vs cap assoluti → **PR di integrazione successiva** (dopo la decisione owner 1).

Strategia che non perde correzioni valide: (a) prima la decisione owner 1; (b) merge della #505 per prima
(Gate A/B #505 secondo #506), (c) rebase/merge di `main` nella #512 con i due conflitti testuali risolti tenendo
entrambe le entry, (d) adattamento degli scenari F026 ai cap assoluti (config esplicita, non abbassamento dei
cap), (e) riesecuzione completa + campagna, (f) Gate B #512. L'ordine inverso (prima #512) lascerebbe la #505 a
dover riscrivere i propri test contro il Microstake.

## D. Piano operativo proposto

1. **PR #508**: la revisione OD-01 ha ricevuto 10 push e 20 review Sol/Grok su tutti i range; restano **3 rilievi di
   contenuto non risposti** (Sol su `20_D03_LOTTO01_A_04.md` formula `replaceOrders` senza `matched(new)`; Grok su
   `21_D03_LOTTO01_B_04.md` retry monetario post-timeout e `B_06.md` Order Stream MISSING) e `40_D03_INDICE_LOTTI_01.md`
   non dichiara una chiusura. Non risulta un verbale di chiusura OD-01 su #506. Proposta: **mantenerla aperta solo
   come revisione documentale** finché l'owner non pubblica il verdetto OD-01 (ACCETTA/RESPINGI per ID) su #506;
   poi chiusura senza merge. Nessuna trasformazione in PR di implementazione.
2. **PR #505**: invariata, protetta, nessun merge, nessuna ripresa prima di Gate A specifico (#506 G2).
3. **PR #512**: PR di implementazione attiva, **in valutazione, non certificata**. Prossimi passi quando l'owner
   riapre la scrittura: un solo push con i 4 PATCH_REQUIRED Codex + 2 GUARDRAIL_GAP Grok (test RED/GREEN), poi
   regressione completa e giro Sol/Grok. Fino ad allora: sola lettura.
4. **Ordine di lavoro**: decisione owner su stub vs cap assoluti → #505 (Gate A/B) → #512 riallineata → PR di
   integrazione (scenari F026 con cap, fence×saga) → best execution / PF-EXEC sullo stesso motore.
5. **Prossima PR autorizzabile**: nessuna nuova PR finché #512 non è gestita; la prima PR dopo #512 è la PR di
   integrazione #505×#512 sopra (scope: `tests/f026/*`, `docs/design/design_handoff.md`, test fence×saga), non
   una PR funzionale.

## E. Evidenze

- Regressione completa `339cd24`: log `fullreg_339cd24/pytest.log` nel checkpoint (`results/`), comando ed
  ambiente in `env.txt`.
- Albero di merge scratch `f63cf3a` (#505 `618884e` + #512 `339cd24`, conflitti testuali risolti tenendo
  entrambe le versioni), test: `merged_tests.log`, elenco fallimenti `merged_failed.txt`, causa:
  `SIGNAL_REJECTED max_order_exposure_exceeded:limit=1.0€` (child F026 eseguito a mano).
- Diff per file e per funzione: `recovery/*.diff`, `mergetree_505_512.txt`.
- Thread Codex: risposte r4303408368…r4303412445; Grok: c.6240897866; overlap-check: c.6236096085.
- Tracciabilità: `docs/traceability/requirements.json` @ `339cd24` (validatore PASS, 0 CERTIFIED).
- Checkpoint: `agent-state/pf-full-delivery` seq 15 (questo rapporto in `results/PF-PR-RECOVERY-20261017.md`).

## F. Decisioni owner richieste (STOP + DOMANDA)

1. Importo temporaneo dello stub Microstake (2 €) soggetto ai cap assoluti per ordine/mercato/evento della #505:
   SÌ (⇒ con cap ordine 1 € nessun Microstake possibile, serve cap ≥ 2 € o esenzione) / NO (solo fondi + esposizione totale).
2. Stake calcolato sopra `max_single_bet`: ridimensionare al cap (oggi) o rifiutare (rettifica 17/10 letta in senso stretto).
3. Ordine di merge: #505 prima di #512 (proposta) o viceversa.
4. OD-01: pubblicare il verdetto per ID su #506 e autorizzare la chiusura della #508 senza merge.
5. Conferma del lo stato vigente «produzione in sola lettura»: durata e condizione di riapertura (push unico dei 6 fix triagiati).
