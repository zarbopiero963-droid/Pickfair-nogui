# PF-CLAUDE-E-ISSUE-CLOSURE-ONLY — verdetti di chiusura (09/11/2026)

Base: main `6519794b125152e62ca02b1821854075f8fc6653` (dopo #614). Prove ricontrollate sul main attuale per tutti i difetti di codice.
Già chiuse con prove: #456 (dup #455), #528, #550, #590. Issue aperte esaminate: **79** (78 dell'audit + #613 nuova).

| Issue | Stato | Requisiti completati | Requisiti mancanti | PR/commit | Test | Chiusura |
|---|---|---|---|---|---|---|
| #274 | KEEP_OPEN | consolidata in #453 | B7/B10, TOP30 parte 2, fasi 3–5, 6 follow-up cashout | #271–#327 storiche | — | no (owner: «NON chiudere») |
| #285 | KEEP_OPEN | snapshot prima del flip SIM (`core/runtime_controller.py:2246`) | test SIM≠LIVE, test sull'ordine, commento `:2241` impreciso | 3511eed | manca il test BLOCK | no |
| #289 | KEEP_OPEN | — | tab Osservabilità con pannelli reali (#426 D17) | — | — | no |
| #290 | KEEP_OPEN | catalogo no-wipe, SIGNAL_NOT_DICT, priceProjection | CS_OTHER 3 gambe, best-price per selection_id, PR06–PR10 | #322 | — | no |
| #293 | KEEP_OPEN | — | replace TIMEOUT→AMBIGUOUS (`order_manager.py:174`), reload SIM, prezzo invariato | — | — | no |
| #297 | KEEP_OPEN | Storico Bet solo SIM | LIVE+SIM, paginazione, writer fuori dal core, coordinator | — | — | no |
| #301 | KEEP_OPEN | — | casi d'accettazione Over 2.5 / dutching CS | — | — | no |
| #302 | KEEP_OPEN | % soglia+frazione (P34), attesa (P35-A) | CANCEL_UNMATCHED, CLOSE_EVENT, SKIP % mancante | — | — | no |
| #305 | KEEP_OPEN | — | piano: requisiti in #290/#302/#306/#309/#312/#320, non completati | — | — | no |
| #306 | KEEP_OPEN | — | 25 PF-CASH NOT_RUN, motore condizionato, Void hedge (P1) | — | — | no |
| #309 | KEEP_OPEN | regola stake = REJECT registrata (#426 c.6728203497) | fan-out, 4 modalità follower | — | — | no |
| #312 | KEEP_OPEN | `app_key_delayed` su main | Banco Prova forced-SIM | — | — | no |
| #320 | KEEP_OPEN | parte di D1/A (PR #340-era) | RSR-08/39/40/41, SL/TP € reali (P1) | — | RSR-01..41 non certificati | no |
| #328 | KEEP_OPEN | — | il report dichiara un completamento non provato; requisiti in #285/#320/#290 aperti | — | — | no |
| #329 | KEEP_OPEN | — | come #293 (fonte protetta §C) | — | — | no |
| #330 | KEEP_OPEN | — | rilievi in #320/#297/#293 aperti (fonte protetta §C) | — | — | no |
| #331 | KEEP_OPEN | — | risoluzione deterministica/dutching in #290 aperta | — | — | no |
| #332 | KEEP_OPEN | — | finding Manus da riconvalidare in #335 | — | — | no |
| #335 | KEEP_OPEN | alcuni finding chiusi | riconvalida di ogni finding su main (P1) | — | — | no |
| #337 | KEEP_OPEN | principi trasversali in CLAUDE.md | sequenza PR non completata | — | — | no |
| #338 | KEEP_OPEN | PR #340, #342, #344 | fasi 3–5 go-live, CATSYNC, D1/D2 | #340 #342 #344 | — | no |
| #343 | KEEP_OPEN | decisione opzione A | `logout()` non chiama `/api/logout` (`betfair_client.py`) | — | — | no |
| #345 | KEEP_OPEN | confronto registro con base (`scripts/guardrail_check.py:193`) | allowlist file critici dal base, wiring test merge-marker | — | — | no |
| #351 | KEEP_OPEN | — | registro collaudo reale (P0) | — | — | no |
| #356 | KEEP_OPEN | — | `finally` di `main()` fa solo `app.destroy()` (`mini_gui.py`) | — | — | no |
| #359 | KEEP_OPEN | PR-A ConfigRegistry | PR-B…E: `--config-report`, tab GUI, logging gate | #366 | — | no |
| #367 | **BLOCKED** | fix di codice CatalogSync/login/anomaly su main | verifica reale su VPS richiesta prima di chiudere, mai fatta | #368 #370 #371 #372 | test mockati | no: manca la prova reale |
| #374 | **OWNER_DECISION** | consolidata in #453/#461 | D-9, D-11, D-12 non registrate altrove | — | — | dopo decisione |
| #380 | KEEP_OPEN | — | sweep finale GUI (PR60) | — | — | no |
| #388 | **OWNER_DECISION** | M1 (PR #447), A1 (PR #448) | decisioni su M2–M10, B1–B3 | #447 #448 | — | dopo decisione |
| #392 | KEEP_OPEN | — | assistente AI sospeso (blueprint) | — | — | no |
| #394 | KEEP_OPEN | handoff su main (PR #414) | sezioni handoff, nota visivi, percorso screenshot | #414 | — | no |
| #398 | **OWNER_DECISION** | — | campagna 69 PR: abbandonare o finanziare | — | — | dopo decisione |
| #410 | KEEP_OPEN | — | `chat_id` non numerico accettato (`telegram_module.py:1303-1317`) | — | — | no |
| #412 | **CLOSE_VERIFIED** | valutazione chiusa con decisione owner #426 D19 (opzione 1, status quo); codice coerente: per-bot `invariant_ok=True`, `listener_stale=False` (`telegram_bot_runtime.py:725,736`); doc `ops/telegram_bots.md:185` | nessuno: la decisione esclude modifiche | — (nessuna modifica richiesta) | n/a | **GO richiesto** |
| #415 | KEEP_OPEN | — | `on_success=` non accettato (`mini_gui.py:2732` vs `:2269`) | — | — | no |
| #416 | KEEP_OPEN | — | invarianti in AGENTS.md, regola screenshot | — | — | no |
| #426 | KEEP_OPEN | — | registro decisioni owner | — | — | no |
| #437 | KEEP_OPEN | — | ombrello finale dopo PR59/PR60/#351 (P1) | — | — | no |
| #446 | KEEP_OPEN | — | hard-verify del commit c8fcaa3 mai eseguito | — | — | no |
| #453 | KEEP_OPEN | — | piano operativo | — | — | no |
| #454 | KEEP_OPEN | rilievi mappati in #453/#461 | Top10 non completati (fonte §C) | — | — | no |
| #455 | KEEP_OPEN | decisione «Quant escluso» | X01–X04 non completati (fonte §C) | — | — | no |
| #457 | KEEP_OPEN | — | P0-1 esposizione proiettata, P0-2 barriera STOP | — | — | no |
| #458 | KEEP_OPEN | — | auto-update X06 assente | — | — | no |
| #459 | KEEP_OPEN | — | valutazione Desktop Commander X07 non fatta | — | — | no |
| #460 | KEEP_OPEN | A3 in parte (PR #609) | cifratura XOR (`core/secret_cipher.py`), B1–B9 (P0) | #609 | — | no |
| #461 | KEEP_OPEN | — | roadmap | — | — | no |
| #471 | KEEP_OPEN | documentazione del limite | limite permanente tenuto aperto dall'owner | #470 | — | no |
| #472 | KEEP_OPEN | visibilità (#475) | enforcement reale; owner: «resta OPEN» | #475 | — | no |
| #489 | KEEP_OPEN | — | promemoria vigente (CLAUDE.md) | — | — | no |
| #491 | KEEP_OPEN | — | ingresso MCP | — | — | no |
| #495 | KEEP_OPEN | — | 7 endpoint Betting, 3 Account, Order Stream (P1) | — | — | no |
| #497 | KEEP_OPEN | — | ledger PR-flow (rinviato) | #496 | — | no |
| #506 | KEEP_OPEN | — | Gate A/B (P0) | — | — | no |
| #507 | KEEP_OPEN | — | 68 test spec NOT_RUN (P1) | — | 0 eseguiti | no |
| #509 | KEEP_OPEN | parte in #562 §8 | lane QA non attiva | — | — | no |
| #510 | KEEP_OPEN | Round 1 SIM | Round 2 multi-tavolo, replay | — | — | no |
| #511 | KEEP_OPEN | — | `roserpina_config_*`, dutching misto (`dutching.py:382`) (P1) | — | — | no |
| #513 | KEEP_OPEN | cap 10 cicli (`scripts/pr_fix_loop_policy.py:22`) | `docs/pf_qa_02.md`, punti 3–11 | #530 | — | no |
| #514 | **OWNER_DECISION** | molte decisioni ratificate (#522, #610) | PF-C22-01, freshness Delayed/Live, migrazione pre-v9 | #522 #610 | — | dopo decisione |
| #515 | KEEP_OPEN | — | motore auto-sblocco non implementato | — | — | no |
| #516 | KEEP_OPEN | — | ref hedge uuid per chiamata (`core/order_router.py:166`), saghe ACTIVE escluse (P1) | — | — | no |
| #517 | KEEP_OPEN | — | floor 0,10 € (`core/microstake_safe.py:83`) | — | — | no |
| #518 | KEEP_OPEN | — | `pf_trace --base`, lease obbligatoria (fix pfckpt non su main) | — | — | no |
| #519 | KEEP_OPEN | — | `NEXT_TRADE_SUBMIT_CONFIRMED` prima del verdetto (`core/runtime_controller.py:8289`) | — | — | no |
| #520 | KEEP_OPEN | — | `AttributeError` su book malformato (`simulation_broker.py`) | — | — | no |
| #521 | KEEP_OPEN | identità settlement (PR #535) | aggregazione per bet_id (test `:210` ancora a -2 senza rilascio) | #535 | — | no |
| #536 | KEEP_OPEN | Linux: lock+inode+lease, 61+227 test, 47/47 mutanti | checklist Windows (criterio MISSING), test 2 processi con sostituzione, limiti FS di rete non registrati | #537 | 61 passed | no finché A/B/C non registrati |
| #562 | KEEP_OPEN | — | CI VPS | — | — | no |
| #565 | **OWNER_DECISION** | ruleset con app 15368 configurato | test negativo (check omonimo di altra app) | — | NOT VERIFIED | dopo decisione |
| #578 | **OWNER_DECISION** | — | kit recovery, pulizia MatchPilot, revoca PAT; checklist backup superata da t257 | — | — | dopo decisione |
| #582 | KEEP_OPEN | — | collaudo Betfair A–D (P0) | — | — | no |
| #586 | KEEP_OPEN | — | coordinamento | — | — | no |
| #588 | KEEP_OPEN | — | ricerca adapter | — | — | no |
| #605 | KEEP_OPEN | — | scenario ext-mutation NOT_RUN | — | — | no |
| #611 | KEEP_OPEN | — | test rosso su main `6519794b` | — | 1 failed | no |
| #612 | KEEP_OPEN | — | `position_stake` usa lo stake richiesto (`core/sl_tp_pnl.py:72-76`) | — | — | no |
| #613 | KEEP_OPEN | — | latch SL/TP dopo fallimento hedge provato (catena F-04/F-05, non su main) (P1) | — | — | no |

## Totali
- Esaminate: **79** aperte (più 4 già chiuse con prove: #456, #528, #550, #590).
- Chiudibili con prove: **1** → #412 (CLOSE_VERIFIED). CLOSE_DUPLICATE_VERIFIED: 0.
- Da tenere aperte: **71** KEEP_OPEN.
- Bloccate per mancanza di prove: **1** → #367.
- Decisione owner: **6** → #374, #388, #398, #514, #565, #578.
