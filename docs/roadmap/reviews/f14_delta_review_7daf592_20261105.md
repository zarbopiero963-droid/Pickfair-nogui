# Review delta F-14 (db11a86..7daf592) — revisore indipendente

Head verificato: 7daf5927 (414047e + 1 commit doc/test MCP-01k). Merge-base = origin/main ada40765. Lavoro offline, nessuna azione su GitHub.

## Verdetto: FIT per aprire la PR — nessun P0/P1, nessun P2 bloccante; 3 NIT (gap di test / precisione doc)

## 1. Probe precedenti
34/34 PASS su 414047e e su 7daf592; le 10 che fallivano su db11a86 (P1-1: 8, P2-1: 2) ora passano.

## 2. Verifica del codice del delta
- Client `betfair_client.py:1729-1736`: FAILURE/PWE + almeno un report non FAILURE (o non dict) => `BET_OUTCOME_MIXED`, in `_RISPOSTA_ILLEGGIBILE_DOPO_L_INVIO` (:61) => order_unknown + `result` conservato.
  FAILURE/PWE senza report o con tutti i report FAILURE => `BET_FAILED`, order_unknown False => FAILED (test `test_engine_rifiuto_grezzo_tutti_failure_resta_definitivo`, mutante N06 "sempre mixed" ucciso). Non è un AMBIGUOUS indiscriminato.
- Engine `core/trading_engine.py:1452-1462`: stessa regola sul corpo grezzo, `ERROR_AMBIGUOUS` + `response` => `_resolve_ambiguity` (riga AMBIGUOUS, riserva/tavolo/chiavi trattenuti, nessun QUICK_BET_FAILED). Tutto-FAILURE resta FAILED (N12 ucciso).
- Nessun reinvio: placeOrders resta `single_shot=True`, nessun nuovo POST/percorso d'invio nel delta (solo classificazione).
- Cancel `_cancel_result` (:1868-1891): report non SUCCESS/FAILURE => `unknown` => AMBIGUOUS anche con `requested` vuoto; FAILED solo senza cancelled né unknown.
  `emergency_stop` (`core/runtime_controller.py:2301-2324`) su ok=False registra errore, non conta cancellati: con report ignoti outcome AMBIGUOUS, ok False => mercato NON dichiarato cancellato.

## 3. Test (numeri reali)
- Branch (4 file F-14) + probe: 183 passed (7daf592); 146 passed senza probe (414047e).
- Regressione su 414047e: tests/acceptance, core, reconciliation, scripts, guardrails + 137 file che importano betfair_client/order_manager/trading_engine/order_router/cashout_executor: **8282 passed, 9 skipped, 0 failed**.
- Su 7daf592 (delta = doc + 1 test): tests/acceptance + tests/guardrails + test_issue461_pr26a: **2441 passed, 8 skipped, 0 failed**.

## 4. Mutazioni (22 sul delta, test del branch)
Uccise 19: M10b (`==`→`>=`), M11, M22, N01-N04, N06-N10, N12-N18 (client mixed off/fuori set/report senza status=FAILURE/solo PWE/sempre mixed; engine off/solo PWE/report senza status/error_type FAILED/sempre/dopo semantic; cancel unknown non raccolto, uncertain solo missing, FAILED/SUCCESS/finale che ignorano unknown). Più il mutante MCP-01k (candidato che espone sizeMatched): ucciso dal nuovo test.
Sopravvissute 3:
- M10 originale (`len(reports) >= len(requested) and bool(requested)`): il nuovo test M10 ha report con betId esplicito e conteggio uguale, quindi non lo copre. Effetto reale: solo risposta malformata (più report che istruzioni, senza betId) => IndexError/attribuzione posizionale. NIT.
- N05 (client mixed solo se TUTTI i report non FAILURE): nessun test con PWE + [FAILURE, TIMEOUT]. place_bet invia una sola istruzione, quindi è un caso multi-report non realistico. NIT.
- N11 (engine non conserva `response` su DOWNSTREAM_MIXED_OUTCOME): betId del corpo grezzo non persistito per i broker non-BetfairClient; il LIVE passa da BetfairClient (order_unknown, coperto da M20). NIT.

## 5. Consumatori di importi da righe incerte (vincolo Claude C / MCP-01k)
Nessun consumatore tratta come eseguiti gli importi del report conservato di righe AMBIGUOUS/INFLIGHT/HEDGE_AMBIGUOUS/HEDGE_PENDING. Sostanza confermata.
Letture di `orders.response_json`:
- `Database._order_bet_id`, `find_orders_by_bet_id`, `_live_bot_orders` (via `_order_bet_id`) e `RuntimeController._open_order_candidates` leggono solo il betId.
- `Database.get_order` serve solo al merge di `update_order`.
- `CashoutExecutor._hedge_rows`/`_previous_hedge_block` leggono `response.bet_id` solo di righe UNMATCHED/PLACED.
- `sizeMatched`/`averagePriceMatched` si leggono altrove solo da listCurrentOrders/settlement (cashout_executor, cashout_resolver, reconciliation_engine, microstake_live_port, runtime_controller:5195).
NIT doc §1: l'elenco «uniche letture» è incompleto (mancano `_live_bot_orders`, `get_order`, `CashoutExecutor._hedge_rows`; `cashout_executor.py` sta alla radice, non in core/). Sono comunque tutte letture di sola identità.
Osservazione (non bloccante): con `result` ora conservato su order_unknown, `OrderRouter._normalize_live` (`core/order_router.py:238-260`) somma `sizeMatched` del report conservato nel campo `matched` di un esito AMBIGUOUS. Valore usato solo per l'audit/notifica di CashoutResidualHandler (`_handle_ambiguous`) e scritto nel response della riga HEDGE_AMBIGUOUS. Mai per posizione/PnL/chiusura della richiesta (`_settle_cashout_request` uncertain=True). Il nome «matched» su un esito ignoto può però fuorviare chi legge l'audit: conviene un'etichetta «non confermato» in una PR successiva.

## 6. Scope e documentazione
- Task key `d_place_no_outcome_ambiguous`: `files` == diff origin/main...7daf592 (15/15, differenza vuota); nessun'altra key né `default` toccata; nessun workflow o file a merge owner.
- Doc aggiornata: riga 20-21 (rifiuto dichiarato vs misto), §2-bis cancel, §1 MCP-01k. CHANGELOG e catalogo mutazioni aggiornati (3 voci: f14_place_mixed_failed, f14_engine_mixed_failed, f14_cancel_unknown_report_ignored).
- PR safety-critical: sì — betfair_client, order_manager, core/trading_engine, core/runtime_controller, database.

## Findings
- P0: nessuno. P1: nessuno. P2: nessuno.
- NIT-1: M10 originale e N05 sopravvivono (`betfair_client.py:1869`, `:1733-1736`): aggiungere test con più report che istruzioni senza betId, e con PWE + [FAILURE, TIMEOUT].
- NIT-2: N11 (`core/trading_engine.py:1461`, `response=response` non coperto da test).
- NIT-3: doc §1 MCP-01k: elenco dei lettori di response_json incompleto; `matched` del router su AMBIGUOUS è un valore non confermato esposto con un nome da dato eseguito.
