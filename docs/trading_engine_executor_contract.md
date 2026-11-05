# TradingEngine ↔ ExecutorManager — contratto d'esito e rilascio della riserva

Riferimenti: PF-ALL-F001, PF-ALL-F023, PF-ALL-F024 (#453), suite `tests/f001/`.

## 1. Esito di un ordine affidato all'executor (F001)

`TradingEngine._execute_via_executor` restituisce **sempre** il risultato concreto della chiamata al
broker oppure solleva. Non restituisce mai un `concurrent.futures.Future` né un handle non risolto.

| Situazione | Classificazione | Rilascio della riserva |
|---|---|---|
| Risposta del broker con esito positivo | `SUBMITTED` (`ACCEPTED_FOR_PROCESSING`) | no: posizione viva |
| Rifiuto confermato: status `FAILURE`, `ok: False`, oppure **tutti** i report `FAILURE` (anche con status top-level `SUCCESS`, ad esempio LAPSED) | `FAILED` | sì |
| `submit` rifiutato dall'executor (chiuso o pieno), Future cancellato prima dell'avvio | `FAILED` pre-invio (`_ErrorePrimaDellInvio`): il client non è mai stato chiamato | sì |
| Attesa scaduta con task **non** avviato: `Future.cancel()` riuscito | `FAILED` pre-invio: è garantito che il task non partirà mai | sì |
| Attesa scaduta con task **avviato**, executor guasto dopo l'avvio | `AMBIGUOUS` + riconciliazione | **no** |
| Risposta persa / `TimeoutError` del client, `order_unknown`, report discordanti, risposta non-dict | `AMBIGUOUS` + riconciliazione | **no** |
| Risposta che dichiara esplicitamente `SUCCESS` senza alcun `betId` | `AMBIGUOUS` + riconciliazione | **no** |
| `placeOrders` LIVE: risposta 2xx arrivata **senza esito** — JSON-RPC senza `result`/`error`, `result` vuoto o senza `status`, `status` fuori da `SUCCESS`/`FAILURE`/`PROCESSED_WITH_ERRORS`/`TIMEOUT`, instruction report senza `status` o con uno status sconosciuto. `BetfairClient.place_bet` la marca `order_unknown` (`BET_STATUS_UNRECOGNIZED`, `BET_REPORT_STATUS_UNRECOGNIZED`) e conserva la risposta grezza in `result`; l'engine la persiste nel `response_json` della riga AMBIGUOUS, cosi' un `betId` gia' assegnato resta l'identita' dell'ordine (`Database.find_orders_by_bet_id`, settlement per bet). Lo stato resta AMBIGUOUS | `AMBIGUOUS` + riconciliazione | **no** |
| `placeOrders` LIVE: rifiuto **dichiarato** da Betfair (`status` `FAILURE`/`PROCESSED_WITH_ERRORS`, report `FAILURE`, elemento JSON-RPC `error`) | `FAILED` | sì |
| Esito ignoto, ma la transizione DB `INFLIGHT→AMBIGUOUS` fallisce (#524) | `AMBIGUOUS` non persistito: `is_terminal=False`, `lifecycle_stage="degraded"`, `QUICK_BET_AMBIGUOUS`; la riga resta `INFLIGHT` su DB e dedupe e riconciliazione la ritrovano al riavvio. **Mai** `FAILED` | **no** |
| Eccezione dopo l'avvio dell'invio (transizione `SUBMITTED` fallita, crash nella costruzione dell'esito, finalize che solleva) (#524) | `AMBIGUOUS` + riconciliazione (`ENGINE_FATAL_AFTER_SEND`), anche con DB indisponibile. **Mai** `FAILED` | **no** |
| Eccezione sollevata dal client | propagata **invariata**: stessa classificazione del percorso diretto | secondo la classificazione |

- **Attesa.** Il tempo di attesa vale `executor.default_timeout`. Se il valore manca o non è valido si usano 30 s (`ExecutorManager` di produzione: 30 s). Deve restare **maggiore** del timeout HTTP del `BetfairClient` (20 s), altrimenti si producono AMBIGUOUS evitabili. Il valore per LIVE resta un'ipotesi da validare (decisione owner).
- **Thread.** L'attesa avviene nel worker dell'EventBus che esegue l'handler, mai nel thread della GUI. Un engine invocato da un worker dello stesso pool dell'executor rifiuta prima dell'invio (`EXECUTOR_REENTRANT_SUBMIT_REFUSED`), per evitare il deadlock.
- **Messaggi pre-invio.** Non contengono la parola "timeout": `_handle_submit_exception` la legge come ambiguità, mentre la classificazione pre-invio è per **tipo**.
- **Rollback d'emergenza.** `executor=None` nel wiring usa il percorso diretto, che rispetta gli stessi esiti (parità verificata da `tests/f001`). Non governa però i task in coda (cancel) né un limite d'attesa proprio, e non è una soluzione certificata.

## 2. Eventi terminali e rilascio del tavolo (F023 / F024)

- Gli eventi del bus dell'engine (`QUICK_BET_*`) portano `table_id` e `market_id`, presi dal payload `CMD_QUICK_BET` del RuntimeController (`decision.table_id`).
- `RuntimeController._release_if_terminal`:
  - **`QUICK_BET_AMBIGUOUS`**: non rilascia nulla. Tavolo, esposizione, riserva e chiave anti-duplicato restano acquisiti fino a un esito certo. Nessun retry, nessun nuovo ingresso sullo stesso intento.
  - **Esiti certi** (`QUICK_BET_FAILED`, …): rilascio della chiave duplicati e del tavolo tramite `TableManager.force_unlock_if_event(table_id, event_key)`. Il tavolo si libera **solo** se porta ancora la riserva di quell'`event_key`. Controllo e rilascio avvengono sotto lo stesso lock: l'operazione è idempotente e non colpisce mai un tavolo riassegnato.
- **Reset** (`ROSERPINA_AUTO_RESET` da drawdown, `reset_cycle`): il runtime tiene la mappa delle riserve incerte (`event_key` → `table_id`). Il reset passa da `_reset_reservations_keeping_uncertain`, che chiama `TableManager.reset_all(keep_table_ids=…)` e `DuplicationGuard.clear(keep_keys=…)`, quindi tavolo e chiave di un ordine AMBIGUOUS sopravvivono. Un esito certo successivo per lo stesso `event_key` toglie la marcatura.
- Il `betId` di una riga `orders` ha un **solo** estrattore (F-14): `core.order_identity.response_bet_id`, usato da
  `Database._order_bet_id` (identita' del bot per cashout e F-04) e da `RuntimeController._open_order_candidates`
  (riconciliazione all'avvio). Raccoglie il `betId`/`bet_id` diretto e quello di ogni `instructionReports` al livello
  top e sotto `result`/`raw`; gli ID numerici diventano stringhe. Un solo valore distinto => quello; nessuno, oppure
  piu' di uno (risposta a piu' istruzioni, `betId` diretto diverso dal report) => `""`: la riga non e' attribuita a
  nessun `betId` e resta alla riconciliazione. Prima i due estrattori avevano precedenze diverse (rilievo Claude C,
  04/11/2026).

## 2-bis. Cancel single-shot e report per istruzione (F-14)

`BetfairClient.cancel_orders` invia `cancelOrders` **una sola volta** (`single_shot=True`): un timeout non prova che il
cancel non sia avvenuto. L'esito e' nel campo `outcome`:

| Situazione | `outcome` | `ok` |
|---|---|---|
| `status` `SUCCESS` e un report `SUCCESS` per **ogni** `betId` richiesto (cancel di tutto il mercato: nessun report richiesto) | `SUCCESS` | sì |
| `status` `FAILURE`, oppure `PROCESSED_WITH_ERRORS` con tutti i report `FAILURE`; errore JSON-RPC; 4xx; sessione scaduta; richiesta non inviata | `FAILED` | no |
| Alcuni report `SUCCESS` e altri `FAILURE` | `PARTIAL` | no |
| Timeout, errore di rete, 5xx, risposta illeggibile, `status` assente o sconosciuto, report mancante o `TIMEOUT` per un `betId` richiesto | `AMBIGUOUS` | no |

`cancelled_count` e `cancelled_bet_ids` contano solo i report `SUCCESS`; `failed_bet_ids` i `FAILURE`; `bet_ids` i
richiesti (riconciliazione per `betId`). Un report senza `instruction.betId` e' attribuito per posizione solo se il
numero dei report coincide con quello delle istruzioni (Betfair li restituisce nello stesso ordine).
`BetfairClient.cancel_order` (cancel di un ghost) solleva `CANCEL_ORDER_UNCONFIRMED` per `AMBIGUOUS`/`PARTIAL` e
`CANCEL_ORDER_FAILED` per `FAILED`. `OrderManager.cancel_order`: esito `AMBIGUOUS` del client, o eccezione di
timeout/rete/5xx/risposta illeggibile => saga `AMBIGUOUS` (terminale: nessuna transizione senza prova, #495) e
`QUICK_BET_CANCEL_AMBIGUOUS`; mai `FAILED`. Riserva e tavolo restano acquisiti finche' il cancel non e' confermato
(PF-EXEC-01). Test: `tests/acceptance/test_f14_cancel_single_shot.py`, `tests/acceptance/test_betid_extractor_unico.py`.

## 3. Fuori da questo contratto (residui tracciati in #453)

- F2 / OWNER-SETTLEMENT-02: rilascio del tavolo al settlement.
- F004 / F018: recovery e ricostruzione dei tavoli al riavvio. La mappa delle riserve incerte è in memoria, quindi dopo un riavvio non esiste.
- F026: il bankroll sincronizzato dal saldo **disponibile** dell'exchange tratta le puntate aperte come perdita. Ne seguono un drawdown apparente e un `ROSERPINA_AUTO_RESET` che libera i tavoli delle posizioni vive (non incerte) e azzera il realizzato del ciclo.
- F017: poller del settlement disattivato di default.
- La riconciliazione degli `AMBIGUOUS` (PR30): fino ad allora un tavolo `AMBIGUOUS` resta occupato. È la scelta prudente richiesta dall'owner: conservare il rischio fino a prova contraria.
