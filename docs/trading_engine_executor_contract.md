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
| `placeOrders` LIVE: rifiuto **dichiarato** da Betfair: `status` `FAILURE` **senza report o con tutti i report `FAILURE`**, `PROCESSED_WITH_ERRORS` **con tutti i report `FAILURE`** (almeno uno), `status` `SUCCESS` con report `FAILURE`, elemento JSON-RPC `error` di rifiuto (le eccezioni APING `TIMEOUT_ERROR`/`UNEXPECTED_ERROR` sono invece esito ignoto, `order_unknown`, come nel cancel: rilievo di Sol su #609) | `FAILED` | sì |
| `placeOrders`: `status` `FAILURE`/`PROCESSED_WITH_ERRORS` con **almeno un report non `FAILURE`** (`SUCCESS` con `betId`, `TIMEOUT`, senza status, voce illeggibile), oppure `PROCESSED_WITH_ERRORS` **senza report** (rilievo di Sol su #609: non prova il fallimento dell'istruzione). Con `status` `SUCCESS` i report si valutano tutti prima di decidere: report discordanti (`FAILURE` e `SUCCESS` con `betId`) o un `TIMEOUT` in qualsiasi posizione sono esito ignoto (`BET_OUTCOME_MIXED`), solo tutti `FAILURE` e' un rifiuto. Anche `instructionReports` presente ma non lista, o un report non oggetto con `status` `SUCCESS`, e' esito ignoto (`BET_REPORT_STATUS_UNRECOGNIZED`); nel corpo grezzo l'engine tratta come AMBIGUOUS anche `status` `TIMEOUT` o un report `TIMEOUT` (`DOWNSTREAM_TIMEOUT_OUTCOME`). `BetfairClient` lo marca `order_unknown` (`BET_OUTCOME_MIXED`, risposta conservata); l'engine riconosce lo stesso caso anche nel corpo grezzo di un altro broker (`DOWNSTREAM_MIXED_OUTCOME`). Revisione Claude B del 05/11/2026 (P1-1): prima era FAILED e liberava tavolo, riserva e chiave mentre l'ordine poteva essere vivo | `AMBIGUOUS` + riconciliazione | **no** |
| Esito ignoto, ma la transizione DB `INFLIGHT→AMBIGUOUS` fallisce (#524) | `AMBIGUOUS` non persistito: `is_terminal=False`, `lifecycle_stage="degraded"`, `QUICK_BET_AMBIGUOUS`; la riga resta `INFLIGHT` su DB e dedupe e riconciliazione la ritrovano al riavvio. **Mai** `FAILED` | **no** |
| Eccezione dopo l'avvio dell'invio (transizione `SUBMITTED` fallita, crash nella costruzione dell'esito, finalize che solleva) (#524) | `AMBIGUOUS` + riconciliazione (`ENGINE_FATAL_AFTER_SEND`), anche con DB indisponibile. **Mai** `FAILED` | **no** |
| Eccezione sollevata dal client | propagata **invariata**: stessa classificazione del percorso diretto | secondo la classificazione |

- **Report conservato di una riga incerta (vincolo MCP-01k, 05/11/2026).** Su una riga `AMBIGUOUS`, `INFLIGHT`,
  `HEDGE_AMBIGUOUS` o `HEDGE_PENDING` il `response_json` conservato **non e' un dato confermato**: serve solo come
  identita' (`betId`, tramite `core.order_identity.response_bet_id`). Nessun consumatore tratta i suoi importi
  (`sizeMatched`, `averagePriceMatched`, `sizeRemaining`) come eseguiti: le quantita' abbinate si leggono solo
  dall'exchange (`listCurrentOrders`/settlement) dopo la riconciliazione. Lettori del `response_json` di `orders` nel
  core, tutti **solo identita'**: `Database._order_bet_id` (via `_live_bot_orders`/`get_bot_active_orders`),
  `Database.find_orders_by_bet_id`, `Database.get_order` (restituisce la risposta grezza, nessun calcolo),
  `RuntimeController._open_order_candidates`, `CashoutExecutor._hedge_rows`/`_hedge_block` (solo il `bet_id` dell'hedge;
  quantita' e residuo dagli ordini correnti dell'exchange). Il candidato di riconciliazione non porta importi abbinati
  (test `test_riga_incerta_non_espone_importi_del_report`). Il `matched` che `OrderRouter._normalize_live` mette su un
  esito `AMBIGUOUS` (`order_unknown`) viene dai report ed e' **non confermato**: l'esito porta
  `matched_confirmed: false` e quel valore finisce solo in audit/notifica, mai in un calcolo di esposizione.
  Prova end-to-end (runtime reale, restart e settlement): `tests/f026/test_f14_matched_non_confermato.py`, report
  diagnostico falso (`sizeMatched` 999, `averagePriceMatched` 9.9) e tre esiti confermati dall'exchange:
  - **pieno** (tutto lo stake abbinato) e **parziale** (1 EUR abbinato): prima della riconciliazione P&L 0, esposizione
    = riserva della richiesta, bankroll pre-ordine, disponibile = 100 − stake dall'exchange; dopo il riavvio la
    ricostruzione F018 usa lo stake della richiesta e il bankroll e' il disponibile letto all'avvio; al settlement il
    realizzato = abbinato confermato × (prezzo medio confermato − 1) × (1 − commissione), riserva rilasciata, bankroll =
    disponibile post-settlement;
  - **nessuno** (nessun abbinamento, l'ordine decade): P&L 0; la riserva resta trattenuta senza prova (un AMBIGUOUS non
    si libera "come annullato", F028_04) finche' la riconciliazione non la chiude; il bankroll resta l'ultima lettura
    (100 − stake), mai gonfiato.
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
| `status` `FAILURE` senza report `SUCCESS`/ignoti, oppure `PROCESSED_WITH_ERRORS` con report presenti e tutti `FAILURE`; errore JSON-RPC di rifiuto; 4xx; sessione scaduta; richiesta non inviata; eccezione APING di rifiuto (`INVALID_INPUT_DATA`, `TOO_MANY_REQUESTS`, `SERVICE_BUSY`, `ACCESS_DENIED`, …) | `FAILED` | no |
| `PROCESSED_WITH_ERRORS` con alcuni report `SUCCESS` e altri `FAILURE`, ognuno sul suo `betId` | `PARTIAL` | no |
| Timeout, errore di rete, 5xx, risposta illeggibile, `status` assente o sconosciuto, report mancante per un `betId` richiesto, report `TIMEOUT`, con stato ignoto o illeggibile (non oggetto, o `instructionReports` non lista), `PROCESSED_WITH_ERRORS` senza report (anche nel cancel di tutto il mercato, senza `betId` richiesti); eccezione APING `TIMEOUT_ERROR` o `UNEXPECTED_ERROR`; `status` in conflitto con i report (`FAILURE` con un `SUCCESS`, `PROCESSED_WITH_ERRORS` senza errori, `SUCCESS` con un `FAILURE`, rilievo di Grok su #609) | `AMBIGUOUS` | no |

`cancelled_count` e `cancelled_bet_ids` contano solo i report `SUCCESS`; `failed_bet_ids` i `FAILURE`; `bet_ids` i
richiesti (riconciliazione per `betId`). Un report senza `instruction.betId` e' attribuito per posizione solo se il
numero dei report coincide con quello delle istruzioni (Betfair li restituisce nello stesso ordine); nel conteggio
entrano anche i report illeggibili, che restano esito ignoto (rilievo di Sol su #609).
`BetfairClient.cancel_order` (cancel di un ghost) solleva `CANCEL_ORDER_UNCONFIRMED` per `AMBIGUOUS`/`PARTIAL` e
`CANCEL_ORDER_FAILED` per `FAILED`. `OrderManager.cancel_order`: esito `AMBIGUOUS` o `PARTIAL` del client (solo `FAILED` e' un rifiuto certo), o eccezione di
timeout/rete/5xx/risposta illeggibile (riconosciuta prima dal tipo: `TimeoutError`, `ConnectionError`, eccezioni
`requests`, anche senza messaggio; poi dal testo, compreso `CANCEL_ORDER_UNCONFIRMED`) => saga `AMBIGUOUS` (terminale: nessuna transizione senza prova, #495) e
`QUICK_BET_CANCEL_AMBIGUOUS`; mai `FAILED`. Riserva e tavolo restano acquisiti finche' il cancel non e' confermato
(PF-EXEC-01). Test: `tests/acceptance/test_f14_cancel_single_shot.py`, `tests/acceptance/test_betid_extractor_unico.py`.

## 3. Fuori da questo contratto (residui tracciati in #453)

- F2 / OWNER-SETTLEMENT-02: rilascio del tavolo al settlement.
- F004 / F018: recovery e ricostruzione dei tavoli al riavvio. La mappa delle riserve incerte è in memoria, quindi dopo un riavvio non esiste.
- F026: il bankroll sincronizzato dal saldo **disponibile** dell'exchange tratta le puntate aperte come perdita. Ne seguono un drawdown apparente e un `ROSERPINA_AUTO_RESET` che libera i tavoli delle posizioni vive (non incerte) e azzera il realizzato del ciclo.
- F017: poller del settlement disattivato di default.
- La riconciliazione degli `AMBIGUOUS` (PR30): fino ad allora un tavolo `AMBIGUOUS` resta occupato. È la scelta prudente richiesta dall'owner: conservare il rischio fino a prova contraria.
