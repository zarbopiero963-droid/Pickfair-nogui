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
- Il ledger estrae il `betId` dalle forme normalizzate top-level e dagli
  `instructionReports` top-level o annidati sotto `result`/`raw`; gli ID
  numerici vengono canonicalizzati a stringa per il confronto al riavvio.

## 3. Fuori da questo contratto (residui tracciati in #453)

- F2 / OWNER-SETTLEMENT-02: rilascio del tavolo al settlement.
- F004 / F018: recovery e ricostruzione dei tavoli al riavvio. La mappa delle riserve incerte è in memoria, quindi dopo un riavvio non esiste.
- F026: il bankroll sincronizzato dal saldo **disponibile** dell'exchange tratta le puntate aperte come perdita. Ne seguono un drawdown apparente e un `ROSERPINA_AUTO_RESET` che libera i tavoli delle posizioni vive (non incerte) e azzera il realizzato del ciclo.
- F017: poller del settlement disattivato di default.
- La riconciliazione degli `AMBIGUOUS` (PR30): fino ad allora un tavolo `AMBIGUOUS` resta occupato. È la scelta prudente richiesta dall'owner: conservare il rischio fino a prova contraria.
