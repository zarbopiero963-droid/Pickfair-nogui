# Arresto d'emergenza (EMERGENCY STOP)

Decisione owner `PF-OWNER-P0-EMERGENCY-STOP-20261110`, difetto P0 `AUD-BF-016` (#627). Questo documento descrive ciò
che è **nel codice** (`RuntimeController.emergency_stop`, `_emergency_cancel_and_verify`).

## Il difetto corretto

L'arresto leggeva gli ordini da cancellare solo dalla tabella `order_saga`, scritta da `OrderManager`, che in
produzione non viene mai costruito. Il motore (`TradingEngine`), il cashout (`OrderRouter`) e il Microstake scrivono
nella tabella `orders`. Risultato: **zero cancellazioni** e arresto dichiarato riuscito, con gli ordini ancora aperti
sull'exchange.

## Cosa fa adesso

1. **Blocco immediato** (invariato): `_emergency_stopped`, LIVE disattivato, LOCKDOWN, stato persistito prima del
   cancel, runtime in SIM per i nuovi ingressi.
2. **Candidati da tutte le fonti locali:** tabella `orders` (ogni stato tranne quelli che non possono avere un ordine
   aperto: CANCELLED, FAILED, REJECTED, SETTLED, ROLLED_BACK, LAPSED, VOIDED, EXPIRED, DUPLICATE_BLOCKED, NOT_ATTEMPTED,
   HEDGE_FAILED; quindi anche INFLIGHT, AMBIGUOUS, HEDGE_* e stati futuri) e `order_saga`. Una fonte illeggibile è un
   errore dichiarato, mai «nessun ordine».
3. **Fotografia dell'exchange prima** (`get_current_orders`, tutti i mercati) sulla sede in cui il runtime operava
   (LIVE: client Betfair; SIM: broker simulato).
4. **Mercati da cancellare:** mercati dei candidati locali più mercati di ordini **aperti** sull'exchange attribuibili
   al bot (`betId` o `customerOrderRef` noti nel ledger). Un ordine aperto **non attribuibile** non viene cancellato
   alla cieca: è elencato in `unattributed_open_orders` e richiede l'operatore.
5. **Cancel per mercato**, riverificando l'autorità del runtime (#536) prima di ognuno. Nel client LIVE la
   cancellazione e la lettura di verifica dell'emergenza attraversano il circuit breaker (`emergency=True`): un breaker
   aperto da timeout di lettura (AUD-BF-010) non blocca più l'uscita protettiva. Fuori dall'emergenza il breaker è
   rispettato come prima.
6. **Fotografia dopo** sui mercati tentati e **esito per ordine** (`order_outcomes`):
   `CANCELLED`, `PARTIALLY_MATCHED` (residuo cancellato, parte abbinata resta), `ALREADY_MATCHED`, `NOT_FOUND`,
   `STILL_OPEN`, `UNKNOWN`; per gli ordini senza `betId` (risposta persa): `NO_OPEN_ORDER_ON_MARKET` o
   `STILL_OPEN`/`UNKNOWN`.
7. **Verifica** (`verification`): `VERIFIED` solo se la lettura finale riesce e non resta nulla di aperto, nessun cancel
   è fallito e l'autorità non è stata persa. Altrimenti `OPEN_ORDERS_REMAIN`, `CANCEL_INCOMPLETE` o `UNVERIFIED`.
8. **Posizioni reali** abbinate prima dell'arresto: elencate in `matched_positions` (mercato, selezione, lato, importo,
   prezzo medio). La fonte dei settlement resta quella precedente all'arresto (`_settlement_source_before_emergency`).
9. **Intervento manuale** (`manual_action_required`): verifica non `VERIFIED` o ordini non attribuibili. Log CRITICAL,
   evento `EMERGENCY_STOP_MANUAL_ACTION_REQUIRED` sul bus e, nella GUI, il dialogo
   «EMERGENCY STOP: intervento manuale richiesto».

## Esito di `--emergency-stop` (CLI)

- RESULT nella scatola nera: `VERIFIED` solo con `verification = VERIFIED` e nessun ordine non attribuibile;
  ordini ancora aperti o cancel incompleto → `FAILED`; verifica mancante o non riuscita → `UNKNOWN`.
- Codice di uscita: **0 solo se VERIFIED**, altrimenti **3** (arresto eseguito e trading bloccato, ma serve
  l'operatore).
- Idempotente: ripeterlo (anche dopo un riavvio) non duplica nulla; un ordine già cancellato risulta `NOT_FOUND`.

## Limiti dichiarati

- Dopo l'arresto il runtime è in SIM: il cashout delle posizioni reali rimaste (AUD-BF-020) non passa da qui ed è un
  rilievo separato. Le posizioni sono comunque elencate nel risultato e nella notifica.
- Il caso in cui l'arresto non parte affatto (DB corrotto, #622) è un difetto distinto, non coperto da questa
  correzione.

## Test

`tests/integration/test_p0_emergency_stop_open_orders.py` (HeadlessApp reale, SQLite reale, exchange simulato) e
mutanti `p0_estop_*` in `guardrails/mutation_catalog.json`.
