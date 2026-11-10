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
   La sede è quella in cui il runtime operava prima dell'arresto; una provenienza ignota (`UNKNOWN`, marker ripristinato
   senza sede) vale **LIVE** (fail-closed).
2. **Candidati da tutte le fonti locali:** solo righe della stessa sede (`payload.simulation_mode`; sede non dichiarata
   = candidata); tabella `orders` (ogni stato tranne quelli che non possono avere un ordine
   aperto: CANCELLED, FAILED, REJECTED, SETTLED, ROLLED_BACK, LAPSED, VOIDED, EXPIRED, DUPLICATE_BLOCKED, NOT_ATTEMPTED,
   HEDGE_FAILED e DENIED pre-invio; quindi anche INFLIGHT, AMBIGUOUS, HEDGE_* e stati futuri), `order_saga` e lo store delle saghe del
   **Microstake nativo** della sede (`microstake.sagas.live` / `microstake.sagas`; ogni fase tranne FAILED e
   ROLLED_BACK: stub, riduzione e sostituto con il ref della saga). Una saga `foreign_ref` (ordine preesistente con lo
   stesso ref, origine non dimostrabile) non è mai un'identità del bot: come nel recovery, non si adotta né si
   cancella; se è aperto resta tra gli ordini non attribuibili. Per l'hedge nativo del cashout il riferimento
   sull'exchange è `response.raw.microstake_ref`, non la `customer_ref` della riga. Una fonte illeggibile è un
   errore dichiarato, mai «nessun ordine».
   `DENIED` (direttiva owner D-629-05) è chiuso solo se è un rifiuto **pre-invio dimostrato**: la macchina a
   stati del motore lo ammette solo da INFLIGHT (risk gate, prima di `_atomic_submit`), mai da SUBMITTED o
   AMBIGUOUS, e la riga non porta alcuna traccia di trasmissione (`betId`, `order_unknown`, `placed`, esito
   AMBIGUOUS/SUCCESS/UNKNOWN, ref Microstake). Con una traccia resta candidato: mai `VERIFIED` per assunzione.
3. **Fotografia dell'exchange prima** (`get_current_orders`, tutti i mercati) sulla sede in cui il runtime operava
   (LIVE: client Betfair; SIM: broker simulato).
4. **Cosa si cancella:** solo gli ordini **aperti del bot**, cioè quelli con `betId` o `customerOrderRef` noti nel
   ledger, cancellati **per `betId`**, in lotti di al massimo 60 per chiamata (limite Betfair di `cancelOrders`;
   un lotto respinto non ferma gli altri e rende l'esito `CANCEL_INCOMPLETE`). Un ordine aperto **non attribuibile**, anche sullo stesso mercato (ordine
   manuale, altro programma, ordine senza riferimento), non viene mai cancellato: è elencato in
   `unattributed_open_orders` e richiede l'operatore (rilievo Sol su `c8338ef3`). **Mai il cancel dell'intero
   mercato**, nemmeno se l'exchange non si può leggere (Sol su `7094b1a6`): in quel caso si cancellano per `betId`
   solo gli ordini del bot noti nel ledger, gli invii senza `betId` restano irrisolti e l'esito è `UNVERIFIED`.
5. **Cancel**, riverificando l'autorità del runtime (#536) prima di ogni mercato **e dopo ogni lotto**. Ogni lotto
   ha il suo record in `cancel_results[].batches` (`index`, `bet_ids`, `status`): `OK`, `FAILED` (rifiuto esplicito),
   `UNKNOWN` (eccezione, timeout o esito `AMBIGUOUS`: il cancel può essere avvenuto) o `NOT_SENT`. Se l'autorità si
   perde con un lotto in volo, quel lotto **conserva il proprio stato di risposta** (successo confermato, errore
   confermato o `UNKNOWN`) ed è marcato `authority_lost_during`; i lotti e i mercati successivi non partono
   (`NOT_SENT`, `runtime_authority`) e l'esito non è mai `VERIFIED` (sonde owner B4/B5). La perdita dell'autorità è un **incidente distinto** (`authority_lost`, anche
   nell'evento sul bus), registrato anche quando la lettura finale mostra gli ordini cancellati: l'esito della
   richiesta (lotto `UNKNOWN`) resta separato dallo stato effettivo degli ordini, deciso dalla lettura finale
   (direttiva owner `PF-OWNER-ESTOP-B5-BATCH-AUDIT-20261110`). Nel client LIVE la
   cancellazione e la lettura di verifica dell'emergenza attraversano il circuit breaker (`emergency=True`): un breaker
   aperto da timeout di lettura (AUD-BF-010) non blocca più l'uscita protettiva. Fuori dall'emergenza il breaker è
   rispettato come prima. Sul broker SIM un cancel riuscito in memoria ma con lo snapshot di reload non scritto
   (`snapshot_persisted: false`) si ripara con `ensure_durable()`; se resta non persistito è un cancel incompleto
   (`snapshot_not_persisted`, esito `CANCEL_INCOMPLETE`), perché al riavvio l'ordine tornerebbe aperto.
6. **Fotografia dopo**, sempre e su tutti i mercati (un ordine accettato dopo la prima lettura compare; gli ordini non
   attribuibili si ricalcolano sullo stato finale), e **esito per ordine** (`order_outcomes`), deciso sullo **stato finale**
   dell'exchange (un abbinamento avvenuto durante il cancel resta un'esposizione): `CANCELLED`, `PARTIALLY_MATCHED`
   (residuo cancellato, parte abbinata resta), `ALREADY_MATCHED`, `NOT_FOUND`, `STILL_OPEN`, `UNKNOWN`. Per gli ordini
   senza `betId` (risposta persa): `SEE_BET_ID` se ritrovati per `customerOrderRef` (con `bet_id_by_ref`),
   `NO_OPEN_ORDER_ON_MARKET`, `NO_OPEN_ORDER_FOUND` (riga senza mercato, nessun ordine con il suo riferimento), oppure
   `UNKNOWN` se sul mercato c'è un ordine aperto non attribuibile. Le identità del bot si riconoscono anche nella
   fotografia finale: un ordine ritrovato solo dopo il cancel, già abbinato, entra in `matched_positions`.
7. **Verifica** (`verification`): `VERIFIED` solo se la lettura finale riesce e non resta nulla di aperto, nessun cancel
   è fallito, l'autorità non è stata persa e le fonti locali sono state lette. Altrimenti `OPEN_ORDERS_REMAIN`,
   `CANCEL_INCOMPLETE`, `UNVERIFIED` o `SOURCES_INCOMPLETE` (tabella `orders`, `order_saga` o store delle saghe
   Microstake illeggibile: gli ordini del bot non sono attribuibili, quindi un exchange vuoto non basta) o
   `SUBMISSIONS_UNRESOLVED`: un invio senza `betId` (INFLIGHT, AMBIGUOUS, saga pre-invio) non ritrovato per
   riferimento sull'exchange (ogni esito tranne `SEE_BET_ID`, anche `UNKNOWN` a exchange illeggibile; elencato in
   `unresolved_submissions`) può
   ancora essere accettato dopo l'ultima lettura, quindi «nessun ordine» non è dimostrato.
8. **Posizioni reali** abbinate prima dell'arresto: elencate in `matched_positions` (mercato, selezione, lato, importo,
   prezzo medio) qualunque sia l'esito del cancel o della verifica. Anche le righe chiuse (es. `CANCELLED` con il
   residuo annullato) restano identità del bot per le posizioni: la parte abbinata è esposizione reale. La fonte dei settlement resta quella precedente all'arresto (`_settlement_source_before_emergency`).
9. **Intervento manuale** (`manual_action_required`): verifica non `VERIFIED`, ordini non attribuibili o stato
   d'emergenza non salvato (`persist_error`: il blocco potrebbe non sopravvivere al riavvio). Log CRITICAL,
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
- `--emergency-stop` lanciato in un processo nuovo, senza una sessione LIVE autenticata, non può leggere né cancellare
  sull'exchange: l'esito è `UNVERIFIED` con rc 3 (onesto, mai un falso VERIFIED). L'arresto dalla GUI o dal runtime
  attivo usa la sessione già aperta (rilievo D-629-03 di Claude D).

## Test

`tests/integration/test_p0_emergency_stop_open_orders.py` (HeadlessApp reale, SQLite reale, exchange simulato) e
mutanti `p0_estop_*` in `guardrails/mutation_catalog.json`.
