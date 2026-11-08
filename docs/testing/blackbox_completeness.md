# Scatola nera: oracolo di completezza finanziaria (PF-BLACKBOX-01, parte di Claude D)

Architettura e contratto: `docs/roadmap/pf_blackbox_01.md` (branch `pf-gov-persistent-roadmap-01` @ `cb2f04bb`), §3
record, §4 scrittura, §4-bis lettura. Questo pacchetto e' **offline**: gira contro una scatola nera finta che segue il
contratto (`tests/blackbox_completeness/fake_blackbox.py`). Quando BB-1/BB-3/BB-4 saranno su main, gli stessi controlli
leggeranno la scatola nera vera tramite `core.blackbox_query` e gireranno sugli scenari f026/G7 del runtime reale.

## Catena obbligatoria per ogni ordine (`oracle.check`)

1. `ORDER_PLACE_REQUEST`: ATTEMPT, `critical=True` (durevole PRIMA dell'invio);
2. esito: `ORDER_PLACE_RESPONSE` (RESULT, VERIFIED / REJECTED / FAILED / UNKNOWN) con causation alla richiesta;
3. abbinamento: `ORDER_MATCHED` con `values_after.size_matched` uguale al `sizeMatched` dell'exchange;
4. hedge: l'ordine `hedge:*` risale per causation a `CASHOUT_REQUEST`, che ha un esito;
5. riconciliazione: un esito UNKNOWN, o una richiesta senza esito (crash dopo l'invio), ha `RECONCILIATION_RESOLVED`;
6. P&L: `SETTLEMENT` per mercato con `values_after.net_pnl` uguale al netto dell'exchange.

Controlli strutturali: `seq` contiguo e unico, catena di hash, causation verso eventi precedenti, ogni scatto
SL/TP/trailing con un esito `SL_TP_CLOSE_*`, nessuna conferma senza ordine reale, nessun segreto nei record, ordini
usciti durante un guasto della scatola nera coperti da un `BLACKBOX_GAP` dichiarato. Nessun anello viene dedotto.

La verita' dell'exchange e' lo **storico completo** degli ordini (anche annullati, es. lo stub del microstake) e i
netti dei settlement.

## Scenari (`test_completeness_oracle.py`)

Campagna con tre ingressi microstake (stub + ordine esatto), un parziale con residuo annullato, SL / TP / trailing con
hedge, settlement; crash fra invio ed esito (riconciliazione per `customerOrderRef` dopo il riavvio); esito ignoto senza
riconciliazione; mercato sospeso e riaperto (protezione in attesa, nessun hedge a mercato sospeso); concorrenza
multitavolo (8 thread, `seq` senza buchi ne' duplicati); disco pieno (nessun nuovo ingresso, uscita coperta da lacuna);
manomissione; segreto nel registro. Ogni anello tolto alla scrittura e' un test BLOCK con il suo codice; 21 mutanti
dell'oracolo (un controllo spento alla volta) sono tutti rilevati.

## Tipi di evento nuovi richiesti (assenti da `EVENT_CATEGORIES`)

`oracle.NEW_TYPES`: `RECONCILIATION_RESOLVED`, `SL_TRIGGERED`, `TP_TRIGGERED`, `TRAILING_TRIGGERED`, `SL_TP_CLOSE_*`
(ROUTED / NOT_EXECUTED / UNCERTAIN / REJECTED / SKIPPED), `PROTECTION_WAITING`, `PROTECTION_RESUMED`,
`PROTECTION_LATCH_RELEASED`, `FUNDS_RESERVED`, `FUNDS_RELEASED`, `BLACKBOX_DEGRADED`, `BLACKBOX_RECOVERED`,
`BLACKBOX_GAP`, `BLACKBOX_UNAVAILABLE`, `UNCLEAN_TERMINATION`. Il microstake non richiede un tipo nuovo:
`data.purpose = MICROSTAKE_STUB / ENTRY / HEDGE` sulla richiesta. Un test fallisce se uno di questi tipi entra in
`EVENT_CATEGORIES` senza aggiornare la lista.

## Disco pieno: semantica di BB-1 (decisione Claude B, pfab-b-20261108-blackbox-oracle-ack-001)

L'oracolo accetta questi due casi e nient'altro:

1. **Buffer recuperato.** A store non disponibile la richiesta di un'uscita protettiva (hedge, cashout, chiusura SL/TP)
   va nel buffer critico in memoria PRIMA dell'invio; i nuovi ingressi sono sempre rifiutati (`BLACKBOX_UNAVAILABLE`).
   Al ripristino il buffer si scarica con gli eventi originali (`ts_utc` originale, `data.buffered=true`, nuovo `seq`
   nell'ordine di scarico) e una `BLACKBOX_GAP` (outcome non UNKNOWN) con intervallo e `customerOrderRef`. Valida solo
   se la richiesta bufferizzata e' presente E una lacuna con riferimenti noti la copre (`BUFFERED_WITHOUT_GAP`);
   un ingresso bufferizzato e' `ENTRY_DURING_OUTAGE`.
2. **Buffer perso** (crash prima dello scarico). Al riavvio `UNCLEAN_TERMINATION` + `BLACKBOX_GAP` con outcome UNKNOWN
   (`data.from_ts` / `data.to_ts`, riferimenti ignoti); la riconciliazione registra gli ordini trovati come
   `RECONCILIATION_RESOLVED` con phase OBSERVATION. Un'uscita (`hedge:*`) senza richiesta e' valida solo se il suo
   `placedDate` cade nell'intervallo di una lacuna UNKNOWN E ha quella riconciliazione; un ingresso mai.

Una richiesta mancante fuori da questi due casi resta `MISSING_REQUEST`. La regola provvisoria precedente («uscita senza
richiesta se una lacuna successiva ne dichiara il ref») e' rifiutata da un test BLOCK.
