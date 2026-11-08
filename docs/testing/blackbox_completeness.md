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
manomissione; segreto nel registro. Ogni anello tolto alla scrittura e' un test BLOCK con il suo codice; 15 mutanti
dell'oracolo (un controllo spento alla volta) sono tutti rilevati.

## Tipi di evento nuovi richiesti (assenti da `EVENT_CATEGORIES`)

`oracle.NEW_TYPES`: `RECONCILIATION_RESOLVED`, `SL_TRIGGERED`, `TP_TRIGGERED`, `TRAILING_TRIGGERED`, `SL_TP_CLOSE_*`
(ROUTED / NOT_EXECUTED / UNCERTAIN / REJECTED / SKIPPED), `PROTECTION_WAITING`, `PROTECTION_RESUMED`,
`PROTECTION_LATCH_RELEASED`, `FUNDS_RESERVED`, `FUNDS_RELEASED`, `BLACKBOX_DEGRADED`, `BLACKBOX_RECOVERED`,
`BLACKBOX_GAP`, `BLACKBOX_UNAVAILABLE`, `UNCLEAN_TERMINATION`. Il microstake non richiede un tipo nuovo:
`data.purpose = MICROSTAKE_STUB / ENTRY / HEDGE` sulla richiesta. Un test fallisce se uno di questi tipi entra in
`EVENT_CATEGORIES` senza aggiornare la lista.

## Punto aperto per l'architettura

Disco pieno: §2 dice «uscite protettive consentite» e anche «`*_REQUESTED` durevole prima di ogni invio». L'oracolo
accetta un'uscita senza richiesta solo se un `BLACKBOX_GAP` successivo ne dichiara il `customerOrderRef` (buffer
critico in memoria scaricato al ripristino). Da confermare in BB-1.
