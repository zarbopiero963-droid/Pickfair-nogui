# Recupero all'avvio: ordini "ghost" e outbox (F-08, #586 RST-02/NF-10/DB-04)

All'avvio `StateRecovery.recover()` chiede all'exchange gli ordini attivi (`fetch_startup_active_orders`) e cerca
quelli senza riga locale ("ghost"): ogni ghost va **persistito** in `startup_orders`, **seminato** nel
`DuplicationGuard` (nessun secondo ordine sullo stesso intento) e annunciato con `STARTUP_GHOST_ORDER_RECOVERED`.

## Cosa cambia con F-08

- **Schema.** `startup_orders(order_id PK, payload, created_at)` e `outbox(id, batch_id, event_name, payload,
  created_at)` sono in `database_schema.SCHEMA_DDL`. Prima non venivano mai create: `startup_order_exists`
  sollevava `OperationalError` e ogni ghost era perso.
- **Fail-closed.** Un errore del DB nel lookup o nella persistenza di un ghost NON vale piu' "gia' presente":
  `recover()` restituisce `ok=False`, `reason=STARTUP_RECONCILIATION_REQUIRED`, `startup_mode=BLOCKED`.
  Prima restituiva `ok=True` (`LIVE_FLAT`) con l'ordine attivo non riconciliato.
- **Riavvio.** Un ghost gia' persistito a un avvio precedente e ancora attivo sull'exchange viene **riseminato**
  nel `DuplicationGuard` a ogni avvio (il runtime ne crea uno nuovo a ogni START), senza un secondo evento
  `STARTUP_GHOST_ORDER_RECOVERED` ne' una seconda riga: `ghost_orders_recovered` conta i ghost nuovi,
  `ghost_orders_active` quelli ancora vivi, e `startup_mode` resta `LIVE_WITH_RECOVERED_GHOSTS` finche' ce ne sono.
- **Gating (F-09).** F-08 rende l'esito veritiero, F-09 lo applica: vedi sotto.

## Esito della fase recovery/reconcile e avvio (F-09, #586 RST-01/REC-08)

`RuntimeController.start()` non si limita piu' a loggare l'esito della fase recovery/reconcile: lo applica con la
stessa semantica della ricostruzione F018 (`_gate_rebuild_on_startup_recovery`). Il runtime parte in **PAUSED** con
`last_error = RECONCILIATION_REQUIRED:<motivo>` e nessun nuovo ingresso se:

| Caso | Motivo nel payload `STARTUP_POSITIONS_REBUILT` |
|---|---|
| `recover()` restituisce `ok` diverso da `True` (o non un dict) | `STATE_RECOVERY_NOT_OK:<reason>` |
| `recover()` o la riconciliazione dei batch sollevano | `STATE_RECOVERY_ERROR:<tipo eccezione>` |
| batch aperti non riconciliati: eccezioni (`failed_count`) **piu'** singoli esiti `ok` diverso da `True` in `reconciled` (`reconcile_batch` non solleva su fetch fallito, `RECONCILE_ALREADY_RUNNING`, `BATCH_NOT_FOUND`...) | `OPEN_BATCH_RECONCILIATION_FAILED:<numero>` |

Il payload riporta sempre `startup_mode` di `recover()` (`LIVE_FLAT`, `LIVE_WITH_RECOVERED_GHOSTS`, `BLOCKED`).
`resume()` **non** scavalca il blocco: `recover()` ripubblica le saghe pendenti (`RECOVER_ORDER`), quindi non viene
ritentato in un resume; serve un nuovo avvio con recovery riuscito (come `RECOVERY_STATE_UNREADABLE`). Se piu' gate
falliscono, il motivo e' quello del primo nella catena (F018/microstake, poi F-09, poi RSR-12, poi held).
Test: `tests/f09/test_f09_recover_gating.py` (HeadlessApp reale in SIM tramite l'harness F026); mutazioni `f09_*`.

## Nota di migrazione e compatibilita'

- DDL **additiva** e **idempotente** (`CREATE TABLE IF NOT EXISTS`): un DB esistente acquisisce le due tabelle alla
  prima apertura, senza toccare tabelle o colonne esistenti; nessun drop, nessun riuso di colonne. Rollback: le
  tabelle nuove si possono ignorare (il codice precedente non le legge con successo).
- Nessun dato da migrare: le tabelle partono vuote; i ghost vengono scritti al primo avvio con ordini attivi.
- Test: `tests/f08/test_f08_startup_orders_real_db.py` (DB SQLite reale, migrazione di un DB senza tabelle senza
  perdita di ordini); mutazioni `f08_*` in `guardrails/mutation_catalog.json`.

## Crash fra il settlement nel broker e la sua contabilizzazione (F-10, #586 RST-03)

Scenario provato sul runtime vero (`tests/recovery/test_f10_rst03_crash_debt.py`, HeadlessApp in SIM con SQLite
reale): il mercato e' gia' CLOSED (LOSS) nel broker quando il processo muore, prima che il runtime riceva
`RUNTIME_CLOSE_POSITION`. Al riavvio la ricostruzione F018 vede l'ordine chiuso (assente dagli ordini correnti) e non
ricrea il tavolo; il settlement SIM per mercato, riemesso dal poller, non trovava piu' alcun tavolo sul mercato e
restava `held` per sempre: **la perdita usciva dal ciclo di recovery** (debito 0 invece della stake persa).

Ora `_resolve_market_settlement`, se nessun tavolo porta il mercato, ricava l'identita' dall'**unica** riga `orders`
del bot su quel mercato (`_resolve_market_settlement_from_order`): `customer_ref`, `event_key` e il tavolo della
riserva persistiti. Il rilascio passa da `release_identified_reservation` come nel settlement per bet LIVE: tavolo
libero => il P&L entra nel suo recovery; tavolo con un'altra riserva => la riserva resta e il P&L entra nel recovery.
Resta `held` (nessun effetto) se il mercato ha hedge/cashout o il registro hedge e' inaffidabile, piu' righe del
bot sul mercato, una gamba di lotto Dutching, identita' o tavolo non persistiti, tavolo inesistente o ordini
illeggibili (questi ultimi due sono transitori, riconciliabili). Il registro `settlement_effects` garantisce un solo
effetto anche per un secondo settlement dello stesso mercato. Mutazioni `f10_*`.


## Picco dell'equity persistente (F-11, #586 DD-03/DD-04)

Decisioni owner: opzione A per la migrazione pre-v9 (#426 c.6145958246) e per la ricostruzione del picco
(#426 c.6146717266).

- **Cosa si persiste.** Il picco del RiskDesk (`equity_peak`, base dello stop sul drawdown % e assoluto e del lockdown
  del MM) e il picco di cassa (`_cash_equity_peak`, base dell'auto-reset Roserpina). Chiave impostazioni
  `risk.equity_peaks_state`, JSON `{"equity_peak", "cash_equity_peak", "set_at"}`. Si scrive a ogni nuovo picco letto
  dal conto e nella stessa transazione dello stato tavoli (settlement, reset di ciclo: il ribasamento del picco di
  cassa dell'auto-reset viaggia con il marker del reset).
- **Avvio.** `RuntimeController._restore_equity_peaks` gira prima della prima lettura del conto: il picco ripristinato
  non viene mai abbassato, quindi una perdita resta perdita anche dopo il riavvio (picco 1000, equity 900 ⇒ drawdown
  10 %). Prima di questa correzione il picco ripartiva dal saldo corrente a ogni riavvio.
- **Picco non dimostrabile ⇒ nessun nuovo rischio.** Chiave illeggibile (`EQUITY_PEAKS_UNREADABLE`), corrotta
  (`EQUITY_PEAKS_CORRUPT`: non JSON, non finita, ≤ 0, booleana) o assente su un'installazione che ha già operato
  (`EQUITY_PEAKS_MISSING_WITH_HISTORY`: stato Roserpina persistito o ordini nel DB) ⇒ avvio in PAUSED con
  `RECONCILIATION_REQUIRED:<motivo>`, come per RSR-12. `resume()` rilegge la chiave e non scavalca il blocco. Nessuna
  scrittura del picco finché il blocco è attivo: l'evidenza resta intatta e non c'è sblocco automatico.
- **Installazione nuova** (nessuna chiave Roserpina, nessun ordine): nessun blocco, il primo picco è la prima lettura
  verificata del conto.

**Nota di migrazione.** Nessuna modifica allo schema del DB: una chiave nuova nella tabella delle impostazioni.
Un'installazione esistente aggiornata a questa versione **parte in PAUSED** (`EQUITY_PEAKS_MISSING_WITH_HISTORY`)
finché non viene scritto un picco valido. Per decisione owner (opzione A) quel valore va ricostruito dallo storico
Betfair in sola lettura e ratificato dall'owner: lo strumento di ricostruzione/ratifica non fa parte di questa
correzione (BLOCKED, decisione owner). Una installazione SIM di prova può ripartire da un DB nuovo.
