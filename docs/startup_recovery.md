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
| `reconcile_all_open_batches()` con `ok` diverso da `True` | `OPEN_BATCH_RECONCILIATION_FAILED:<failed_count>` |

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
