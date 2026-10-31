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
- **Fuori scope (F-09).** Il gating del runtime sull'esito di `recover()` (PAUSED + RECONCILIATION_REQUIRED) e'
  la scheda F-09: F-08 rende l'esito veritiero, F-09 lo applica.

## Nota di migrazione e compatibilita'

- DDL **additiva** e **idempotente** (`CREATE TABLE IF NOT EXISTS`): un DB esistente acquisisce le due tabelle alla
  prima apertura, senza toccare tabelle o colonne esistenti; nessun drop, nessun riuso di colonne. Rollback: le
  tabelle nuove si possono ignorare (il codice precedente non le legge con successo).
- Nessun dato da migrare: le tabelle partono vuote; i ghost vengono scritti al primo avvio con ordini attivi.
- Test: `tests/f08/test_f08_startup_orders_real_db.py` (DB SQLite reale, migrazione di un DB senza tabelle senza
  perdita di ordini); mutazioni `f08_*` in `guardrails/mutation_catalog.json`.
