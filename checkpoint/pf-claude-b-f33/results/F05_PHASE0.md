# F-05 — Phase 0 (read-only, `main@6fa29b2`, worktree `wt-f05`, nessuna modifica al Core)

Fonte: mappatura dei consumer della tabella `orders` (agente di esplorazione, 31/10/2026) + lettura diretta di
`core/order_router.py`, `cashout_executor.py`, `database.py`. Paths: `rc` = `core/runtime_controller.py`.

## 1. Percorso dell'hedge oggi
`CashoutRouter` → `REQ_EXECUTE_CASHOUT` → bridge → `CMD_EXECUTE_CASHOUT` → `CashoutExecutor.on_cmd_execute_cashout`
→ `_build_place_payload` (`customer_ref = payload["source"]`, es. "TELEGRAM", cashout_executor.py:184; `event_key` "")
→ `OrderRouter.place` (core/order_router.py:74-113; sotto 1 EUR → Microstake nativo) → broker.
**Nessuna scrittura in `orders` né in `order_saga`.** Il `customer_ref` non univoco arriva a Betfair come `customerRef`
(dedup Betfair 60 s). In SIM il broker scrive comunque ogni bet in `simulation_bets` (simulation_broker.py:2077-2082).

## 2. Scrittori di `orders`
`database.insert_order` (1660) / `update_order` (1696): solo `core/trading_engine.py` (~2960-3020) per gli ingressi.

## 3. Lettori di `orders` e effetto di una riga hedge (stesso mercato, lato opposto)
| Lettore | Uso | Effetto di una riga hedge con la forma di un ingresso |
|---|---|---|
| `get_order`, `order_exists_inflight`, `find_duplicate_order`, `load_pending_*` (engine dedup) | per id/ref | nessuno con ref univoco `hedge:` |
| `find_orders_by_bet_id` → `rc._resolve_settlement_reservation` (rc:5692) | prenotazione al settlement | esce prima per mercati hedgiati (rc:5690); altrimenti hold (safe) |
| `get_bot_active_orders` = `_sim_bot_orders` + `_live_bot_orders` (database.py:2322-2412) | identità del bot per bet_id, nessun filtro di lato/origine | l'hedge diventa identità (VOLUTO, vedi §4) |
| `rc._open_order_candidates` (5101-5166) → **F018 `_rebuild_open_positions`** (5295-5376) | ricostruzione tavoli al riavvio | **RISCHIO**: senza `event_key` → `UNATTRIBUTED_PENDING_ORDERS` → avvio PAUSED; con `event_key` → **tavolo fantasma** con liability LAY |
| `rc._release_final_unmatched_orders` F028 (5204-5275) | rilascio residui non abbinati | rifiutato da `force_unlock_if_event` (customer_ref), fragile |
| `rc._resolve_market_settlement` SIM (5728-5766) | rilascio tavolo se UNA sola riga sul mercato | **REGRESSIONE**: 2 righe ⇒ tavolo mai rilasciato dopo un cashout SIM (rompe F026_10/F026_12) |
| `rc._single_bet_position` (5911-5950) | conta righe per mercato | blocco nuovo con `per_bet=False` |
| `rc._market_has_foreign_cleared_legs` (5872-5909) | gamba estranea blocca il rilascio | oggi l'hedge è «estraneo»; dopo non più: resta coperto dal registro dei mercati hedgiati |
| observability (`get_recent_orders_for_diagnostics`) | conteggi | rumore diagnostico |
Nessun lettore filtra per `order_origin`/`role`; unico filtro per prefisso: `dut-` (rc:4105). `ORIGIN_*` solo audit.

## 4. P&L, daily-loss, settlement
- **LIVE (rischio di denaro attuale):** `bet_to_market` del poller (rc:810-820) viene solo da `get_bot_active_orders` ⇒ la
  bet dell'hedge **non è interrogata** in `listClearedOrders` (rc:839) e verrebbe scartata (rc:886-898) ⇒ **P&L
  dell'hedge assente da realizzato e daily-loss in LIVE** (perdita o vincita sovrastimata). Con l'hedge persistito entra via
  `apply_cleared_market_settlement(settlement_ref=bet_id)`, idempotente per (mercato, bet) e con netto di mercato
  ricalcolato (core/pnl_engine.py:495-513).
- **SIM:** già contabilizzato (settlement per mercato su tutte le bet del broker; `_poll_simulated_settlements_locked`).
- **Secondo cashout LIVE oggi:** l'hedge non è identità ⇒ il router vede ancora l'ingresso EXECUTION_COMPLETE come posizione a
  un lato e **lo copre di nuovo**; argini solo in memoria (dedup bridge 2 s, barriera). Dopo la persistenza la selezione diventa
  MIXED e viene saltata (cashout_resolver.py:121-126): niente doppio hedge, ma il residuo di un hedge parziale non viene più chiuso.

## 5. Crash e riavvio
- Kill fra `place` dell'hedge e la sua registrazione: oggi nessuna traccia locale (LIVE) ⇒ al riavvio F018 vede solo l'ingresso;
  il registro dei mercati hedgiati (`roserpina.hedged_markets`, rc:5768-5870, durevole, chiave mercato+timestamp) è l'unico segno.
- Con la persistenza: la riga va scritta **prima** di `CASHOUT_SUCCESS` e lo stato AMBIGUOUS va persistito come tale (mai retry).
- `order_saga`: **da non usare** per l'hedge (emergency cancel per mercato, stale-pending, ghost detection).

## 6. Invarianti che F-05 deve rispettare
1. Ogni hedge piazzato ha una riga nel ledger con `customer_ref = hedge:<market>:<selection>:<uuid>` univoco, lo stesso inviato
   come `customerRef`, scritta prima dell'esito pubblicato; bet_id registrato; AMBIGUOUS persistito.
2. La riga hedge **non** crea né ricostruisce tavoli (F018), non blocca il rilascio SIM per mercato, non entra in F028.
3. In LIVE la bet dell'hedge entra nell'identità del bot ⇒ P&L e daily-loss includono l'hedge una sola volta.
4. Un solo cashout attivo per posizione, durevole (non solo in memoria): nessun secondo hedge sulla stessa (mercato, selezione)
   finché il precedente non è finale.
5. Nessuna modifica alla matematica del green-up; nessun limite finanziario nuovo; Microstake invariato (owner-reserved).
6. Ledger Decimal che quadra dopo hedge parziale/totale; nessuna doppia contabilizzazione al settlement.

## 7. Punti di modifica minimi proposti (da verificare con test RED prima di scrivere)
1. `CashoutExecutor`: genera il ref `hedge:` una volta, lo passa a `place`, persiste con `db.insert_order` payload
   `{market_id, selection_id, bet_type, price, stake, role:"HEDGE", order_origin:"CASHOUT_HEDGE", cashout_request_id}` **senza**
   `event_key`/`table_id`/`batch_id`; stato fuori da SUBMITTED/AMBIGUOUS/INFLIGHT, oppure filtro esplicito (punto 2).
2. Predicato unico `is_hedge_order_ref(ref)` (prefisso `hedge:`) applicato in `_open_order_candidates`,
   `_resolve_market_settlement`, `_single_bet_position`, e nell'Oracle di tests/f026.
3. `_live_bot_orders` NON filtrato (fix daily-loss LIVE); decidere il comportamento del residuo di un hedge parziale (MIXED skip).
4. Registro dei mercati hedgiati invariato; check durevole «un cashout attivo per posizione» via `find_active_hedge` in
   `CashoutExecutor` prima di `place`, sotto l'authority operation.
5. Test RED: F018 rebuild con riga hedge (nessun tavolo, `ok`), rilascio SIM dopo hedge persistito (F026_10/12 verdi),
   poller LIVE che interroga la bet dell'hedge, router che salta la selezione MIXED, secondo comando cashout rifiutato in modo
   durevole dopo un riavvio. L'xfail strict `test_cashout_hedge_is_written_to_ledger` di F-33 deve diventare XPASS.

## 8. Stato
Analisi completa sui punti elencati. **Non verificato ancora con test eseguiti**: i RED del §7.5 non sono scritti.
F-05 è finanziaria critica ⇒ merge manuale owner; nessuna PR finché #589 è aperta.
