# F-05 — consumer della tabella `orders` (base main 6fa29b2) e comportamento con le righe hedge

Righe hedge: `customer_ref` = `hedge:` + 26 hex; stati `HEDGE_PENDING/PLACED/UNMATCHED/FAILED/AMBIGUOUS`;
payload `role=HEDGE`, `order_origin=CASHOUT_HEDGE`, senza `event_key`/`table_id`/`batch_id`.

| Consumer | Query/filtro | Vede l'hedge? | Effetto voluto |
|---|---|---|---|
| `Database.order_exists_inflight` (dedupe engine) | status IN (INFLIGHT, SUBMITTED) | no | invariato |
| `Database.load_pending_customer_refs/correlation_ids` (engine, avvio) | IN (INFLIGHT, SUBMITTED) | no | invariato |
| `Database.find_duplicate_order` | per customer_ref/correlation_id | solo col suo ref unico | nessuna collisione |
| `RuntimeController._open_order_candidates` (riconciliazione avvio / F018) | IN (SUBMITTED, AMBIGUOUS, INFLIGHT) | no | nessun tavolo fantasma, nessun PAUSED |
| `RuntimeController._resolve_market_settlement` (rilascio SIM per mercato) | NOT IN (FAILED, DENIED, DUP) | escluso da `is_hedge_order_ref` | F026_10/12 invariati |
| `RuntimeController._single_bet_position` | NOT IN (...) | escluso da `is_hedge_order_ref` | invariato (mercato hedgiato gia' escluso dal registro) |
| `RuntimeController._persisted_dutching_leg` | ref `dut-` | no | invariato |
| `Database.find_orders_by_bet_id` (settlement per bet) | per betId | si', riga hedge | senza event_key => nessuna attribuzione (come oggi: nessuna riga) |
| `Database._live_bot_orders` -> `get_bot_active_orders` (identita' I1) | status != INFLIGHT | **si'** | daily-loss/settlement poll LIVE vedono la bet; router: posizione MISTA => nessun secondo hedge |
| `_market_has_foreign_cleared_legs` | identita' I1 | si' (non piu' "estranea") | rilascio per bet comunque bloccato dal registro mercati hedgiati |

Rischi residui dichiarati:
- riga `HEDGE_PENDING` dopo un crash fra write-ahead e invio: traccia senza betId; nessuna riconciliazione automatica
  dell'hedge all'avvio (come oggi, ma ora tracciato). Follow-up: riconciliare `HEDGE_PENDING` contro currentOrders.
- `customer_ref` inviato al broker cambia da `source` del comando al ref `hedge:` unico (dedupe Betfair per richiesta).
