# Settlement poller — il ciclo di chiusura reale (PR `runtime_settlement_wiring`)

Prima di questa PR il settlement reale non aveva **nessuna via d'ingresso**: il
`core.pnl_engine.PnLEngine` (unico publisher di `RUNTIME_CLOSE_POSITION`) non
era mai istanziato, il PnL realizzato non arrivava mai e il monitor daily-loss
confrontava il limite con un valore fermo a zero. Questa pagina documenta il
filo nuovo e le sue regole operative.

## Il filo

```
listClearedOrders (SETTLED, group_by=MARKET, settled_after=now-lookback)
  → BetfairService.list_cleared_orders          (facade fail-closed LIVE)
  → RuntimeController._poll_cleared_settlements (dedupe durevole + in-memory)
  → PnLEngine.apply_cleared_market_settlement   (commissione market-net policy)
  → RUNTIME_CLOSE_POSITION                      (payload canonico "accepted")
  → RuntimeController._on_close_position        (contratto → realized PnL →
                                                 daily-loss → bankroll sync →
                                                 hard-stop se il limite sfonda)
```

- Il `PnLEngine` e' costruito in `RuntimeController.__init__` (entrambi gli
  entrypoint, headless e GUI) con la **commissione di policy**
  (`BETFAIR_ITALY_COMMISSION_PCT`): e' l'unica forma che il contratto
  settlement accetta. Il `profit` del report Betfair e' **GROSS**
  (pre-commissione); la commissione riportata da Betfair viaggia nel payload
  solo come osservabilita' (`betfair_reported_commission`). Il saldo VERO
  resta il bankroll sync post-settlement (`get_account_funds`).
- **Auto-close mark-to-market: DISARMATO di default**
  (`auto_close_enabled=False`). La chiusura a soglia non piazza ordini reali:
  realizzerebbe PnL contabile fantasma su una posizione ancora viva. Il
  guardrail di cablaggio asserisce che sull'app reale resti disarmato.

## Chiavi di configurazione (settings, pattern market-data)

| chiave | default | note |
|---|---|---|
| `settlement.poll_enabled` | `False` | **dormiente di default** (decisione owner, stesso precedente del TTL DIRECT B6.3.2b). Finche' e' OFF il filo esiste ma non trasporta dati. |
| `settlement.poll_sec` | `60` | intervallo del giro, clamp minimo 5s. |
| `settlement.lookback_hours` | `24` | finestra `settled_after`, clamp 1..168h. |

Qualsiasi errore di lettura/parse della config ⇒ poller **disabilitato**
(fail-closed). Le chiavi non sono ancora enumerate in `config_registry.py`
(superficie console/GUI): entreranno con la PR di GUI parity.

## Regole fail-closed (non negoziabili)

- **LIVE only**: in SIMULATION il poller non parte e non chiama nulla; la
  facade solleva `CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION` (la parity del
  broker simulato — `record_realized_settlement` e' gia' pronto ma senza
  consumer — arrivera' con una PR dedicata).
- **Runtime non ACTIVE** ⇒ giro no-op (vale anche dopo emergency/lockdown).
- **Fetch fallito ⇒ round abortito**: mai una lista vuota spacciata per
  «nessun settlement»; si ritenta al giro successivo.
- **Riga malformata scartata** (marketId/profit assenti o non numerici): il
  poller non inventa MAI un profit.
- **Dedupe a due livelli** sulla chiave deterministica
  `cleared:<market_id>` (la stessa `settlement_key` del consumer):
  1. in-memory per il processo vivo;
  2. **durevole al riavvio**: prima di emettere si interroga il cycle
     recovery state; se il checkpoint del consumer esiste, il settlement e'
     gia' stato consegnato e NON si ri-applica. Stato db illeggibile ⇒
     emissione sospesa senza marcare (ritentata quando il db risponde).
- Lifecycle: thread daemon avviato da `start()` (solo se abilitato), fermato
  da `stop()` con join; l'emergency stop lo rende inerte via gate ACTIVE.

## Come si abilita (owner)

1. Impostare `settlement.poll_enabled = true` nelle settings (DB).
2. Riavviare il runtime (la config e' letta allo start del poller).
3. Verifica: log `Settlement poller avviato (poll_sec=… lookback_hours=…)` e,
   al primo mercato settlato, `[PnL] Cleared settlement <market> …`.

## Test

- `tests/unit/test_pnl_engine_cleared_settlement.py` — motore: payload
  canonico accettato dal contratto VERO, market-net con esempio numerico,
  fail-closed senza stato parziale, flag auto-close in entrambe le direzioni.
- `tests/core/test_settlement_poller.py` — poller: emissione e parametri
  fetch, filo intero fino a breach+emergency stop, SIM/non-ACTIVE, fetch
  fallito, righe malformate, dedupe in-memory e durevole, db illeggibile,
  config fail-closed, lifecycle thread, facade fail-closed.
- `tests/guardrails/test_cablaggio_runtime.py` — voce promossa da GAP a
  CABLATO: `MARKET_BOOK_UPDATE` sottoscritto, PnLEngine reale sul bus
  dell'app per entrambi gli entrypoint, auto-close disarmato.
