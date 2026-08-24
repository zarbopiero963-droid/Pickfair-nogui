# Settlement poller — il ciclo di chiusura reale (PR `runtime_settlement_wiring`)

Prima di questa PR il settlement reale non aveva **nessuna via d'ingresso**: il
`core.pnl_engine.PnLEngine` (unico publisher di `RUNTIME_CLOSE_POSITION`) non
era mai istanziato, il PnL realizzato non arrivava mai e il monitor daily-loss
confrontava il limite con un valore fermo a zero. Questa pagina documenta il
filo nuovo e le sue regole operative.

## Il filo

```
listClearedOrders (SETTLED, bet_ids del bot, righe per-bet)
  → BetfairService.list_cleared_orders          (facade fail-closed LIVE)
  → RuntimeController._poll_cleared_settlements (identita' per-bet + dedupe)
  → PnLEngine.apply_cleared_market_settlement   (per (mercato, betId);
                                                 commissione market-net policy)
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

- **Identita' del bot PER-BET (I1)** — hardening dal giro review di #440: il
  poll interroga SOLO i `betId` registrati dal bot
  (`db.get_bot_active_orders`, ledger a DENYLIST SIM+LIVE: le bet settlate
  RESTANO nel ledger — si escludono solo `CANCELLED` in SIM e `INFLIGHT` in
  LIVE — la stessa allowlist d'identita' del cashout routing), passati come
  `bet_ids` alla API (a blocchi da 1000) e ri-verificati client-side riga
  per riga (betId nell'allowlist E mercato coerente col ledger). Le
  scommesse MANUALI dell'account — anche sullo STESSO mercato del bot — non
  entrano MAI nel daily-loss: un profitto esterno maschererebbe le perdite
  del bot (fail-open sul kill-switch). Gli id del ledger SIM (`SIMBET-*`,
  non numerici) sono esclusi dal poll LIVE. Identita' illeggibile o vuota ⇒
  nessuna chiamata.
- **LIVE only**: in SIMULATION il poller non parte e non chiama nulla; la
  facade solleva `CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION` (la parity del
  broker simulato — `record_realized_settlement` e' gia' pronto ma senza
  consumer — arrivera' con una PR dedicata).
- **Runtime non ACTIVE** ⇒ giro no-op (vale anche dopo emergency/lockdown).
- **Primo giro di ogni generazione = sweep completo** (senza
  `settled_after`, orizzonte Betfair ~90 giorni): recupera i settlement
  maturati durante un downtime piu' lungo del lookback; il dedupe filtra il
  gia' consegnato. Lo sweep si dichiara fatto SOLO se fetch ED emissioni
  sono riusciti (una riga transitoriamente fallita si ritenta ancora a
  orizzonte completo) e vale per la SOLA generazione del thread che l'ha
  eseguito (un thread vecchio oltre il join non puo' bruciare lo sweep di
  quella nuova). Poi finestra mobile.
- **Fetch fallito ⇒ round abortito**: mai una lista vuota spacciata per
  «nessun settlement»; si ritenta al giro successivo.
- **Riga malformata scartata** (marketId/profit assenti o non numerici): il
  poller non inventa MAI un profit.
- **Idempotenza a TRE livelli** sulla chiave deterministica
  `cleared:<market_id>:<bet_id>` (la stessa `settlement_key` del consumer;
  piu' bet dello stesso mercato si applicano in sequenza e l'aggregatore
  ricalcola il market-net a ogni passo):
  1. **nel motore**: `PnLEngine` rifiuta di ri-realizzare lo stesso
     settlement (mercato, ref) — `CLEARED_SETTLEMENT_DUPLICATE` — qualunque
     sia il chiamante;
  2. in-memory nel poller per il processo vivo;
  3. **durevole al riavvio**: prima di emettere si interroga il cycle
     recovery state; se il checkpoint del consumer esiste, il settlement e'
     gia' stato consegnato e NON si ri-applica. Stato db illeggibile ⇒
     emissione sospesa senza marcare (ritentata quando il db risponde).
- **L'unita' atomica e' il CLUSTER TEMPORALE di mercato**: gambe dello
  stesso mercato settlate entro 2s **dall'ANCORA** (la prima gamba del
  cluster — la finestra NON e' transitiva: una catena t=0/1.9/3.8s non si
  fonde in un cluster illimitato, hardening R7) sono lo stesso evento di
  settlement (il jitter dei timestamp non e' un ordine reale) e dentro il
  cluster valgono i profit DECRESCENTI — il minimo dei prefissi coincide
  col netto del cluster, quindi le gambe perdenti di un dutching non
  possono far scattare l'emergency stop su un settlement che netta
  positivo. Gambe dello stesso mercato OLTRE la finestra (settlement
  parziali) sono eventi DISTINTI: cluster separati, rigiocati in ordine
  cronologico anche intrecciati con altri mercati — un dip storico reale
  scatta il breach quando sarebbe scattato, mai attenuato da una vincita
  successiva. Le date sono confrontate come datetime REALI (Z/offset/
  frazioni via `fromisoformat`: il confronto lessicografico tra formati
  ISO misti non e' cronologico; il floor supportato — Python 3.11 su CI,
  EXE Windows e venv — parsa nativamente anche offset senza `:`, frazioni
  lunghe e virgola decimale). Gambe non databili: **worst-case PER GAMBA,
  senza netting** (hardening R7: una coppia +100/-80 senza date non e'
  provabilmente un evento unico, e piazzata in coda col netto avrebbe
  potuto occultare un breach storico) — perdite non databili in TESTA,
  profitti non databili in coda. Costo accettato e documentato: una gamba
  di dutching perdente SENZA data accanto alla vincente datata puo' far
  scattare uno stop spurio su un mercato che netta positivo (fail-closed,
  mai fail-open); col caso reale Betfair (date presenti) vale il cluster
  winners-first.
- **Nota (degradazione conservativa)**: l'aggregatore market-net e' in
  memoria. Se un RIAVVIO cade tra due bet dello stesso mercato, il rimborso
  di commissione tra le due non viene riconosciuto: la perdita risulta al
  massimo SOVRA-stimata e il profitto SOTTO-stimato (es. +100 poi restart
  poi -30: net -30 invece di -28.65). Mai nella direzione fail-open.
- **Thread-safety**: lo stato del `PnLEngine` (posizioni, ledger market-net)
  e' serializzato da un lock unico tra worker del bus (fill/market update) e
  thread del poller; i GIRI di poll sono serializzati da un round-lock (un
  giro in corso esclude il successivo, anche di un thread vecchio); il loop
  del thread usa il SUO stop-event passato come argomento (un restart che
  rimpiazza l'attributo non gli fa mai adottare l'event nuovo).
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
