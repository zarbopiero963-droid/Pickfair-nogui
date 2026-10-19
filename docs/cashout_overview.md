# Cashout Pickfair — Spiegazione completa (deliverable B5)

> Stato: **cashout attivo end-to-end** dal merge di #311 (trigger) + #313 (residui) + #314 (copy-pattern).
> Questo documento spiega cosa fa, come, con esempi numerici reali, e i possibili sviluppi.

---

## 1. Cos'è il cashout, in una riga

Chiudere in anticipo una posizione aperta del bot piazzando una scommessa **opposta** (green-up):
una posizione **BACK** si chiude con una **LAY** alla quota corrente (e viceversa), bloccando lo
stesso risultato economico **comunque vada l'evento**.

---

## 2. La pipeline end-to-end

```
Telegram (nativo)  ─┐
                    ├─► TelegramListener.parse_signal ─► SIGNAL_RECEIVED
copy-pattern (regex)┘                                        │
                                                             ▼
                                   RuntimeController._on_signal_received
                                   · gate live: emergency-stop (incl. daily-loss
                                     pending), session-guard, deploy-gate, runtime-active
                                   · guard anti-silent-drop (catena cablata?)
                                   · branch signal_type ∈ {CASHOUT, CASHOUT_ALL}
                                                             │
                                                             ▼
                                   CashoutRouter (A3, query LIVE)
                                   · posizioni del bot = list_current_orders +
                                     identità DB (get_bot_active_orders, I1)
                                   · prezzo di chiusura dal lato eseguibile, dal
                                     book chiesto con i prezzi; cancella il
                                     resting non abbinato prima del green-up
                                   · calcola lo stake green-up (dutching)
                                             │ pubblica SOLO
                                             ▼
                                   REQ_EXECUTE_CASHOUT
                                             │
                                             ▼
                                   CashoutRequestBridge (cashout-only)
                                   · dedup per identità posizione, normalizza,
                                     valida (side, selection_id>0), CASHOUT_FAILED
                                     strutturato sui rifiuti
                                             │ pubblica
                                             ▼
                                   CMD_EXECUTE_CASHOUT
                                             │
                                             ▼
                                   CashoutExecutor
                                   · invarianti real-money hard + SafetyLayer
                                   · piazza l'hedge via OrderRouter (sim/live)
                                             │
                                ┌────────────┴─────────────┐
                                ▼                          ▼
                       CASHOUT_SUCCESS              CASHOUT_FAILED
                       · sim-broadcast guard        · CashoutResidualHandler:
                         (no MASTER_CASHOUT ai         UNMATCHED → 1 cancel + persist
                          follower se simulato)        AMBIGUOUS → no-cancel + reconcile
                                                       ERROR/REJECTED → notify + persist
```

Mappa PR: B0 resolver (#296) · B1 router (#300) · B2.1 cancel-adapter (#303) ·
B2.2 identità DB (#304) · B2.3 executor (#307) · B2.4a flatten/depth (#308) ·
B2.4b-1 catena dormiente (#310) · **B2.4b-2 trigger+residuo (#311, attiva)** ·
B2.5 residui shape/sim-guard (#313) · **2.1-C copy-pattern action (#314)** ·
SafetyLayer nell'executor (#445) · **catena anche nella GUI (PR04 #461)**.

Bridge, executor, `OrderRouter` e gestore del residuo li costruisce
`cashout_wiring.cabla_catena_cashout`, la **stessa** funzione in
`headless_main` e in `mini_gui`: i due entrypoint non possono divergere, e su
ciascun bus c'è **un solo** consumer per topic (due consumer di
`REQ_EXECUTE_CASHOUT` sarebbero due hedge reali per un comando). Router e
resolver restano costruiti dal `RuntimeController` a ogni segnale.

---

## 3. La matematica del green-up (con numeri reali)

Formula (`dutching.dynamic_cashout_single`, commissione Betfair Italia 4.5%):

- **stake di chiusura** = `matched_stake × matched_price / current_price`
- BACK: `profit_if_win = stake×(mp−1) − cash×(cp−1)`, `profit_if_lose = cash − stake`
- `green_up (lordo) = (profit_if_win + profit_if_lose) / 2`
- `net_profit = green_up − commissione 4.5%`

### Esempio 1 — BACK in profitto
Avevo BACK **€10 @ 3.0**; la quota è scesa a **2.0** (il mio esito è più probabile).
→ piazzo **LAY €15.00 @ 2.0**. profit_if_win = +5.00, profit_if_lose = +5.00 →
**green-up lordo €5.00**, **netto €4.78**. Guadagno **garantito** ~€4.78 comunque vada.

### Esempio 2 — BACK in perdita (cashout come stop-loss)
BACK **€10 @ 3.0**; la quota è salita a **4.0** (esito meno probabile).
→ **LAY €7.50 @ 4.0**. profit = **−€2.50** in entrambi i casi: chiudo accettando una
perdita di €2.50 invece di rischiare gli interi €10.

### Esempio 3 — LAY in profitto
LAY **€10 @ 2.0**; la quota è salita a **3.0**.
→ piazzo **BACK €6.67 @ 3.0**. green-up lordo **€3.34**, netto **€3.19**.

> Nota: lo stake green-up è arrotondato allo step Betfair; se arrotonda a 0 il
> resolver **rifiuta** (niente ordine inutile). La commissione 4.5% è enforced
> nel layer math (Betfair Italia), non configurabile a runtime.
>
> La «quota corrente» (`current_price`) è il prezzo a cui la chiusura si abbina
> subito: per chiudere un BACK si banca al miglior `availableToLay`, per chiudere
> un LAY si punta al miglior `availableToBack` (vedi §5).

---

## 4. I due punti d'ingresso

1. **Telegram nativo** — un messaggio "CASHOUT" / "CASHOUT ALL" parsato da
   `_parse_cashout_signal`. `CASHOUT` singolo è ristretto alla partita via
   `event_name` (estratto dal testo); `CASHOUT_ALL` chiude tutto.

2. **Copy-pattern (regex)** — un pattern in `signal_patterns` con colonna
   **`action`** (`QUICK_BET` default / `CASHOUT` / `CASHOUT_ALL`). Quando il
   pattern matcha e supera i suoi filtri (minuto/score/live_only):
   - `action=CASHOUT_ALL` → emette il signal_type cashout, confluisce nel
     **medesimo** routing nativo (un solo percorso d'esecuzione);
   - `action=CASHOUT` singolo → **fail-closed**: skip visibile con warning
     `copy_pattern_cashout_single_requires_targeting` (vedi §7), perché
     confluirebbe nel routing event-level e chiuderebbe **tutta** la partita
     invece della selezione configurata.

Entrambi i percorsi finiscono in `SIGNAL_RECEIVED` → stesso routing: **nessuna
duplicazione di logica d'esecuzione**.

---

## 5. Come si trovano le posizioni da chiudere (design A3)

Scelta owner **A3**: la sorgente delle posizioni è una **query LIVE degli ordini
correnti** al momento del cashout (`list_current_orders`), **non** un tracker a
eventi (il `PnLEngine` sarebbe rimasto vuoto in produzione — vedi PR #298 chiusa).

- L'identità "ordine del bot" viene dal DB (`get_bot_active_orders`, identità
  **I1**): il feed Betfair è a livello account, quindi si filtra ai soli ordini
  piazzati dal bot (niente bet manuali o di altre strategie).
- Si gestiscono solo posizioni a **lato singolo** (BACK *o* LAY); le selezioni
  **miste** (matched su entrambi i lati) vengono **saltate** per non rischiare un
  hedge sul lato sbagliato (il netting P&L-based è un follow-up).
- Il **resting non abbinato** (`sizeRemaining`) viene **cancellato** prima del
  green-up, così non resta esposizione fantasma.
- **Prezzo di chiusura** (`cashout_router._closing_price`): l'hedge si prezza dal
  lato su cui si abbina subito, come sull'exchange Betfair (#383, F6 della #453):
  chiudere un BACK = LAY al miglior `availableToLay`, chiudere un LAY = BACK al
  miglior `availableToBack`. Il gate di profondità (L95) guarda la size dello
  stesso livello. Fino a F6 il router usava il lato opposto, allineato al motore
  SIM di allora: in LIVE l'hedge sarebbe rimasto a riposo e il gestore del
  residuo l'avrebbe cancellato.
- **Book con i prezzi** in LIVE: il runtime passa al router
  `get_market_book_snapshot(market_id, include_prices=True)`, cioè
  `listMarketBook` con `EX_BEST_OFFERS`. Senza `priceProjection` Betfair non
  popola le ladder: fino a F6 il router non trovava il prezzo di chiusura e
  saltava ogni posizione con un solo warning nel log. In SIM il flag è
  ininfluente.

---

## 6. Sicurezza: gate e fail-closed

- **Gate live a monte** (E1): un cashout passa solo se l'order-entry live è
  consentito — emergency-stop (incl. daily-loss pending), session-guard LIVE,
  deploy-gate, runtime-active.
- **Guard anti-silent-drop**: il trigger è in `RuntimeController` (tutti gli
  entrypoint); la catena la cablano `HeadlessApp` e `MiniPickfairGUI` con
  `cashout_wiring` (dalla PR04 #461, prima solo l'headless). In un entrypoint
  senza catena (un test, uno strumento) il cashout viene **rifiutato
  visibilmente** (`cashout_chain_not_wired`), mai droppato in silenzio.
- **Nella GUI**: l'esito (`CASHOUT_SUCCESS` / `CASHOUT_FAILED`) compare nella
  tab `Log`, e Storico Bet e Risk Desk si aggiornano. La GUI non ha un
  controllo cashout manuale: il comando arriva dalla chat o dal copy-pattern,
  come nell'headless.
- **Il runtime pubblica solo `REQ_EXECUTE_CASHOUT`**, mai `CMD_*` diretto.
- **Dedup** per identità posizione nel bridge: un price-drift o metadata diversi
  non generano un secondo hedge per la stessa posizione.
- **Validazione**: `selection_id>0` (bool rifiutati), `side` esplicito, stake>0.
- **Commissione 4.5%** enforced; **nessun retry automatico** lato esecuzione.

---

## 7. Gestione del residuo (CASHOUT_FAILED)

`CashoutResidualHandler` consuma `CASHOUT_FAILED` (shape **dict** uniforme da
runtime, bridge, executor e risk_middleware), split per status:

| status | azione |
|---|---|
| **UNMATCHED** (hedge piazzato ma non abbinato) | **un solo** cancel sicuro del resting via cancel-adapter (mai retry) + persist record con esito (`cancel_attempted`/`cancel_ok`/`cancel_error`) + notify operatore |
| **AMBIGUOUS** (esito ignoto da timeout/rete) | **mai** cancel automatico (l'ordine potrebbe esistere) + persist record di **reconciliation** + notify **alta severità** |
| **ERROR / REJECTED / FAILURE** | notify + persist diagnostico se c'è contesto |

Persistenza su `audit_events` (zero migrazioni). Notify Telegram (best-effort).
Il cancel è limitato **esclusivamente** al cashout UNMATCHED: mai
QUICK_BET/dutching/cancel/replace.

Dopo ogni esito l'handler pubblica `CASHOUT_RESIDUAL_HANDLED` (`status`,
`cancel_attempted`, `cancel_ok`, `bet_id`, `market_id`, `selection_id`,
`cashout_request_id`).

### 7.1 Barriera d'ammissione degli ingressi (PR28-d, #522)

Un CASHOUT/CASHOUT_ALL apre una **barriera** sul proprio ambito (globale per
CASHOUT_ALL; altrimenti la partita — ogni alias `event_name`/`event`/`match` —
più mercato o selezione). Finché è attiva, nessun nuovo ingresso nell'ambito
viene ammesso (`cashout_invalidation_active`); il cashout non viene mai
ritardato (PF-EXEC-03).

- Ogni `REQ_EXECUTE_CASHOUT` porta un `cashout_request_id`; la barriera resta
  attiva finché **ogni** richiesta ha un esito certo: `CASHOUT_SUCCESS`,
  `CASHOUT_FAILED` certo, `CASHOUT_REQUEST_SUPPRESSED` (duplicato del bridge).
- **UNMATCHED** non chiude la barriera da solo: la chiude
  `CASHOUT_RESIDUAL_HANDLED` con `cancel_ok=True`. Cancel non tentato (nessun
  `bet_id`) o non confermato => barriera tenuta (incerta) fino al riavvio.
- **Match parziale**: `CASHOUT_SUCCESS` dichiara `requested`, `residual` e `partial`.
  Solo `partial=False` (match completo) chiude la richiesta. Con `partial` vero o
  non dichiarato il `CashoutResidualHandler` esegue **un solo** cancel del residuo
  dello stesso `bet_id`, persiste un record `PARTIAL` e pubblica
  `CASHOUT_RESIDUAL_HANDLED`: la barriera si chiude solo con `cancel_ok=True`,
  altrimenti resta incerta fino al riavvio e alla riconciliazione.
- **AMBIGUOUS/ERROR** => barriera incerta, tenuta fino al riavvio.
- `get_status()` espone `cashout_barriers_active` e `cashout_barriers_uncertain`.
- **Fence fino all'engine**: col bus non bloccante `CMD_QUICK_BET` è solo
  accodato. L'ingresso porta la registrazione del fence (`customer_ref`) e il
  `TradingEngine` la ricontrolla prima del piazzamento
  (`admission_fence_verdict`): cashout iniziato nel suo ambito =>
  `cashout_invalidation_stale`, `QUICK_BET_FAILED`, tavolo e dedupe liberati.
  La registrazione si consuma per ogni ordine che arriva all'engine, anche
  negato da safe mode o RiskGate; un errore del controllo nega l'ordine
  (`ADMISSION_FENCE_RAISED` / `ADMISSION_FENCE_BAD_RESULT`).
- **Un fence per richiesta**: due ingressi in coda con lo stesso `customer_ref`
  non si scambiano il fence; finché la coda di quel ref non si svuota sono
  negati tutti (`customer_ref_in_conflitto`, fail-closed).
- **Ingresso già in invio**: un ingresso che ha superato il fence resta registrato
  come «in volo» finché l'engine non ha l'esito dell'invio
  (`admission_placement_done`, qualunque esito). Un cashout nel suo ambito, prima
  di fotografare l'exchange, attende quegli ingressi (fino a 35 s,
  `_ENTRIES_IN_FLIGHT_WAIT_S`), così la fotografia del router contiene anche
  l'ordine appena piazzato. Attesa scaduta, o routing con il lock d'ammissione
  trattenuto: il cashout parte comunque (mai bloccato) e la barriera diventa
  incerta fino al riavvio.

- **Invio con esito ignoto**: se l'engine rinuncia all'esito (AMBIGUOUS, attesa
  dell'executor scaduta: il task può ancora inviare), eccezione o risultato
  mancante, l'ingresso passa fra gli «ambigui». Un cashout nel suo ambito diventa
  subito incerto (nessuna attesa) finché un esito noto dello stesso ordine
  (`QUICK_BET_*` diverso da AMBIGUOUS, con lo stesso `correlation_id`, non un
  rifiuto pre-invio DENIED/DUPLICATE_BLOCKED) non lo risolve. Oggi questi esiti
  arrivano solo dal ciclo di vita dell'ordine in LIVE (`order_manager`); la
  riconciliazione non pubblica `QUICK_BET_*`, quindi in SIM l'ingresso resta
  ambiguo fino al riavvio (fail-closed). La registrazione appartiene alla
  richiesta che l'ha creata: un secondo ingresso con lo stesso `customer_ref`,
  mentre il primo è in invio o ambiguo, viene negato (`ingresso_gia_in_invio`)
  e non la sovrascrive.

Limiti noti:
- un residuo di hedge parziale che si abbina da solo prima del cancel fa fallire
  il cancel: barriera incerta fino al riavvio anche a posizione chiusa
  (fail-closed, costo operativo);
- l'attesa degli ingressi in invio occupa il thread che instrada il cashout
  (worker del bus o poller dei settlement) fino all'esito dell'invio.

**Sim-broadcast guard**: un cashout eseguito in **simulazione** non fa broadcast
del `MASTER_CASHOUT` ai follower reali (il flag `sim` è catturato al piazzamento,
non riletto dopo, per evitare uno switch SIM/LIVE in volo).

---

## 8. Limiti noti & possibili aggiunte (follow-up)

1. **Targeting per-selezione del CASHOUT singolo da copy-pattern** *(il più
   richiesto)*. Oggi un copy-pattern `CASHOUT` singolo è fail-closed. Per
   abilitarlo serve estendere `CashoutRouter`/`cashout_resolver` perché un
   cashout possa restringersi a `selection_id`/`market_type` specifici (passati
   nel segnale) invece che all'intero evento. **Scope: cashout core**.
2. **Netting P&L-based delle posizioni miste**: oggi le selezioni con matched su
   entrambi i lati vengono saltate; un netting per residuo netto le renderebbe
   chiudibili in sicurezza.
3. **Cashout parziale** (chiudere il 50% di una posizione): la math `dutching`
   già scala con lo stake; servirebbe un parametro `fraction` nel segnale + UI.
4. **Reconciliation automatica degli AMBIGUOUS**: oggi è manuale (record +
   notify). Un job che ri-interroga Betfair dopo N secondi per risolvere l'esito
   ignoto chiuderebbe il loop senza intervento umano.
5. ~~**Wiring comune della catena**~~ — **fatto** (PR04 #461):
   `cashout_wiring.cabla_catena_cashout` costruisce
   OrderRouter/Executor/Bridge/ResidualHandler per `headless_main` e per
   `mini_gui`. Nella stessa PR la chiusura della finestra ferma il runtime per
   primo (hook `runtime_stop`, come `HeadlessApp.stop`), e
   `RuntimeController.stop` rende il runtime non attivo prima di smontare
   Telegram e Betfair: un comando arrivato durante lo smontaggio è rifiutato
   (`runtime_non_attivo:STOPPED`), un cashout differito dal grace è annullato
   (`CASHOUT_FAILED` `grace_aborted`). Se lo stop di Telegram fallisce,
   Betfair si disconnette comunque. Restano per la PR17 della #461 lo
   shutdown unico e idempotente GUI/headless (errori del main, doppia
   chiusura) e i comandi già accettati dal bus prima dello stop, che oggi
   completano durante lo smontaggio.
6. **Telemetria/UI del cashout**: contatori SUCCESS/UNMATCHED/AMBIGUOUS e green-up
   cumulato negli observability snapshot.

---

## 9. Verifica

Catena coperta da ~200 unit/integration test (resolver, router, cancel-adapter,
bridge, executor, residual-handler, trigger, copy-pattern, migrazione DB). Gate
CI verdi su tutte le PR (smoke, integration, e2e, guardrails).
DeepSource grade A sulle ultime PR.
