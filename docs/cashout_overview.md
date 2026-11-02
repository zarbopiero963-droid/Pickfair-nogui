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
  negati tutti (`customer_ref_in_conflitto`, fail-closed). Una richiesta
  rifiutata dall'engine prima del controllo (lockdown, normalizzazione fallita)
  scarta solo il proprio fence (`admission_fence_discard`), così non resta un
  fence orfano.
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
- un fence orfano (due ingressi in coda con lo stesso `customer_ref` consumati
  dall'engine in ordine inverso, bus che scarta la coda allo stop) nega le
  riconsegne di quel ref fino al riavvio: mai un ingresso senza fence. Oggi
  nessun produttore fornisce `customer_ref` duplicati;
- lo scarto del fence presuppone un solo consumer di `CMD_QUICK_BET` per bus
  (oggi un solo `TradingEngine`): un secondo consumer va prima reso compatibile;
- l'attesa degli ingressi in invio occupa il thread che instrada il cashout
  (worker del bus o poller dei settlement) fino all'esito dell'invio.

### 7.2 Dutching e bet manuale davanti alla barriera (#523)

Le corsie Dutching (`REQ_PLACE_DUTCHING`) e `manual_bet` restano **dormienti**
(nessun produttore in produzione, `tests/guardrails/test_dutching_lane_dormant_523.py`
invariato); la #523 le rende conformi ai gate prima di qualunque attivazione.

- **Lato obbligatorio**: ogni gamba e ogni bet manuale passano da
  `validators.signal_side` (stessa normalizzazione dei segnali: `"back "` →
  BACK). Lato assente, vuoto, non BACK/LAY o alias in conflitto => rifiuto
  (`INVALID_SIDE`), nessun ordine, nessun BACK per default. Il `RiskMiddleware`
  inoltra il lato esplicito o `None`, mai un default.
- **Ingresso atomico**: controlli di cap, dedupe, riserva del tavolo e
  ammissione di un lotto o di un bet manuale avvengono sotto
  `_risk_admission_lock` (lo stesso dei segnali); la pubblicazione avviene dopo,
  fuori dal lock. Due ingressi concorrenti non leggono la stessa esposizione né
  riservano lo stesso tavolo. Il tavolo è sempre riservato dal controller (un
  `table_id` del chiamante è ignorato); se la riserva fallisce la chiave
  anti-duplicato è rilasciata (retry sicuro). Lato da `side` o dall'alias
  `effectiveType`: entrambi presenti e diversi => `INVALID_SIDE`. Regola unica
  `validators.leg_side` (#529 cicli A4/A5), la stessa nel `RiskMiddleware`
  (`REQ_PLACE_DUTCHING`) e nel `DutchingController`: ogni campo di lato della
  gamba **presente** (`side`, `effectiveType`, `bet_type`) è esplicito, qualunque
  valore abbia, e deve essere BACK o LAY; vuoto, soli spazi, `None`, `"None"`,
  non stringa o sconosciuto => `INVALID_SIDE`, mai ereditato dal lotto; campi
  espliciti discordanti => `INVALID_SIDE`, mai una scelta arbitraria. Il lato
  esplicito della gamba prevale sul `bet_type` del lotto, che vale solo come
  fallback per una gamba **senza alcun campo di lato**; un `bet_type` del lotto
  presente (non `None`) deve essere a sua volta BACK/LAY. Sul bus il lotto non
  valido è rifiutato già dal middleware (`DUTCHING_FAILED`, nessun comando né
  ordine); `_payload_da_cmd` inoltra al controller solo i campi di lato presenti.
  La chiave anti-duplicato del lotto si acquisisce nella fase controllata di
  ammissione (sotto il lock, dopo tutti i controlli che possono rifiutarlo) e si
  rilascia solo se acquisita da quel lotto (con l'anti-duplicato disattivato la
  chiave dell'evento non è sua). Il `precheck` pubblico (e il preflight) è
  **privo di effetti permanenti**: controlla in sola lettura che la chiave non
  sia già attiva, non acquisisce né fissa nulla. Un lotto che fallisce prima di pubblicare una gamba è tolto
  dal registro senza rilascio e il controller libera tavolo e chiave una sola
  volta.
- **Identificativi storici**: gli ordini il cui intento è già consumato
  (`consumed_order_intents`, persistito nel DB e valido anche dopo il riavvio)
  sono rifiutati all'ammissione (`ingresso_gia_eseguito`); verifica del DB in
  errore => rifiuto (`ammissione_non_verificabile`).
- **Ammissione atomica del lotto**: prima di pubblicare il controller chiama
  `RuntimeController.admit_controller_entries(orders, batch_id=...)`. Sotto
  `_risk_admission_lock` cattura un fence per ogni gamba: se anche una sola
  cade in un ambito con cashout attivo, il lotto intero è rifiutato
  (`cashout_invalidation_active`, `DUTCHING_BATCH_REJECTED`) e nessun fence
  resta registrato. Le gambe ammesse portano il fence fino all'engine (§7.1):
  un cashout fra ammissione e invio nega le gambe in coda
  (`cashout_invalidation_stale`). **Il lotto resta atomico anche dopo
  l'ammissione** (#529 ciclo A2): alla verifica di una gamba l'engine controlla
  anche i fence delle gambe sorelle ancora in coda; se uno solo è stale (per
  esempio un cashout limitato alla selezione di un'altra gamba) il lotto è
  invalidato (`lotto_invalidato`) e nessuna gamba non ancora partita viene
  piazzata, anche con consegne concorrenti. Le gambe già inviate o incerte
  restano contabilizzate e trattenute. Runtime senza questa API, eccezione o esito
  non stringa => rifiuto (`barriera_cashout_non_disponibile` /
  `ammissione_non_verificabile`). Un lotto ancora aperto con lo stesso `batch_id`, o una sua
  gamba, non viene sovrascritto da nessuna ammissione, nemmeno a ordine
  singolo (`lotto_gia_attivo`).
- **Identificativi del Dutching** (#529 ciclo A2): `market_id`, ogni
  `selectionId`, ogni quota e lo stake totale passano dai validatori condivisi
  (`validators.order_market_id` / `order_selection_id` / `order_price` /
  `order_stake`, gli stessi della normalizzazione dell'engine) prima di
  qualunque acquisizione, riserva o pubblicazione: una sola gamba non valida
  rifiuta l'intero lotto, mai un piazzamento parziale. Ciclo A3: anche lo
  **stake calcolato di ogni gamba** passa da `validators.order_stake` (il
  riparto può produrre stake nulli o negativi: totale 0.05 su quote 2/3/50 dà
  0.10/0.10/−0.15); una gamba con stake nullo, negativo o non finito rifiuta
  il lotto in `precheck`, preflight e dry-run, senza chiavi, tavoli, esposizione
  né ordini.
- **Bet manuale**: `market_id`, `selection_id`, quota e stake sono validati
  con i validatori condivisi prima di ogni acquisizione (un ordine che l'engine
  rifiuterebbe non occupa un tavolo). Esposizione = rischio reale (`validators.order_exposure`:
  BACK stake, LAY stake × (quota − 1)) per `max_single_bet`, `max_total` e il
  tavolo, che ora il bet manuale occupa (nessun tavolo libero => rifiuto). Il
  dedupe si acquisisce dopo i controlli; un errore del dedupe rifiuta
  (prima era fail-open), anche nel `precheck` del Dutching.
- **Esito del lotto: trattenuto, mai rilasciato da eventi (#529, scelta owner
  F)**: le gambe di un lotto condividono tavolo e chiave anti-duplicato e lo
  stato terminale del lotto non è provato dal singolo esito. Nessun evento di
  ciclo di vita di una gamba (SUCCESS, FILLED, FAILED, ROLLBACK_DONE, AMBIGUOUS,
  tardivo o contraddittorio, `DUPLICATE_BLOCKED`) rilascia tavolo, esposizione o
  chiave del lotto (`_hold_batch_leg`): il lotto resta trattenuto fino a
  settlement e riconciliazione, anche con tutte le gambe fallite. Un AMBIGUOUS
  marca incerta la riserva sul tavolo del lotto. Unico rilascio: un lotto che
  fallisce **prima** di pubblicare qualunque gamba (nessun ordine partito) esce
  dal registro e il controller libera tavolo e chiave una sola volta; con
  almeno una gamba pubblicata il lotto resta trattenuto per intero. Il registro
  delle gambe è in memoria per la durata del processo e lo scrive solo
  `admit_controller_entries`; dopo un riavvio una gamba `dut-` fuori registro è
  trattenuta se l'ordine persistito porta `batch_id` o se il DB è illeggibile
  (un `batch_id` inoltrato da un segnale può solo trattenere quell'ordine, mai
  far rilasciare qualcosa). Un ref a monte `dut-…` senza `batch_id` resta un
  ordine singolo. Anche lo sweep F028 (ordini annullati/decaduti senza
  abbinato) salta le gambe di un lotto: una gamba sola non libera il tavolo
  condiviso. La chiave anti-duplicato del lotto è **fissata** (non scade col
  TTL): nel percorso Dutching la rilasciano soltanto i rollback prima della
  pubblicazione. I settlement dal poller **non** la rilasciano: dalla #461
  PR29-c `RUNTIME_CLOSE_POSITION` libera una chiave solo insieme al tavolo
  della riserva identificata (bet → `customer_ref` → tavolo), e un lotto ha
  rilascio a livello di lotto. Con `auto_close_enabled` attivo (default
  `False`, dormiente) la chiusura mark-to-market del `PnLEngine` di una gamba
  porta il `customer_ref` della gamba, che il tavolo del lotto non porta:
  tavolo e chiave restano, la perdita della gamba entra nel recovery del
  tavolo; il rilascio a livello di lotto resta della PR30. Fuori da quel percorso la chiave resta fissata fino
  al riavvio o a un reset esplicito della guardia (`DuplicationGuard.clear`; stesso evento => sempre
  `Duplicato bloccato`). Nemmeno il tavolo del lotto è liberato da un
  settlement per bet: con più gambe sullo stesso mercato il P&L della gamba non
  è il netto del tavolo e `_release_settled_tables` non rilascia (il mercato è
  solo marcato regolato per l'auto-reset); lo libera un settlement con
  `table_id`, uno di mercato SIM con tavolo univoco e certo, l'auto-reset o il
  riavvio. Limiti noti, fail-closed:
  il registro in memoria non si chiude al settlement (lo stesso lotto resta
  `lotto_gia_attivo` fino al riavvio) e un'incertezza marcata da un AMBIGUOUS
  resta fino a riavvio o riconciliazione. **PF-EXEC-02 non è completo**: esito del lotto, segnalazione
  del parziale, ribilanciamento, copertura e rilascio a livello di lotto sono
  della PR30.

**DEFERRED a PR30** (non in questa PR): PF-EXEC-02 completo (esito del lotto
dagli eventi, segnalazione del parziale, ribilanciamento e copertura con miglior
prezzo dinamico e Microstake, rilascio a livello di lotto), persistenza e
riconciliazione del lotto al riavvio (PF-EXEC-06; oggi il controller cerca
`runtime.dutching_batch_manager`, che il runtime non definisce). Costo noto del
fail-closed: un lotto con tutte le gambe fallite trattiene il tavolo fino a
settlement, riconciliazione o riavvio. Le corsie restano dormienti finché PR30
non è completa. Al riavvio valgono la dedupe e la riconciliazione per
singolo ordine: una gamba incerta resta incerta e nessuna gamba viene
ripiazzata. Limite noto: il broker SIM non verifica i fondi su nessuna corsia
(parità SIM/LIVE, tracciato a parte).

**Sim-broadcast guard**: un cashout eseguito in **simulazione** non fa broadcast
del `MASTER_CASHOUT` ai follower reali (il flag `sim` è catturato al piazzamento,
non riletto dopo, per evitare uno switch SIM/LIVE in volo).

### 7.3 L'hedge nel ledger `orders` (F-05, CSH-06)

Prima di F-05 l'hedge del cashout passava da `OrderRouter` senza alcuna riga nel ledger `orders`: il denaro si
muoveva sull'exchange ma il bot non lo contabilizzava (daily-loss LIVE cieco sull'hedge, secondo cashout LIVE che
poteva ricoprire la stessa posizione). Da F-05 `CashoutExecutor`, costruito con `ledger=db` da
`cabla_catena_cashout` (headless e GUI):

1. assegna all'hedge un `customer_ref` proprio, `hedge:` + 26 esadecimali (32 caratteri), inviato al broker;
2. **prima dell'invio** scrive la riga `orders` in stato `HEDGE_PENDING` (write-ahead), con payload
   `role="HEDGE"`, `order_origin="CASHOUT_HEDGE"`, mercato/selezione/lato/prezzo/stake, `green_up`,
   `cashout_request_id`, **senza** `event_key`/`table_id`/`batch_id`. Se la riga non si scrive, l'hedge NON parte
   (`CASHOUT_FAILED` `REJECTED`, `ledger_write_failed`);
3. aggiorna la stessa riga con l'esito: `HEDGE_PLACED` (abbinato), `HEDGE_UNMATCHED` (a riposo), `HEDGE_FAILED`
   (rifiutato), `HEDGE_AMBIGUOUS` (esito ignoto: mai retry). Un aggiornamento fallito lascia la riga
   `HEDGE_PENDING` e un log CRITICAL; l'esito pubblicato resta quello reale.

Gli stati `HEDGE_*` non sono mai `INFLIGHT/SUBMITTED/AMBIGUOUS`: de-dup dell'engine, riconciliazione all'avvio e
rebuild dei tavoli (F018) non li vedono (nessun tavolo fantasma, nessun avvio in PAUSED). I consumer che attribuiscono
un ordine a un tavolo (`_resolve_market_settlement`, `_single_bet_position`) escludono i ref `hedge:`
(`core.hedge_ref.is_hedge_order_ref`): l'hedge non e' un secondo ingresso e il P&L del mercato si contabilizza una
volta. L'identita' I1 (`Database.get_bot_active_orders`) invece include la bet dell'hedge: il daily-loss LIVE la
vede, e ingresso + hedge sulla stessa selezione formano una posizione MISTA che il router non ricopre.

**Idempotenza.** Prima del write-ahead, sotto un lock (i worker del bus sono concorrenti), l'executor rifiuta con
`REJECTED`: `duplicate_cashout_request` se la stessa `cashout_request_id` ha gia' un hedge non `HEDGE_FAILED`;
`hedge_outcome_uncertain` se sulla stessa selezione c'e' un hedge `HEDGE_PENDING`/`HEDGE_AMBIGUOUS` (esito ignoto:
mai retry); `previous_hedge_still_live` se sulla stessa selezione un hedge `HEDGE_UNMATCHED`/`HEDGE_PLACED` e' ancora
abbinabile sull'exchange (`sizeRemaining > 0` negli ordini correnti di `BetfairService.list_current_orders`, residuo
illeggibile o non finito = vivo): si ripunta solo dopo la cancellazione confermata del precedente (PF-EXEC-01; il
router pubblica il nuovo hedge prima del flatten best-effort del resting); `previous_hedge_matched` se il precedente
risulta gia' abbinato (`sizeMatched > 0` con residuo 0: `listCurrentOrders` riporta l'hedge eseguito fino al
settlement, quindi la posizione e' gia' coperta). Si ripunta solo se l'hedge precedente e' sparito dagli ordini
correnti o e' chiuso con abbinato 0 e residuo 0; `hedge_liveness_unverifiable` se gli ordini
correnti non si leggono, non sono una lista di ordini ben formati (`None`, envelope, elemento non-dict, ordine senza `betId`), la fonte manca
o l'hedge precedente non ha `bet_id`; `ledger_read_failed` se il ledger non si
legge. Un hedge `HEDGE_FAILED` (mai piazzato) non blocca.

**Riavvio dopo un crash.** `CashoutExecutor.wire()` marca `HEDGE_AMBIGUOUS` (`ambiguity_reason=restart_before_outcome`)
ogni riga `HEDGE_PENDING` del processo precedente: l'hedge puo' essere sull'exchange, quindi riconciliazione manuale,
nessun nuovo hedge sulla selezione e nessun ingresso aperto fantasma. Limite residuo: la riconciliazione AUTOMATICA
di queste righe contro `listCurrentOrders` (LIVE non riporta il `customerOrderRef`) e' un follow-up.
Test: `tests/f05/` (unita' e HeadlessApp reale in SIM), mutazioni `f05_*` in `guardrails/mutation_catalog.json`.

### 7.4 P&L del monitor SL/TP (F-01, CSH-01/CSH-10)

`RuntimeController._monitor_sl_tp_trailing` (inerte fino a F-02: nessuna sorgente SL/TP nei meta dei tavoli)
confronta SL/TP con il **netto liquidabile** della posizione (decisione owner #426 c.6112703848), calcolato da
`core/sl_tp_pnl.py` in Decimal con la stessa formula del cashout (`dutching.dynamic_cashout_single`), estesa a piu'
livelli del book:

- hedge al lato opposto della posizione (BACK => `availableToLay`, LAY => `availableToBack`), mai `lastPriceTraded`,
  **consumato livello per livello, ognuno al proprio prezzo e fino alla propria size**, finche'
  `somma(hedge_i * prezzo_i) = stake * ingresso` (`sl_tp_pnl.closing_legs`); P&L = media dei due esiti della posizione
  con quelle gambe (`sl_tp_pnl.liquidable_pnl_legs`). Applicare il prezzo del livello piu' profondo a tutta la
  posizione dichiarava coperti book che non lo erano e poteva invertire il P&L (Codex e Sol su `abfd2ed`);
- il TP scatta solo se il book basta a chiudere l'intera posizione e confronta il netto dell'esito PEGGIORE sulle
  gambe arrotondate (profitto garantito: un'ultima gamba arrotondata a zero non conta); lo SL, chiusura
  protettiva (PF-EXEC-03), scatta anche con book insufficiente: il residuo non coperto e' valutato all'ultimo livello;
- la quota minima 1.01 e' valida (stessa formula del cashout, calcolata qui anche dove `dynamic_cashout_single`
  la rifiuta);
- stake reale della gamba: `meta["stake"]`, altrimenti BACK = esposizione e LAY = liability/(prezzo-1);
- commissione di policy del cashout (`BETFAIR_ITALY_COMMISSION_PCT`), solo sul netto positivo;
- input non validi (NaN/inf/non numerici, book vuoto) o calcolo che solleva => nessuna decisione (fail-closed).

Esempi (stake 10, ingresso 2.0, commissione 4.5 %): BACK chiuso in LAY a 2.5 => -2.00, a 1.6 => +2.39;
LAY chiuso in BACK a 2.5 => +1.91, a 1.6 => -2.50; LAY con liability 10 a 3.0 (stake 5) chiuso a 4.0 => +1.19.
Su piu' livelli: BACK chiuso con LAY 12 a 1.60 + 1 a 2.50 => 12 a 1.60 + 0.32 a 2.50, netto +2.22 (non -2.00).
Prima il BACK aveva il segno invertito (una perdita sembrava profitto). Test: `tests/core/test_sl_tp_pnl_sign.py`.

### 7.5 Hedge coerente con la posizione (F-06, CSH-05/MM-02/NF-01)

Prima di F-06 `CashoutExecutor` piazzava qualunque lato e size arrivasse nel comando (un hedge BACK su una posizione
BACK la raddoppia; un LAY 50 su una posizione da 0.23 apre una nuova esposizione) e `RiskMiddleware` metteva
`side="LAY"` quando il lato mancava. Ora:

- `RiskMiddleware._cashout_side`: lato solo se dichiarato `BACK`/`LAY`; mancante o invalido => `CASHOUT_FAILED`
  (`side_mancante_o_invalido`), mai un lato di default. L'auto-close del monitor SL/TP (inerte fino a F-02) dovra'
  quindi mandare il lato dell'hedge;
- `CashoutExecutor._hedge_coherence_block`, dentro il preflight del ledger (dopo i controlli sugli hedge precedenti
  di §7.3, sotto lo stesso lock) e anche senza ledger, decide con `RiskGate.protective_hedge`:
  - lato della posizione da `original_pos` (`cashout_resolver` lo mette sempre); assente/invalido =>
    `hedge_non_coerente:posizione_assente`;
  - lato dell'hedge = opposto della posizione, altrimenti `hedge_non_coerente:lato`;
  - posizione da chiudere = quella IDENTIFICATA dal comando (`original_pos`: stake x prezzo, illeggibile =>
    `posizione_assente`), limitata dall'alto dall'esposizione NETTA della selezione sull'exchange
    (`BetfairService.list_current_orders`, che in LIVE riporta anche gli ordini `EXECUTION_COMPLETE`): tutto l'abbinato
    del lato della posizione meno tutto l'abbinato del lato opposto, hedge compresi (una coppia gia' chiusa dal suo
    hedge si annulla, Sol su `73296a3`), `sizeMatched * averagePriceMatched`
    (se <= 1: `priceSize.price`, come `cashout_resolver._row_matched_price`). Vale il minore dei due: altri ingressi
    dello stesso lato (manuali, altri tavoli) non allargano mai l'hedge, una coppia gia' neutra non conta e una chiusura
    gia' abbinata lo riduce (Sol su `a16f0d3`). Esposizione nulla => `posizione_assente`; ordini illeggibili,
    malformati o in errore, `sizeMatched` assente o non numerico (mai letto come zero, Codex su `73296a3`), una riga
    che non si puo' escludere con certezza dalla selezione (mercato illeggibile; selezione illeggibile sul mercato
    target; lato illeggibile sulla selezione target, Codex su `954c2cd`; grafie camelCase/snake_case
    dell'identificativo presenti e in disaccordo, Sol su `360aa7d`) =>
    `hedge_non_coerente:posizione_non_verificabile`. Un `original_pos` col lato opposto alla posizione reale ha netto
    negativo sul lato dichiarato => rifiuto (mai un raddoppio);
  - stake dell'hedge <= `esposizione / prezzo_hedge + 0.005` (confronto esatto in Decimal: il mezzo centesimo e'
    il massimo che l'arrotondamento ROUND_HALF_UP del resolver aggiunge), altrimenti `hedge_non_coerente:size`. Un
    secondo arrotondamento su un float rifiutava green-up legittimi al confine (0.35 x 11.0 / 4.4 = 0.875 => 0.88
    dal resolver, 0.87 ricalcolato). Nessun cap di stake: Roserpina resta l'unica autorita'
    sullo stake d'ingresso, qui si verifica solo che l'hedge chiuda (PF-EXEC-03, nessun inseguimento);
  - senza fonte ordini (doppi di test) l'esposizione e' quella di `original_pos` (`stake * price`): il controllo non
    si salta mai.

Esempio: BACK 0.23 abbinato a 2.0 (esposizione 0.46), chiusura a 1.8 => hedge LAY massimo 0.26; LAY 50 => rifiuto.
Limite: ordini manuali del lato opposto sulla stessa selezione riducono il netto e possono far rifiutare il green-up
del bot (fail-closed: mai un hedge oltre l'esposizione reale). Test: `tests/f06/`; mutazioni `f06_*`.

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
