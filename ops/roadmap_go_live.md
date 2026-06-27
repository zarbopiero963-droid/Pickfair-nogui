# Roadmap Go-Live — Pickfair (denaro reale, operatività autonoma, copy trading via Telegram)

> Documento consolidato dall'audit completo del 2026-06-10.
> Fonde: audit a 4 aree (esecuzione, Telegram, safety/ops, config/deploy),
> verifica del branch `claude/test-copy-trading-features-EgIvQ`,
> validazione punto-per-punto dell'audit Codex (UFA-001..020),
> analisi micro-stake / copy-mirror / stake-mode / best-price.

## Stato di partenza (verificato)

**Solido in `main`:**

- Esecuzione live Betfair completa: `placeOrders` JSON-RPC reale, login con certificati,
  gate fail-closed SIMULATION/LIVE (`core/safety_layer.py:91`), cancel orders.
- Deploy gate LIVE con blocker reali (`core/runtime_controller.py:771-851`), incluso:
  key-source strict forzato in LIVE, hard-stop config obbligatoria
  (`max_daily_loss`, `max_drawdown_hard_stop_pct`, `max_open_exposure`).
- Daily-loss breach monitoring (solo alert, non ferma il bot).
- Money management Roserpina forzato su ogni segnale; circuit breaker API e ordini;
  safe mode; kill switch; watchdog/invariant/anomaly/incident stack completo.
- Runtime headless 24/7; segreti cifrati at rest; nessun secret committato.
- Lato master copy trading: broadcast MASTER SIGNAL (singola/dutching) già implementato.
- Parser follower multi-formato + resolver con best-price dal book live (percorso resolver).

**Sul branch `claude/test-copy-trading-features-EgIvQ` (NON mergiato, base ~80 PR indietro):**

- Form pattern modale unico; parola chiave (keyword/regex/entrambi); dropdown con tutti
  i mercati Betfair; pattern predefiniti; stake fisso / MM auto / pre-match per pattern;
  fix estrattori per messaggi senza emoji; 7 test E2E con Telegram reale + workflow CI
  (`telegram-live-integration.yml`, cron + manuale, skip senza secrets).

**Blocker confermati (tutte le fonti concordano):**

| # | Blocker | Evidenza |
|---|---|---|
| B1 | Listener Telegram = stub, nessuna connessione reale | `telegram_listener.py:83-90` (stub anche sul branch) |
| B2 | Keepalive sessione betting assente (scade ~20 min) | keepalive solo su streaming (`streaming_feed.py:246`) |
| B3 | `get_current_orders`/`listCurrentOrders` mancanti nel client live | chiamati da `reconciliation_engine.py:644,1898` |
| B4 | Daily-loss breach allerta ma non ferma | `test_..._is_alerted_and_does_not_force_stop` |
| B5 | Cashout mirror incompleto: manca publisher `CASHOUT_SUCCESS`, manca routing segnale→`REQ_EXECUTE_CASHOUT`, manca executor `CMD_EXECUTE_CASHOUT` | il bus REQ→CMD esiste già (`core/risk_middleware.py:41,243`) |
| B6 | Best price NON sul percorso DIRECT (copy) | `telegram_signal_processor.py:262`; flag `use_best_price` morto |
| B7 | Stake master non trasmesso; MM sovrascrive sempre lo stake | `telegram_sender.py:47-57`; `runtime_controller.py:1410` |
| B8 | `replace_orders` chiamato ma inesistente; niente `sizeReduction` | `order_manager.py:842`; `betfair_client.py:552` |
| B9 | Suite test bloccata (collisioni nomi file; FakeBatchManager) | `issues/` |
| B10 | Gate ops tutti `[ ]`, backup DB non automatizzato | `ops/*.md` |

---

## FASE 0 — Fondamenta (1–2 giorni)

| Task | Descrizione | Note |
|---|---|---|
| 0.1 ✅ | Ambiente riproducibile: `bash scripts/setup_dev_env.sh` (venv dedicato; il setuptools Debian rompe la build di pyaes) | Fatto 2026-06-10 |
| 0.2 ✅ | Issue di test: verificate già risolte su main (file rinominati in passato, FakeBatchManager 7/7 pass); doc in `issues/` aggiornati | Baseline: **3057 passed, 0 failed** |
| 0.3 ✅ | Lint baseline live-critical: ruff a zero sui file in scope, incl. fix del bug latente `Set` in `trading_engine.py` | Import canonico protetto con `# isort: off` (guardrail); `order_manager.py` rinviato per overlap con PR #224 |
| 0.4 ✅ | Rebase del branch copy-trading su main: 5 commit cherry-pickati (form pattern modale, parola chiave, tutti i mercati Betfair, stake fisso/MM auto/pre-match per pattern, fix estrattori, E2E Telegram reali + workflow CI) | Import `customtkinter` reso guarded per compatibilità headless; suite 3057 passed + 7 E2E skipped senza secrets |

## FASE 1 — Blocker live (3–4 giorni)

| Task | Descrizione | Note |
|---|---|---|
| 1.1 ✅ | **Listener Telethon reale**: runtime in thread dedicato con event loop proprio, handler NewMessage sui `monitored_chats`, `last_successful_message_ts` su ogni messaggio, fail-closed su telethon/sessione mancante o non autorizzata, disconnessione inattesa → FAILED (autoheal decide); client factory iniettabile per i test; fix keyword: le righe statistiche (📈🥅🎯📊/Possesso) sono escluse dal match parola chiave | Lo snapshot runtime ora riporta gli handler Telethon (0/1) come richiesto dall'invariant guard; i callback restano in `status()` |
| 1.2 ✅ | **Keepalive sessione betting**: `BetfairClient.keep_alive()` chiama l'**endpoint Betfair `/api/keepAlive`** (con `X-Authentication`) — è l'operazione che resetta il timeout di sessione (per i doc Betfair una normale API come `getAccountFunds` NON lo estende); non muta ordini/conto, redazione token sugli errori. Loop daemon ~600s in `BetfairService` (mirror del keepalive streaming), avviato dopo il login live e fermato in `disconnect()`. Su session-error (anche forma con spazio "SESSION EXPIRED") instrada al re-auth **fail-closed bounded esistente** (`handle_session_expiry`); errore generico = soft-log + contatore (loop continua). Re-entrancy-safe: il re-auth parte dal thread di keepalive → `_stop_session_keepalive` non fa self-join (stop-event per-thread, start idempotente, restart dopo re-auth verificato). Un tick stale **non ri-autentica** se è in corso shutdown/switch-SIMULATION o se il client live non è più quello catturato. Niente keepalive in simulazione o a sessione invalida. Metriche via `keepalive_status()`. | B2 risolto |
| 1.3 ✅ | **`get_current_orders` + RPC `listCurrentOrders`**: `BetfairClient.get_current_orders(market_ids=None)` chiama `SportsAPING/v1.0/listCurrentOrders` (filtro `marketIds` opzionale) e ritorna la **lista** `currentOrders` — ogni errore API/sessione/rete **propaga** (no lista vuota silenziosa). `SimulationBroker.get_current_orders` rispetta lo stesso contratto list-of-dicts (riusa `list_current_orders`), così l'engine interroga uniformemente il broker attivo via `get_client()`. `BetfairService.list_current_orders()` è la facade di startup ghost-order: in **SIM** delega al broker; in **LIVE è fail-closed** — sessione invalida o client live mancante **sollevano** (mai `[]` silenzioso che verrebbe letto come "nessun ordine remoto"), un `SESSION_EXPIRED` durante il fetch instrada il re-auth bounded (`handle_session_expiry`) e poi **ri-solleva**; `state_recovery` tratta l'eccezione come "ghost reconciliation RICHIESTA ma NON completata". **Paginazione**: `get_current_orders` percorre tutte le pagine (`fromRecord`/`recordCount` finché `moreAvailable` è false) — una risposta troncata non viene mai restituita parziale (falserebbe il ghost-detect); superato il cap di pagine **solleva** (fail-closed). Una pagina vuota con `moreAvailable=True` (snapshot incoerente/stale) **solleva** invece di restituire un set parziale. Parità SIM/LIVE: `SimulationBroker.get_current_orders` esclude gli ordini terminali (`CANCELLED`/`LAPSED`/`VOIDED`/`EXPIRED`), che il `listCurrentOrders` live non ritorna. **Match-key fidelity**: i row del sim espongono `customerOrderRef` (l'engine indicizza prima per customer ref); `BetfairService.list_current_orders` normalizza i row dello startup-recovery aggiungendo `bet_id`/`order_id`/`customer_ref` snake_case (originali camelCase preservati) perché `state_recovery._is_missing_in_db` chiavea su snake — senza, un ordine live già persistito risulterebbe mancante a ogni riavvio. **Cancellazione ghost live**: `BetfairClient.cancel_order(bet_id, market_id=None)` (+ parità sim) risolve il `market_id` dai current orders e delega a `cancel_orders`; se il bet non è più corrente è no-op. Wira il path `_cancel_ghost_orders` dell'engine (usato con `ghost_order_action="CANCEL"`, non-default; il default resta `LOG_AND_FLAG`) senza toccare il file forbidden. | B3 / UFA-005 risolto |
| 1.4 ✅ | **Daily-loss breach → kill switch**: enforcement **SINCRONO** in `_on_close_position` — applicato il PnL, l'order-entry viene bloccato dal **pending-stop sincrono** (armato sotto lock prima dell'apply), poi gira il **bankroll-sync sul client live** (prima del flip a SIMULATION) e infine `_enforce_daily_loss_hard_stop` chiama `emergency_stop` (cancel-all + persist + lockdown, fail-closed) **prima della submission auto-trade**, così l'auto-trade è bloccato da `_risk_allows_auto_trade` e **nessun ordine (auto o segnale concorrente) parte dopo il breach**. Difese: **precheck** a inizio metodo (stato monitor cached) che fail-closa anche i rami di early-return (rejected/recovery-ambiguo); enforce su **ACTIVE o PAUSED** (un settlement in pausa ferma) + **`resume()` e `start()` rifiutano** in LIVE se breached same-day (un settlement live dopo `stop()` non si bypassa con un riavvio); atomico sotto lock (no doppio stop); `emergency_stop` robusto al fallimento dello status-snapshot (cancel-all anche in outage); persist sincrono al rilevamento. Round-5: il rifiuto LIVE di `start()` **sincronizza lo stato a SIMULATION** (`execution_mode`/`live_enabled`/`set_simulation_mode`, non resta live-capable); i rami di rifiuto di `start()`/`resume()` usano `_safe_status_snapshot` e **sopravvivono all'outage broker** (degradano su stato locale cached invece di propagare l'eccezione); il ramo recovery fail-closed usa la **proiezione read-only del PnL corrente** (`_projected_daily_loss_breach`) così un settlement che da solo sfonderebbe il limite ferma comunque. Round-6: la proiezione **onora il rollover di giornata** (ribasa la baseline come il monitor, niente falso breach al primo settlement del nuovo giorno); **pending-stop sincrono** armato sotto lock prima di applicare il PnL chiude la finestra in cui un `SIGNAL_RECEIVED` concorrente (dispatch multi-worker) piazzerebbe un ordine tra apply-PnL ed emergency_stop (il gate di `_on_signal_received` legge `_emergency_stopped` **o** pending sotto lock); il **bankroll sync gira prima del flip a SIMULATION** (client live, order-entry già bloccato dal pending-stop); un breach in ramo recovery mentre il runtime è **STOPPED viene persistito** nello stato del monitor così `start()`/`resume()` lo rifiutano invece di riprendere. Round-7: il read-modify-write su `_daily_loss_monitor_state` è **serializzato** (`_daily_loss_state_lock`, publish fuori dal lock per evitare deadlock sulla coda del bus) così un update concorrente non sovrascrive il breach persistito; il pending-stop è incluso nel **choke point `is_live_allowed()`** (che il `TradingEngine` interroga prima di eseguire un `CMD_QUICK_BET` già in coda) e in un **recheck immediatamente prima di ogni publish `CMD_QUICK_BET`** (segnale e auto-trade), così nemmeno un ordine già accodato o un segnale a metà validazione parte nella finestra di sync; il ramo recovery fail-closed **emette il `DAILY_LOSS_BREACH_TRIGGERED`** (una volta) così i subscriber non perdono il primo breach. Round-8: i recheck pre-submit **rilasciano le risorse** (duplication guard + `force_unlock` del tavolo) e scrivono un checkpoint `CYCLE_BLOCKED`/`NOT_ATTEMPTED` invece di lasciare tavolo/checkpoint appesi; un breach recovery in stato STOPPED fa `emergency_stop` **DUREVOLE** (persist su db, ricaricato fail-closed al riavvio). Round-9: il breach è persistito su db **prima dell'I/O di rete del bankroll-sync** (`_persist_db_emergency_marker`, senza mutare lo stato in-memory così il sync usa il client live) per sopravvivere a un crash nella finestra di sync; il recheck pre-submit del segnale gira **prima di `SIGNAL_APPROVED`** (niente ordine saltato registrato come approvato). **Follow-up tracciati** (richiedono modifiche fuori dall'allowlist o a `get_status`): hard-block lato `TradingEngine` sul pending (oggi demote a SIMULATION) e snapshot di lockdown non-mutante (evita l'overwrite del bankroll live dal broker simulato nello status post-stop). | B4 risolto |
| 1.5 ✅ | **`replace_orders`** nel client live + simulazione: `BetfairClient.replace_orders(market_id, bet_id, new_price)` chiama `SportsAPING/v1.0/replaceOrders` (`single_shot`, non idempotente → mai retry) e ritorna la response Betfair raw col **nuovo betId sollevato** da `placeInstructionReport` al top-level del report, così `order_manager.replace_order` legge `instructionReports[0]['betId']`; input validati (`INVALID_MARKET_ID`/`INVALID_BET_ID`/`INVALID_PRICE`), errori API/sessione/rete **propagati** → `order_manager` mappa a `REPLACE_REJECTED`. `SimulationBroker.replace_orders` per la parità SIM/LIVE (cancella il vecchio ordine, ne piazza uno nuovo a `new_price` con betId nuovo; ordine assente/non-EXECUTABLE/prezzo invalido → report `FAILURE`). Risolve la chiamata orfana di `order_manager.py:875` (B8). | B8 risolto |

## FASE 2 — Copy/mirror trading completo (2–3 giorni)

| Task | Descrizione | Note |
|---|---|---|
| 2.0 ✅ | **Prep cashout — normalizzazione broker in `OrderRouter`**: `place()` filtra i kwargs per la firma del broker attivo (fix `TypeError` sul client live, che non accetta i metadata audit `customer_ref`/`event_key`/`table_id`/`batch_id`/`event_name`/`market_name`/`runner_name`, tutti forwardati al sim) e normalizza il ritorno a `{ok, placed, order_unknown, status, matched, bet_id, error, raw}` deterministico/fail-closed: `placed` = ordine accettato/esiste (`bet_id` restituito → niente orphan/retry duplicato), `ok` = `placed and matched>0` (un LIMIT non abbinato è `placed=True, ok=False` su SIM **e** LIVE — parità), `order_unknown` = esito incerto da timeout/rete → `status=AMBIGUOUS`, mai fallimento da ritentare. È il seam unico consentito dai guardrail `no-bypass` (`.place_bet(` ammesso solo nei layer broker + `core/order_router.py`), quindi l'executor del cashout piazzerà via router. Broker `place_bet` non toccati. | Prerequisito 2.1 (B5); router prima sim-only/rotto per il live |
| **Stato cashout (B5)** 📌 | **Indice di avanzamento** (aggiornato a ogni PR): 2.0 ✅ · 2.1-A ✅ · B0 ✅ · B1 ✅ · **B2.1 ✅ · B2.2 ✅ · B2.3 ✅ · B2.4a ✅ · B2.4b-1 ✅ · B2.4b-2 ✅ · B2.5 ✅ · 2.1-C ✅**. **PR rimanenti per chiudere B5: 0** — resta solo il **deliverable finale** (spiegazione completa con esempi numerici reali + possibili aggiunte). **Il cashout è attivo end-to-end** (trigger → router → bridge → executor → residual handler), via Telegram nativo **e** copy-pattern (`action`) — **+ deliverable finale** (spiegazione completa con esempi numerici reali + possibili aggiunte di funzionalità). Nota: i moduli puri (resolver/router/adapter/identità) sono pronti ma **inerti** finché B2.4 non li cabla nel runtime. | Checklist sempre visibile |
| 2.1-A ✅ | **Cashout — executor + publisher**: `CashoutExecutor` consuma `CMD_EXECUTE_CASHOUT` (già pubblicato da `risk_middleware` su `REQ_EXECUTE_CASHOUT`), piazza il bet di green-up via `OrderRouter` normalizzato (2.0) e pubblica `CASHOUT_SUCCESS`/`CASHOUT_FAILED`. Fail-closed: `order_unknown` → AMBIGUOUS mai retry; `placed` ma `matched<=0` → UNMATCHED (green-up non bloccato, `bet_id` riportato); solo `matched>0` → SUCCESS. **Dormiente** finché non c'è il routing (2.1-B) o la UI → sicuro. | Executor + publisher (B5); zero file forbidden |
| 2.1-B0 ✅ | **Cashout — resolver posizioni A3 (puro)**. Scelta owner **(A3)**: la sorgente posizioni è una **query LIVE degli ordini correnti** al momento del cashout, **non** un tracker a eventi. (Le opzioni A1/A2 con `PnLEngine` sono state scartate: in produzione `order_manager` non è cablato nel `TradingEngine`, quindi `QUICK_BET_FILLED/PARTIAL` non vengono pubblicati → la `PnLEngine.snapshot()` resterebbe sempre vuota.) `cashout_resolver.py` (file nuovo, **zero file forbidden**, funzioni pure). `reconstruct_open_positions(current_orders, *, is_bot_order)` — **N2, fail-closed**: filtra agli ordini **del bot** (`is_bot_order` obbligatorio: il feed è account-level, niente bet manuali/altre strategie); gestisce solo posizioni a **lato singolo** (BACK *o* LAY: il caso copy-mirror), **salta** le selezioni **miste** (matched su entrambi i lati, es. post-cashout-parziale) per non rischiare un hedge sul lato sbagliato (il netting P&L-based è follow-up); espone `resting_remainder` (`sizeRemaining`) perché il routing cancelli l'ordine non abbinato prima del cashout; prezzo `averagePriceMatched` LIVE / `priceSize.price` SIM-proxy. `build_cashout_request(position, current_price, …)` calcola il green-up via `dutching.dynamic_cashout_single` e costruisce il payload `REQ_EXECUTE_CASHOUT` (lato opposto, fail-closed su math/commissione, rigetto se lo stake arrotonda a 0). 16 unit test. | Cuore di A3, testabile senza broker; il runtime (2.1-B) fornisce ordini+prezzi (B5) |
| 2.1-B1 ✅ | **Cashout — router orchestratore (puro)** (design A3 + identità DB **I1**). `cashout_router.py` (file nuovo, **zero file forbidden**, I/O iniettati → testabile senza broker). `CashoutRouter.route(signal)`: carica gli ordini **del bot dal DB** (identità I1: il `customerOrderRef` NON è inviato a Betfair sul piazzamento `betfair_client.py:608-630` → si filtra per `bet_id` ∈ record-del-bot; i record DB danno anche `market_id → event_name`) → `list_current_orders` (stato live abbinato) → `cashout_resolver` (solo bot, lato singolo, miste saltate) → per posizione: gate tradabilità **OPEN** (inline, SUSPENDED saltato best-effort), prezzo live al lato di chiusura, **cancel del resting** non abbinato (prima del cashout), pubblica `REQ_EXECUTE_CASHOUT`. `CASHOUT_ALL` = fan-out N; `CASHOUT` singolo = ristretto all'`event_name` (senza event_name → fail-closed, niente). Fail-closed: `list_current_orders` che solleva → `CASHOUT_FAILED`, abort. 11 unit test. | Cuore del routing, zero forbidden (B5) |
| 2.1-B2.1 ✅ | **Cashout — cancel adapter (puro)** (decisione owner **C1**). `cashout_cancel_adapter.py` (file nuovo, **zero file forbidden**, callable iniettati → testabile). `CashoutCancelAdapter.cancel(market_id, bet_ids)` adatta le firme **keyword-only** dei broker al contratto **posizionale** del router e instrada **sim/live** (`is_simulation()`): LIVE `BetfairClient.cancel_orders(*, market_id, bet_ids=...)`, SIM `SimulationBroker.cancel_orders(*, market_id, instructions=[{betId}…])`. `response_confirms_cancel()` normalizza l'esito a **bool** ispezionando `ok`/`status`/`instructionReports` sia top sia **annidati** sotto `result` (chiude il *cluster cancel* dei rilievi Codex su #300, L285 + risposte non confermate, **fuori** dal router puro). **Sicurezza**: `bet_ids` vuoto **non** raggiunge il broker (= cancel-all del mercato) → no-op `True`. Fail-closed: eccezione o risposta ambigua → `False` (router salta la posizione). `response_confirms_cancel` è **allowlist** (richiede prova positiva per-istruzione: `instructionReports` non vuoti e tutti OK; envelope `ok`/`status` senza report o shape inattesa → non confermato). 18 unit test. | Chiude il cluster cancel al confine giusto (B5) |
| 2.1-B2.2 ✅ | **Cashout — DB identità ordini bot (read-only)** (identità **I1**, design owner **X**). `database.py::get_bot_active_orders()` (additivo, **read-only**, deduplicato per `bet_id`): ritorna `{bet_id, market_id, event_name}` per ogni bet **del bot** viva, unendo i due ledger — **SIM** `simulation_bets` (status `EXECUTABLE`/`EXECUTION_COMPLETE`, `event_name` colonna) e **LIVE** tabella `orders` (`_live_bot_orders`): store **autoritativo** del percorso headless, che costruisce `TradingEngine` **senza** `OrderManager` e quindi NON scrive `order_saga` (Codex P1 — con `order_saga` il cashout live sarebbe stato inerte). `betId` dal `response_json` (`_order_bet_id`: chiave `bet_id`/`betId` di `OrderRouter`, **o** `result.instructionReports` del broker `place_bet`, **o** `raw.instructionReports`), `market_id`/`event_name` dal `payload_json` (request valorizzata da `risk_middleware`). **Denylist** di stati (non allowlist, che ometteva casi — Codex P1/P2): SIM esclude solo `CANCELLED` (include `EXECUTABLE`/`EXECUTION_COMPLETE`/`PARTIAL`); LIVE esclude solo `INFLIGHT` pre-piazzamento (include `MATCHED`/`PARTIALLY_MATCHED`/`COMPLETED`/… — l'identità reale è il `betId` estratto). La liveness è ri-verificata dal router contro `list_current_orders` (le righe terminali stale sono filtrate a valle). Il `customerOrderRef` NON è inviato a Betfair → l'identità è il `bet_id` registrato; liveness reale ri-verificata dal router contro `list_current_orders` (bet_id stale innocui, univoci su Betfair). Niente fallback su `event_key` (slug di deduplica che romperebbe il match del CASHOUT singolo, Greptile P1). 11 unit test. **Nota sequencing**: le **costanti** `REQ/CMD_EXECUTE_CASHOUT`/`CASHOUT_SUCCESS/FAILED` si spostano in **B2.4** (definite **e** consumate nello stesso PR dal wiring → niente codice morto, hard-verify §9). | Prerequisito I1, additivo read-only (B5) |
| 2.1-B2.3 ✅ | **Cashout — enrich `event_name` nel parse del segnale**. `telegram_listener._parse_cashout_signal` ora include `event_name = self._extract_event_name(text)` nell'output sia per `CASHOUT` sia per `CASHOUT_ALL` (parse-only, nessun cambio al runtime del listener). Serve al routing del **CASHOUT singolo** per restringere la chiusura alla partita; `''` se non estraibile → il router fa **fail-closed** (salta il singolo per quel mercato), il `CASHOUT_ALL` lo ignora. 6 unit test (event_name da `🆚`, da riga `vs`, da label `Partita:`/`Match:` multi-riga, `CASHOUT_ALL` con key vuota, singolo senza nome → `''`, testo non-cashout → None). | Sblocca il CASHOUT singolo ristretto alla partita (B5) |
| 2.1-B2.4a ✅ | **Cashout — flatten resting puro (L147) + depth/partial-fill (L95)** (puro, `cashout_router`). **L95** (policy owner **R1**): `_closing_price` ritorna `(prezzo, size)` e `_close_position` **rifiuta** il cashout se lo stake dell'hedge supera la size disponibile al prezzo eseguibile (`stake > size` → skip fail-closed; size 0/ignota = nessuna profondità confermata → reject) — meglio non chiudere che abbinarsi a metà lasciando esposizione. **L147**: dopo le posizioni, `_flatten_pure_resting` cancella (con conferma) gli ordini bot **del tutto** non abbinati (`sizeMatched==0`, `sizeRemaining>0`) che non formano una posizione — `_in_scope_markets`: tutti i mercati per `CASHOUT_ALL`, solo quelli della partita per il `CASHOUT` singolo — altrimenti si abbinerebbero **dopo** il cashout del master riaprendo esposizione. 32 unit test (era 23). | Sicurezza chiusura: niente partial silenzioso, niente resting orfano (B5) |
| 2.1-B2.4b-1 ✅ | **Cashout — catena di esecuzione (dormiente)** (decisione owner **A hardening**: bridge cashout-only, **non** la `RiskMiddleware` completa). `cashout_request_bridge.py` (file nuovo): `CashoutRequestBridge` sottoscrive **solo** `REQ_EXECUTE_CASHOUT`, fa **dedup** (anti double-click/replay, finestra 2s) + **normalizzazione** + **validazione minima** fail-closed e pubblica `CMD_EXECUTE_CASHOUT`. Su payload invalido pubblica un **`CASHOUT_FAILED` strutturato** (dict `status=REJECTED`, stessa forma di `CashoutExecutor._fail` → chiude il residuo 2.1-A (b) «uniformare lo shape di CASHOUT_FAILED»), **non** una stringa. `side` **mai defaultato** a LAY (mancante/invalido → reject: defaultarlo aumenterebbe l'esposizione invece di chiuderla). Wiring in `headless_main._wire_cashout_execution_chain()`: costruisce `OrderRouter` (seam sim/live mode-aware) + `CashoutExecutor` (`wire()` su `CMD_EXECUTE_CASHOUT`) + `CashoutRequestBridge` (`wire()` su `REQ_EXECUTE_CASHOUT`). **Perché un bridge dedicato e non la `RiskMiddleware`**: quella è il bridge centrale di TUTTI gli order type (QUICK_BET/DUTCHING/CANCEL/REPLACE oltre a CASHOUT, oggi serviti da `_NullRiskMiddleware`): cablarla avrebbe blast radius inaccettabile sugli altri flussi. **Dormiente by-design**: nessun componente qui **emette** `REQ_EXECUTE_CASHOUT` → catena inerte, nessun ordine reale parte finché B2.4b-2 non aggiunge il trigger. 16 unit test (forward+normalizzazione, 7 reject strutturati incl. side-non-defaultato, dedup, **altri order type non toccati**, wiring dormiente headless). Forbidden registrato: `headless_main.py`. | Catena REQ→CMD→placement pronta e testabile, **inerte** finché B2.4b-2 (B5) |
| 2.1-B2.4b-2 ✅ | **Cashout — trigger runtime + gestione residuo (attiva il cashout)**. Branch in `core/runtime_controller._on_signal_received` per `signal_type ∈ {CASHOUT, CASHOUT_ALL}` **dopo** `_runtime_active` (quindi a valle di emergency-stop incl. daily-loss pending, session-guard LIVE, deploy-gate → **E1 gratis**), **prima** del check campi-obbligatori (che scarterebbe `CASHOUT_ALL` come `campi_mancanti`). `_route_cashout_signal` costruisce `CashoutRouter` (B1) coi servizi reali (`betfair_service.list_current_orders`/`get_market_book_snapshot`, `db.get_bot_active_orders`, `bus.publish`, `config.commission_pct`) + `CashoutCancelAdapter` (B2.1, sim/live via `is_simulation_mode`); il runtime pubblica **solo** `REQ_EXECUTE_CASHOUT` (mai `CMD_*` diretto), fail-closed (errore costruzione/routing → `CASHOUT_FAILED` strutturato). **Costanti** `REQ/CMD_EXECUTE_CASHOUT`/`CASHOUT_SUCCESS/FAILED` centralizzate in `core/trading_constants.py` (i 3 moduli cashout — executor/bridge/router — le importano da lì: single-source, no drift). **`CashoutResidualHandler`** (file nuovo, **zero forbidden**, I/O iniettati) consuma `CASHOUT_FAILED` split-per-status (decisione owner): **UNMATCHED** ⇒ un solo cancel sicuro del resting via adapter (mai retry) + persist record con esito (`cancel_attempted`/`cancel_ok`/`cancel_error`) + notify; **AMBIGUOUS** ⇒ **no-cancel** + persist reconciliation + notify alta severità; **ERROR/REJECTED/FAILURE** ⇒ notify + persist diagnostico se c'è contesto. Cancel limitato **esclusivamente** al cashout UNMATCHED (mai QUICK_BET/dutching/cancel/replace). Persist su `audit_events` (`db.insert_audit_event`, zero migrazioni), notify Telegram via `telegram_service` (non il global non inizializzato). Cablato in `headless_main._wire_cashout_execution_chain`. **Guard anti-silent-drop** (decisione owner): il trigger è in `RuntimeController` (ogni entrypoint) ma la catena è cablata solo in `HeadlessApp` (go-live); in `mini_gui`/dev il CASHOUT viene **rifiutato visibilmente** (`SIGNAL_REJECTED` reason `cashout_chain_not_wired`, nessun `REQ` pubblicato, nessun side-effect broker) invece di cadere in silenzio — rilevamento via subscriber su `REQ_EXECUTE_CASHOUT`, fail-closed. Follow-up separato: valutare wiring comune/in `RuntimeController`. Unit test (trigger incl. guard, residual, bridge, notify). Forbidden registrati: `core/runtime_controller.py`, `core/trading_constants.py`, `headless_main.py`, `cashout_router.py`, `cashout_executor.py`, `cashout_request_bridge.py`. | Accende il cashout end-to-end (B5) |
| 2.1-B2.5 ✅ | **Cashout — residui 2.1-A** (3 fix stretti). **(1) Shape `CASHOUT_FAILED`**: `core/risk_middleware._handle_cashout` su payload invalido pubblicava una **stringa**; ora pubblica un **dict strutturato** (`status=REJECTED`, `reason`, `bet_id=None`, `matched=0.0`, `market_id`, `selection_id`) uniforme a `CashoutRequestBridge`/`CashoutExecutor` → il `CashoutResidualHandler` legge i campi in modo coerente. **(2) `selection_id>0`**: `_handle_cashout` rifiuta `selection_id` non-positivo (`int(...)<=0` → CASHOUT_FAILED), non solo non-intero. **(3) Sim-broadcast guard**: `telegram_sender._on_cashout_success` ora salta il broadcast `MASTER_CASHOUT` ai follower se `data["sim"]` (parità con `_on_quick_bet_success`/`_on_dutching_success`); il flag `sim` è valorizzato da `CashoutExecutor` nel `CASHOUT_SUCCESS`, derivato dal broker attivo via `OrderRouter.service.is_simulation_mode()` (difensivo, default False=live). Nota: il default-LAY del `side` in `RiskMiddleware` resta fuori scope (modulo servito da `_NullRiskMiddleware` in produzione; il path attivo è il bridge che già rifiuta il side mancante). 11 unit test nuovi (4 risk_middleware, 4 telegram_sender, 2 executor sim, +). Forbidden registrati: `core/risk_middleware.py`, `telegram_sender.py`. | Chiude i residui 2.1-A (B5) |
| 2.1-B2 | **Cashout nativo — wiring del router** (i file forbidden). Decomposta (staging puro→forbidden): **B2.1** cancel adapter ✅ · **B2.2** metodo DB read-only `get_bot_active_orders()` ✅ · **B2.3** enrich `event_name` in `telegram_listener` ✅ · **B2.4a** flatten L147 + depth L95 (puro) ✅ · **B2.4b** **costanti** `core/trading_constants.py` + wiring core (`runtime_controller._on_signal_received` + `headless_main`) · **B2.5** residui 2.1-A (consumer `CASHOUT_FAILED`+notifica, shape `risk_middleware`, sim-broadcast guard, selection_id>0). Cabla `CashoutRouter` (2.1-B1): branch in `runtime_controller._on_signal_received` per `signal_type ∈ {CASHOUT, CASHOUT_ALL}` **PRIMA** del check campi-obbligatori (oggi scarterebbe `CASHOUT_ALL` come `campi_mancanti`); fornisce i servizi reali (`betfair_service.list_current_orders`/`get_market_book_snapshot`/`cancel_orders`, `bus.publish`, `self.config.commission_pct`) + un **metodo DB read-only** per gli ordini-del-bot attivi (identità I1: `bet_id`+`market_id`+`event_name`). Enrich `telegram_listener._parse_cashout_signal` con `event_name = self._extract_event_name(text)`; **costanti** `REQ/CMD_EXECUTE_CASHOUT`/`CASHOUT_SUCCESS/FAILED` in `core/trading_constants.py`; wiring `CashoutExecutor` in `headless_main`. `CASHOUT_ALL` → **fan-out N** (1 `REQ_EXECUTE_CASHOUT` per posizione aperta, `side` invertito BACK↔LAY, `green_up` sulla quota **live di ogni** mercato). `CASHOUT` singolo → **ristretto alla partita** (chiude **tutte** le posizioni di quell'`event_name` — decisione owner, superset del «1 REQ» della issue). **Costanti** `REQ/CMD_EXECUTE_CASHOUT`, `CASHOUT_SUCCESS/FAILED` in `core/trading_constants.py` (oggi stringhe hard-coded). **Gate tradabilità per-posizione** `_is_tradable_market_book` (`TRADABLE_MARKET_STATUS="OPEN"`): chiudi solo mercati OPEN (pre-match-OPEN **e** in-play-OPEN); **SUSPENDED non aborta il batch** → best-effort + retry/deferral. Enrich `telegram_listener` con `event_name`; prezzo live via `betfair_service.get_market_book_snapshot`; wiring executor in `headless_main`. **+ Deferred da 2.1-A** (#296): (a) **consumer `CASHOUT_FAILED`** + notifica operatore (AMBIGUOUS/UNMATCHED/ERROR oggi silenziosi); (b) **shape dict** di `CASHOUT_FAILED` anche in `RiskMiddleware` (oggi stringa); (c) **lifecycle ordine non-abbinato/ambiguo** (cancel o persist/reconcile). Phase 0: risoluzione posizione singola con più batch/`event_key`, idempotenza `_is_duplicate` (N cashout legittimi ≠ duplicati), atomicità best-effort. Forbidden da registrare: `core/runtime_controller.py`, `core/trading_constants.py`, `telegram_listener.py`, `telegram_sender.py`, `core/risk_middleware.py`, `headless_main.py` | Senza questo il copy parziale è più pericoloso di nessun copy (B5) |
| 2.1-C ✅ | **Copy-pattern (regex) — azione cashout** (Opzione A, colonna). Colonna **`action TEXT NOT NULL DEFAULT 'QUICK_BET'`** in `signal_patterns` (`database_schema.py`) con **migrazione idempotente** in `database._init_db` (`PRAGMA table_info` + `ALTER ADD COLUMN` se mancante → DB esistenti non si rompono; default = comportamento storico). `action` propagato in `get_signal_patterns`/`save_signal_pattern`/`update_signal_pattern`, normalizzato a `{QUICK_BET, CASHOUT, CASHOUT_ALL}` (valore ignoto → `QUICK_BET`, fail-safe: un pattern malformato resta una quick bet, non innesca cashout). In `telegram_listener._parse_custom_patterns`: dopo i filtri del pattern (minuto/score/live_only), un `action=CASHOUT_ALL` **non** costruisce la bet ma ritorna lo **stesso** `signal_type` del cashout nativo (`{signal_type, event_name, raw_text, pattern_id, pattern_label}`) → confluisce in `parse_signal`→`SIGNAL_RECEIVED`→`_on_signal_received`→**routing cashout 2.1-B** (un solo percorso d'esecuzione). **`action=CASHOUT` singolo è fail-closed (scelta owner, Codex+Greptile P1)**: confluirebbe nel routing event-level che chiude **tutte** le posizioni della partita (non la selezione configurata) → viene **skippato visibilmente** (`logger.warning` reason `copy_pattern_cashout_single_requires_targeting`), nessun segnale emesso, nessun side-effect broker. Il targeting per-selezione/market è un **follow-up separato** (estende `CashoutRouter`/resolver). 12 unit test (DB roundtrip/default/normalizzazione/migrazione legacy + listener CASHOUT/CASHOUT_ALL/QUICK_BET/absent). Forbidden registrati: `database.py`, `database_schema.py`, `telegram_listener.py`. | Cashout anche da copy-pattern, percorso unificato (B5) |
| 2.1-C (orig) | **Copy-pattern (regex) — azione cashout** (spec issue #295, cap.2). Aggiungere il campo **`action`** (`QUICK_BET` default / `CASHOUT` / `CASHOUT_ALL`) ai `signal_patterns` (oggi nessuna colonna azione: `database_schema.py:63-77`): **Opzione A (preferita)** colonna `action TEXT DEFAULT 'QUICK_BET'` con migrazione in `database_schema.py` + propagazione in `database.py` (`get_signal_patterns:532`/`add_signal_pattern:569`); **Opzione B** chiave in `extra_json` (decidere in Phase 0). In `_match_copy_pattern` (`telegram_listener.py:520-637`): se `action ∈ {CASHOUT, CASHOUT_ALL}` **non** costruire la bet ma emettere lo **stesso** `signal_type` del cashout nativo → confluisce nel **medesimo routing** di 2.1-B (un solo percorso d'esecuzione). Per `CASHOUT` singolo `market_type`/`selection_template` identificano la posizione/partita; valgono le stesse 3 regole pre-match/in-play. Forbidden da registrare: `telegram_listener.py`, `database.py`, `database_schema.py` | Dipende da 2.1-B (routing unificato) — B5 |
| 2.2 | **Best price su percorso DIRECT** (B6, decomposto, default-OFF). **B6.1 ✅** estrattore PURO `direct_best_price.resolve_direct_best_price(market_book, selection_id, side, master_price, max_deviation_pct)` (file nuovo, **zero forbidden**, niente broker/bus/runtime): best price **difensivo** — per BACK solo `availableToBack`, per LAY solo `availableToLay`, **mai cross-spread** (a differenza del resolver `aggressive_best_price`); tolleranza vs master (oltre → fallback); **fail-closed** su book assente / mercato non-OPEN (anche `marketDefinition.status`) / runner mancante o **non esplicitamente ACTIVE** (status assente ⇒ fallback, severità pari al market status) / lato senza liquidità (size>0) / prezzo non finito o ≤1.0 / **tolleranza malformata** (None/stringa non numerica ⇒ `invalid_tolerance`, non 0.0 silenzioso) / **selection_id frazionario o bool** (no troncamento `55.9`→`55`) → sempre fallback al `master_price`, mai prezzo inventato. Schema malformato (tipi inattesi) ⇒ fallback, mai eccezione. 25 unit test (marker `unit`). **B6.2 ✅** wiring sul DIRECT dietro **flag globale default-OFF** `use_best_price_direct` (letto via `getattr(self.config, "use_best_price_direct", False)`, **niente edit della config class**). Seam in `RuntimeController._on_signal_received`: metodo `_apply_direct_best_price(payload)` eseguito **PRIMA dei gate finali pre-submit**, che vengono **RIESEGUITI dopo lo snapshot** (Codex P2: il fetch è lento e in LIVE può invalidare la sessione, quindi non deve aggirare i gate). Ordine: snapshot → (a) session-guard LIVE re-check (solo se snapshot tentato) → (b) recheck daily-loss/emergency → solo allora `SIGNAL_APPROVED` e `CMD_QUICK_BET`. Se dopo lo snapshot la sessione è invalida o daily-loss/emergency è pending: niente `SIGNAL_APPROVED`/`CMD_QUICK_BET`, `SIGNAL_REJECTED` (`*:pre_submit_recheck`), risorse rilasciate (`_release_acquired_and_reject`). **Dormiente**: `_apply_direct_best_price` ritorna `False` con flag OFF ⇒ no-op assoluto (nessun fetch, nessuna mutazione payload, nessun gate nuovo, byte-identico a oggi); `True` con flag ON ⇒ il chiamante riesegue i gate. Flag ON ⇒ `betfair_service.get_market_book_snapshot(market_id)` (gia' fail-closed) → `resolve_direct_best_price(...)` (B6.1) → override `payload['price']` col best difensivo entro tolleranza, altrimenti master; audit leggero `best_price_source`/`best_price_reason` (override→INFO, fallback→DEBUG); tolleranza via `getattr(config, "best_price_max_deviation_pct", 2.0)`. Tutto il body in try/except ⇒ mai un crash sul percorso d'ordine. 11 unit test (7 estrattore-in-seam + 4 ordine-gate). **⚠️ B6.2 NON è LIVE-ready**: dormiente fino alla PR di attivazione che risolve i deferred sotto. **Attivazione LIVE (decomposta in 2 PR, flag resta default-OFF)**: **PR-1 ✅ priceProjection** — `betfair_client.get_market_book` e `betfair_service.get_market_book_snapshot` hanno un param opt-in `include_prices` (default `False` = invariato per i chiamanti esistenti); `_apply_direct_best_price` chiama con `include_prices=True` ⇒ `listMarketBook` chiede le ladder `EX_BEST_OFFERS` (senza, in LIVE il book tornava senza ladder e il best-price faceva sempre fallback al master, Codex P2 #2). In SIMULATION il flag è ininfluente (broker già completo). **PR-2 ⬜ propagazione audit-record** di `best_price_source`/`best_price_reason` — `trading_engine._normalize_quick_bet` copia solo chiavi note, oggi l'audit dell'override vive solo nel log (Greptile P2); (3) per-signal `use_best_price` (gia' in `safety_layer`/`risk_middleware`, morto) come secondo livello sopra il kill-switch globale. **B6.3 ⬜** TTL/cancel ordini non abbinati (poller, no retry, persist/notify). | B6 |
| 2.3 | **Stake mode**: aggiungere `master_stake` al messaggio broadcast; selettore FISSO / 100% MASTER / MM nelle impostazioni; runtime che rispetta il mode (MM continua a validare i limiti di esposizione) | B7 |
| 2.4 | **Micro-stake**: `sizeReduction` su cancel + orchestrazione (piazza a quota non abbinabile → riduci → replace quota) + regola payout-minimo per saltare il trucco a quote alte | ⚠️ uso sistematico sotto-minimo = violazione T&C Betfair; usare solo per fase validazione/green-up |

## FASE 3 — Hardening (1–2 giorni)

| Task | Descrizione |
|---|---|
| 3.1 | Guard fail-closed: rifiuto payload con `simulated: True` nel percorso LIVE (UFA-016) |
| 3.2 | Blocker deploy gate per streaming health, bankroll sync, reconciliation health (UFA-012) |
| 3.3 | Topic `RECONCILE_NOW`/`RECOVER_PENDING`: collegare handler reali o rinominarli (UFA-014) |
| 3.4 | Decisione esplicita su cifratura `username` (UFA-004, parte valida) |
| 3.5 | Backup DB automatizzato: cron + `scripts/db_restore_validate.py` (B10) |
| 3.6 | ~~Enforcement dei nuovi item del gate micro-stake in `LIVE_MICRO_REQUIRED_CHECKS`~~ — già fatto in questa PR (`scripts/live_gate.py` + test aggiornati) |

## FASE 4 — Deploy su VPS Windows (2 giorni)

> Decisione: deploy su **VPS Windows** con app desktop (opzione B). L'operatore usa
> solo Windows: configurazione e monitoraggio via GUI in Desktop Remoto, alert via
> Telegram. Il PC di casa NON è un host valido per il 24/7 (update/sospensioni);
> l'eventuale migrazione futura a Linux headless resta banale (stesso codice).

| Task | Descrizione |
|---|---|
| 4.1 | Dichiarare `customtkinter` in un `requirements-gui.txt` dedicato (oggi importata dalla GUI ma non dichiarata; tenerla fuori dai requirements core) |
| 4.2 | Packaging PyInstaller: `Pickfair.exe` (GUI) + modalità headless avviabile come servizio |
| 4.3 | Servizio Windows via NSSM (o Task Scheduler all'avvio) con riavvio automatico, equivalente di systemd `Restart=always`; variabili d'ambiente del servizio configurate via `nssm set <servizio> AppEnvironmentExtra` |
| 4.4 | Protezione chiave su Windows: preferire ACL NTFS su directory dedicata (`icacls`); in alternativa `PICKFAIR_SECRET_KEY` come variabile d'ambiente, sapendo che è leggibile da ogni processo dello stesso utente (il `chmod 0600` di `~/.pickfair/db.key` è quasi no-op su NTFS) |
| 4.5 | Hardening VPS: sospensione disattivata, Windows Update con orari attivi + riavvio programmato seguito da auto-start dell'app |
| 4.6 | Credenziali nel DB: Betfair live (app key attivata, certificato registrato sull'account) e Telegram (api_id/api_hash/session) |
| 4.7 | Suite test eseguita su Windows + shutdown pulito via servizio: mappare l'handler di `headless_main.py:804` su SIGINT/SIGBREAK, perché SIGTERM non viene consegnato su Windows |
| 4.8 | Evidence leggera: script che genera artifact di readiness (commit, env, esito test, config hard-stop) + checklist firmata — versione snella di UFA-010/011 |

## FASE 5 — Validazione progressiva (calendario, bot autonomo)

| Step | Durata | Uscita |
|---|---|---|
| 5.1 Paper compresso: SIMULATION con canale segnali reale | 2–3 giorni | N segnali reali parsati/risolti correttamente → spunta `ops/paper_trading_gate.md` |
| 5.2 Micro-stake live: stake minimo, hard-stop giornaliero basso, `auto_bet` attivo | fino a ~20–30 scommesse validate (max 2 settimane); il minimo di transazioni necessario, NON una durata fissa — l'uso prolungato del sotto-minimo aumenta il rischio di flag sull'account | riconciliazione pulita, zero incidenti → spunta `ops/live_microstake_gate.md` |
| 5.3 Ramp-up: alzare gradualmente i limiti MM | — | operatività a regime |

## Audit matematica Exchange (giugno 2026) — esiti e test mancanti

Triplo audit read-only (commissione 4.5%, dutching BACK/LAY, hedge/green-up)
eseguito su richiesta dell'owner contro la matematica di riferimento.

**Verdetti** (evidenza file:riga negli audit di sessione):

| Dominio | Esito | Note |
|---|---|---|
| Commissione 4.5% | ✅ CORRETTA | vedi nota sotto |
| Dutching BACK | ✅ CORRETTO | Formula ≡ riferimento `stake_i=(B/oᵢ)/S`; spread post-rounding ≤0.15 testato; dutch non profittevole riportato onestamente |
| Dutching LAY | ✅ CORRETTO | Modello equal-profit esplicito; liability `stake×(odds−1)`; worst-case esposta dal controller |
| Hedge/green-up | ✅ formule corrette | vedi nota sotto |

Nota commissione: tasso centralizzato fail-closed (`trading_config.py`);
solo su vincite nette positive; mai doppia applicazione; netting per
MERCATO via `MarketNetRealizedSettlementAggregator`.
Nota hedge: prezzo medio ponderato, chiusura parziale onesta,
realized/unrealized separati, no doppia realizzazione
(`core/position_ledger.py`); live e sim condividono gli stessi componenti.

**Limite architetturale noto (MEDIUM)**: il green-up è helper/preview-only —
il runtime non dichiara mai uno stato "posizione green/hedged" autoritativo.
Accettabile oggi; serve per automazione hedge intelligente (vedi 2.x).

### Test matematici mancanti (in ordine di priorità)

1. **Parity live/sim commissione end-to-end** (unico gap vero della commissione)
2. **Parity settlement LAY** (`test_dutching_realized_settlement_parity` esiste solo per BACK)
3. **Worst-case liability LAY mai sottostimata** (`max(liability_i)`)
4. **Idempotenza commissione su retry settlement** (stesso market/correlation 2× → mai doppia)
5. **Chiusura parziale multi-fill (3+ leg)** con prezzo medio residuo ponderato
6. **Stress dutching su N grandi** (20-100 esiti stile risultato esatto)
7. **Idempotenza equalizzazione rounding** (equalize 2× → nessun drift)
8. **Precisione Decimal su stake estremi** + refund commissione esatto su +X/−X
9. **Integrazione tick ladder Betfair nel dutching** (oggi rounding a centesimi, preview-only: documentato)
10. **Smoke integrazione: settlement rifiutato su basis ≠ market_net_realized**
11. **Segregazione ledger commissioni per market_id** (no crosstalk)

### Gap E2E (da analisi formato canale + checklist owner)

- **Real-money guard esplicito** negli E2E (env/flag → RuntimeError, indipendente da safe mode)
- **Limiti quota minima / stake massimo come flusso E2E** (oggi coperti solo a livello unit/MM)
- ✅ Catena completa messaggio→ordine simulato→DB: aggiunta in PR #263

### Proof operazionali deterministici (audit Phase 0 — giugno 2026)

Audit total-control su dove estendere le suite di prova operazionale
(restart equivalence, disconnect storm, stale su tempo simulato,
cooldown/lockout, no false-healthy) oltre Telegram — che dopo la Fase 1.1
è il sottosistema meglio provato (17 test runtime + race/timeout/lock).

Ordine sicuro (PR piccole, solo test, zero modifiche alle autorità):

1. **StreamingFeed** (`services/streaming_feed.py`): base esistente (18 unit
   + soak chaos) ma mancano: equivalenza storm di riconnessione, stale su
   tempo simulato lungo, budget degradazione auth — miglior rapporto
   valore/rischio CI (non mappato dal routing dinamico → blast contenuto)
   - ✅ **FATTA (PR proof_streaming_feed)** — `tests/unit/test_streaming_feed_operational_proofs.py`
     (proof deterministiche netto-nuove a funzione pura, dedup sui ~19
     esistenti): heartbeat-dead (no-message non morto, confine == timeout,
     floor 1.0s su clock finto); degradazione 503 con `healthy=False` e flag
     persistente fino al reconnect; subscribe kwargs che OMETTONO clk vuoto e
     lo includono dopo cattura; isolamento dell'eccezione nella callback
     on_disconnect (connected=False, no propagazione). Solo test, nessuna
     modifica a `services/streaming_feed.py`.
   - ✅ **ESTESA** (idempotenza + stale lungo, scelta owner anti-padding): doppio
     `start()` non crea un secondo thread (`already_running`, stesso thread);
     `stop()` idempotente e safe anche senza start; start→stop rapido pulisce e
     consente il restart; **stale su tempo simulato lungo** — jump di +100s su
     clock finto rende il feed morto, un messaggio fresco ripristina la freschezza
     (`_run_loop` reso no-op che attende lo stop → niente thread in loop/sleep
     reali). I gap flaky (recovery parziale del counter auth) restano coperti dal
     chaos soak, non duplicati.
2. **EventBus** (`core/event_bus.py`): drain vs lossy shutdown, isolamento
   subscriber avvelenato, metriche di pressione sotto carico
   - ✅ **FATTA (PR proof_eventbus)** — `tests/unit/test_event_bus_operational_proofs.py`
     (5 proof deterministiche netto-nuove, dedup su 35 test esistenti):
     ordering FIFO single-worker, re-entrancy/cascade senza deadlock,
     consistenza contatori `delivered+errori == enqueued == dequeued` dopo
     drain (anti-perdita-silenziosa), unsubscribe idempotente, identità del
     payload. Solo test, nessuna modifica a `core/event_bus.py`.
3. **BetfairService session gate**: re-auth bounded, fail-closed su sessione
   invalida (integra i 17 test di `test_session_expiry_recovery.py`)
   - ✅ **FATTA (PR proof_session_gate)** — `tests/unit/test_session_gate_fail_closed_proofs.py`
     (6 proof fail-closed netto-nuove, dedup sui 17 esistenti): load_password
     che solleva (no crash/no fail-open) e che ritorna None; confine password
     whitespace (passata, non "mancante"); ritorno sim<->live con live ancora
     rifiutato; place_order su invalid che NON re-invoca il recovery; ramo
     eccezione di place_order (SESSION_EXPIRED sollevato -> recovery + re-raise).
     Solo test, nessuna modifica a `services/betfair_service.py`.
4. **RuntimeController control-path** (solo dopo 1-3): start/stop/pause/
   emergency non bloccanti — alta autorità, va toccato per ultimo
   - ✅ **FATTA (PR proof_runtime_controller)** — `tests/unit/test_runtime_controller_gate_proofs.py`
     (6 proof fail-closed deterministiche netto-nuove, dedup sui 13+ file
     esistenti e sulla matrice `assert_live_gate_or_refuse`): is_live_allowed
     con solo `live_enabled=False`; is_live_allowed fail-closed se il deploy
     gate SOLLEVA (+ effective mode→SIMULATION); rifiuto segnale LIVE con reason
     `deploy_gate_no_go:` e `session_invalid_live_blocked`; `_risk_allows_auto_trade`
     negato con `runtime_not_active`/`desk_lockdown` e approvato in stato normale.
     Solo test, nessuna modifica a `core/runtime_controller.py`/`core/safety_layer.py`.

Vietato senza prova di necessità: `core/trading_engine.py`,
`core/runtime_controller.py`, `core/reconciliation_engine.py`,
`database.py`, `betfair_client.py`, `observability/watchdog_service.py`.
Recovery/reconciliation: copertura GIÀ FORTE (26 file di test dedicati:
crash mid-order, saga replay, dedup post-restart, ghost detection,
fencing) — non duplicare evidenza.

## Programma test "hedge-fund grade" (richiesta owner, giugno 2026)

Obiettivo: provare che il sistema NON perde soldi negli stati strani
(duplicati, crash, rete lenta, Telegram doppio, Betfair ambiguo, live gate
sbagliato, emergency stop, reconciliation post-riavvio). Le proprietà chiave:

- 1 segnale valido → max 1 ordine; segnale ambiguo → 0 ordini o AMBIGUOUS
- timeout Betfair → mai doppia bet; Telegram duplicato → mai doppia bet
- DB crash → recovery coerente; emergency stop → LIVE impossibile
- readiness incerta / secret mancante / audit mancante → fail-closed

NOTA DEDUP (dagli audit di questa sessione): molte aree sono GIÀ coperte —
duplication guard atomico, reconciliation matrix (12 file), recovery (14
file), commissione/dutching/PnL (audit matematico STRONG), lifecycle
Telegram (17 test). Ogni PR del programma DEVE verificare la copertura
esistente prima di scrivere: si aggiunge solo signal nuovo.

### Ordine PR (serie A-M, una alla volta)

Nota: le lettere J e K non sono usate (numerazione all'italiana del
piano originale dell'owner: A-I poi L, M). Nessuna PR mancante.

| PR | Suite | Note dedup |
|---|---|---|
| A | **Live gate fail-closed matrix**: execution_mode mancante/invalido→SIM; LIVE bloccato senza live_enabled/readiness/kill-switch/key-source/hard-stop; stato contraddittorio→blocca | `assert_live_gate_or_refuse` + deploy gate runtime esistono: testare la MATRICE completa |
| B | **Order lifecycle FSM hard**: transizioni vietate (COMPLETED→altro, FAILED→COMPLETED), AMBIGUOUS senza reason, doppia finalize | TradingEngine ha già invariants: provarli a matrice |
| C | **Duplicate/concurrency storm**: 100 thread stesso event_key→1 acquire; payload senza key→blocca; TTL; seed post-restart | acquire atomico già testato: aggiungere storm + edge |
| D | **Reconciliation crash matrix**: INFLIGHT vs Betfair MATCHED/assente/persa; audit persist fallisce→fail-closed; batch parallelo→uno solo | 12 file esistenti: SOLO i buchi della matrice |
| E | **Money management caps + malformed**: cap 25%, quota≤1→0, lockdown→0, DEFENSE ridotto, esposizione piena→blocca, NaN/inf/stringhe→safe | include TOP30 #22-24 |
| F | **Betfair network ambiguity**: timeout placeOrders→AMBIGUOUS mai SUCCESS, response persa→reconciliation, circuit breaker, session expired | FATTA. Dedup: timeout→AMBIGUOUS, breaker wiring e session expiry erano già coperti. **2 BUG REALI trovati e fixati (autorizzati dall'owner)**: (1) `_post_jsonrpc` ritentava placeOrders su timeout/errore rete (fino a 3 POST, no customerRef → rischio doppia/tripla bet reale; il DuplicationGuard non protegge i retry HTTP) → placeOrders ora SINGLE-SHOT; (2) `order_unknown` era True solo per TIMEOUT: connection reset/HTTP 5xx dopo l'invio finivano FAILED locale senza reconciliation (ordine fantasma) → esteso a NETWORK_ERROR/HTTP_5xx + wiring in `_raise_if_failed_semantic_response` (order_unknown→AMBIGUOUS+reconcile). Gap residuo minore: ciclo recovery breaker OPEN→HALF_OPEN→CLOSED (rimandato, basso rischio) |
| G | **Telegram E2E sim-only + live (secrets)**: stale message→blocca, canale non autorizzato→blocca, flood 100 msg | FATTA. **2 GAP REALI trovati e fixati (autorizzati dall'owner)**: (1) nessun controllo anti-stale su `event.message.date` — il backlog post-reconnect avrebbe piazzato bet su partite già cambiate → guardia `max_message_age_seconds` (default 300s, configurabile), liveness preservata; (2) `handle_incoming` non ricontrollava il chat_id (unica barriera: filtro Telethon alla registrazione) → difesa in profondità, chat fuori lista = scarto totale. Più: flood E2E 100 msg → esattamente 1 ordine; segnali concatenati → first-match-wins. Live e2e con credenziali (`test_telegram_live_e2e.py`) resta skip-senza-secrets by design |
| H | **Emergency stop + lockdown hard**: cancelOrders fallisce durante stop→resta LOCKDOWN | FATTA. Base già coperta (eccezione/ok=False nel cancel → resta bloccato). **2 GAP REALI trovati e fixati (autorizzati dall'owner)**: (1) lo stato emergency viveva SOLO in memoria — crash/riavvio del processo (VPS/supervisor, Fase 4) faceva ripartire il bot operativo bypassando reset_emergency() → ora persistito su DB settings (scritto PRIMA del cancel-all, ripristinato al costruttore, pulito solo da reset_emergency; persist fallito = riportato nel risultato, mai silenzioso); (2) get_status() non esponeva is_emergency_stopped → aggiunto allo snapshot. Più test: timeout nel cancel, fallimento parziale N/M mercati, segnale in arrivo DURANTE il cancel (rifiutato: flag settato prima), reset_cycle NON pulisce l'emergenza, flusso reset→segnali riammessi |
| I | **Observability secret redaction**: password/api_key/session_string/cert mai nei log/alert; correlation_id obbligatorio | FATTA. Dedup: la redazione STRUTTURATA esisteva già ed era testata (sanitize_value per chiave, export json/csv, snapshot DB, alert Telegram, cifratura at-rest). **GAP REALE trovato e fixato (autorizzato dall'owner)**: le STRINGHE d'errore grezze non erano redatte — `LOGIN_FAILED: {data}` faceva eco all'intera risposta di login e le eccezioni di rete col token dentro arrivavano intatte a io_snapshot/get_status['runtime_io']/log/circuit breaker → ora redazione per valore del session token in `_record_io`/`_post_jsonrpc` + LOGIN_FAILED solo con loginStatus. Più: contratto correlation_id-non-redatto dal sanitizer (filo del forensics) |
| L | **Chaos/stress**: partition post-submit, DB locked, kill a metà ordine, clock skew, 100 segnali/5s, restart con INFLIGHT | FATTA. Dedup Phase 0: 5/6 scenari GIA' coperti in modo forte (74+ file tra tests/chaos, recovery, reconciliation, failure) — partition/ghost, DB locked, kill mid-order, 100 segnali/5s, restart INFLIGHT. **GAP REALE trovato e fixato (autorizzato dall'owner)**: il `DuplicationGuard` usava `time.time()` (wall-clock) per il TTL → un salto del wall-clock in AVANTI oltre il TTL (NTP/sleep VPS/set manuale) faceva scadere prematuramente una chiave di dedup → il duplicato passava `acquire()` = DOPPIA BET. Fix: TTL su `time.monotonic()` (immune ai salti); audit `_registered_at` resta wall-clock. Test: BLOCK (salto-avanti non libera la chiave / seed restart protetto), PASS (scadenza monotonica normale, salto-indietro nessun leak, audit wall-clock) + 3 test TTL esistenti migrati a monotonic |
| M | **Hard-verify PR gate**: PASS/BLOCK/MALFORMED/EDGE su scope guard, current-head, forbidden files | FATTA. Dedup Phase 0: la logica PR-automation ha ~1080 test (test_pr_automation_controller) + flow/readiness, MA l'ENTRY-POINT `scripts/guardrail_check.py` non aveva alcun file di unit test dedicato (7/9 funzioni senza copertura diretta; fail-closed su JSON malformato non testato). Nuovo `tests/scripts/test_guardrail_check.py` (test-only): copre load_json/normalize_changed_files/extract_tasks/_is_placeholder_or_invalid/_is_allowed_task_key/_select_task_candidate/touches_critical_files/resolve_task/validate_task_selection + main() E2E con PASS/BLOCK/MALFORMED. **Caratterizzati i confini** del gate: valida fail-closed (file mancante/JSON invalido/pr_meta non-dict/pr_files non-list/TASK marker mancante o non registrato/task_file_change+critical) ma NON enforce per-task `files`/`max_files` (solo warning >25) né blocca `.github/workflows/*` con task registrato (enforcement scope nel pr_automation_controller). Nessun bug di prodotto: il gap era la copertura mancante dell'entry-point |

### TOP 20 operazionali (task `test_suite_top20`)

I 20 bucket (runtime invariants, lifecycle contract, ACK/terminal, copy/
pattern exclusivity, restart no-reexec, dedup stabile, reconciliation
isolation, no-false-recovery, Telegram no-false-healthy/watchdog grace/
autoheal bounded, parity live-sim runtime+engine, risk contract, event bus
isolamento+ordering, throttle bounded, watchdog purity, MM exposure block,
dual PnL distinti) si integrano dentro le PR A-M dove naturale; i residui
(event bus, watchdog, autoheal bounded) = PR dedicata "proof operazionali"
già pianificata sopra. Regola: estendere i file test esistenti, zero
modifiche di produzione salvo seam minimi giustificati.

### TOP 30 matematici (task `test_suite_top30_math`)

Cashout esatto BACK/LAY ±, break-even senza flip di segno, partial exit,
dutching uniformità 2/3/N esiti, budget, no stake negativi, order
independence, invalid odds fail-closed, cache coerente, esiti non coperti
mai etichettati hedged, MM caps, PnL no-NaN/inf, property/metamorphic.
NOTA: l'audit matematico ha già verificato STRONG gran parte del core —
questa PR copre: i numeri 1-8 (cashout exact math, casa naturale
`tests/unit/test_cashout_math.py` su `dynamic_cashout_single` +
`calculate_cashout_pnl`), 20-21, 28-30 property-based, più gli 11 test
mancanti dell'audit matematico (sezione sopra). Vietato inventare un
cashout engine finto: usare le superfici reali.

PARTE 1 FATTA (`tests/unit/test_top30_math_gaps.py`, test-only su
`dutching.py`). Phase 0 dedup: math STRONG (85-90% già coperto: equal-profit
2/3/N, budget, no stake negativi, invalid odds fail-closed, cache, rounding
bounded, PnL finite, weighted-average multi-fill, liability LAY floor,
commissione per-market). Coperti i 4 gap a **funzione pura**: order-independence,
stress N grande (20/50/100, prima max N=8), break-even senza flip di segno
(green-up cashout, BACK+LAY), caratterizzazione tick-ladder (stake ai centesimi,
preview-only, no snap a tick Betfair). **FINDING caratterizzato (non bug che
perde soldi)**: il dutching è order-independent solo a meno di un residuo di
rounding di 1 centesimo, la cui assegnazione dipende dalla posizione in lista;
budget ed equal-profit sono comunque preservati e deterministici per-ordine. Il
test blocca peggioramenti (residuo > 1 cent); renderlo strettamente
order-independent toccherebbe `dutching.py` (critico) → rimandato.

**PARTE 2 FATTA** (`tests/integration/test_top30_math_gaps_part2.py`, test-only su
superfici reali — `MarketNetRealizedSettlementAggregator`/`SimulationBroker`/
`PnLEngine`/`SimulationState`/`dutching`). Copre i residui di integrazione: **parity
settlement LAY** dedicata (la parity esistente era BACK-only), **parity commissione
live (aggregator) vs sim (broker)** multi-leg, **commissione market-net
path-independent** (split vs combinata → stesso totale, no doppia per-leg), **refund
±X** (+100/−100 → commissione totale 0) + **precisione Decimal estrema** (0.001…1e6),
**segregazione ledger per market_id** (no crosstalk), **worst-case liability LAY**
(mai sottostimata, monotona, BACK capped allo stake), **idempotenza equalize** (2× →
nessun drift), **contratto settlement-basis** (`market_net_realized`). 15 test.
**Nota onesta**: l'idempotenza "retry settlement" è riformulata come commissione
path-independent — l'aggregator/broker **accumulano** (no dedup per-correlation: la
dedup vive nel runtime `_processed_realized_pnl_keys`), quindi non si finge
un'idempotenza che il broker non ha.

## Backlog (non bloccante)

### Selezione "quant" a basso costo (valutazione owner, giugno 2026)

Dalla lista hedge-fund: scartati multi-account/churning Premium Charge
(violazione T&C Betfair), spoofing (manipolazione di mercato), latenza
HFT/FPGA/co-location (fuori scala per il caso d'uso). Vale la pena:

- **Market-level volatility breaker**: congela il singolo match se la
  quota si muove oltre soglia anomala in N secondi senza evento nel feed
  (gol fantasma/VAR); si aggancia allo stack breaker esistente e ai tick
  gia' disponibili. Azione: cancel ordini pendenti su quel mercato +
  pausa 180s. Complessita' media, valore alto in-play.
- **Bet delay in-play nel broker simulato**: simulare i ~5s di ritardo
  Betfair sui mercati live (requote/lapse durante l'attesa) per paper
  trading fedele. Complessita' bassa.
- **Stake mode Fractional Kelly (opzionale)**: accanto a FISSO/MASTER/MM
  (task 2.3), mode Kelly frazionario (10-25%) con cap MM sempre validi.
  Solo matematica + test. Complessita' medio-bassa.
- **Alert breaker ad alta priorita' su Telegram**: formattazione
  d'emergenza quando scatta un breaker (stack alert gia' esistente);
  NIENTE comandi remoti di sblocco senza 2FA (superficie d'attacco,
  decisione owner separata).

### Altri item

- Supporto runner "Under X.5" in `TelegramBetResolver` (oggi risolve solo "Over X.5";
  i preset Under del form pattern sono disattivati finché manca — vedi nota in
  `telegram_module._PREDEFINED`).
- Fix `pr-self-check-refresh.yml`: sui trigger `workflow_run` il PR number arriva
  vuoto e il refresh esce senza fare nulla (bug latente dell'automazione).
- Guardrail JSON orfani: `copy_engine`, `session_manager`, `rate_limiter`,
  `live_gate` referenziano moduli i cui file NON esistono nel repo (creati
  dal bot autofix di maggio); i loro ultra-check fallirebbero se mai
  innescati. Decidere: creare i moduli o rimuovere spec+routing. (I 9
  moduli con file reali sono stati riparati nella PR-D del programma test.)

- Parser Trainer (proposto, mai implementato): bottone "Addestra" su messaggio →
  form guidato → auto-generazione regex → test immediato.
- Pre-submit risk envelope centralizzato (UFA-007), policy esecuzione per origine
  Telegram (UFA-009), boundary helper preview (UFA-017), visibilità stub headless (UFA-018).
- Evidence machinery completa in stile Codex (UFA-010/011/013) se servirà multi-operatore.

## Da NON fare

- Task Codex UFA-004 (key source) e UFA-006 (hard-stop validator) come scritti:
  già implementati in main (Phase A/B, PR #166/#167).
- UFA-002 (tempdir) come PR sul repo: era un problema della sandbox di audit.
- Micro-stake come strategia permanente (rischio sospensione account).

## Stima totale

| Voce | Stima |
|---|---|
| Sviluppo (Fasi 0–4) | **~11–14 giorni** lavorativi caso peggiore (Fase 4 Windows: +1 giorno per packaging/servizio) |
| Validazione (Fase 5) | 2–3 settimane calendario (bot autonomo, supervisione log) |
| Primo euro reale (micro-stake) | possibile a fine Fase 4 + step 5.1 |

**Regola non negoziabile**: l'`auto_bet` senza supervisione si attiva solo con TUTTE
le Fasi 0–4 completate — in particolare cashout mirror (B5/2.1), guard anti-payload-simulato
(3.1) e daily-loss → kill switch (1.4) — e dopo lo step 5.1. Soddisfare solo una parte
dei prerequisiti NON autorizza l'operatività non supervisionata. È ciò che trasforma
un bug del parser da catastrofe a perdita contenuta.
