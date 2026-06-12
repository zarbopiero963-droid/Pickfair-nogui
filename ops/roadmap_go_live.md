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
| 1.2 | **Keepalive sessione betting**: loop ~10 min (`keepAlive`/`get_account_funds`), re-auth fail-closed già esistente | B2 |
| 1.3 | **`get_current_orders` + RPC `listCurrentOrders`** nel client live + interfaccia `BetfairService.list_current_orders` senza fallback silenzioso in LIVE | Abilita rilevamento ghost orders (B3, UFA-005) |
| 1.4 | **Daily-loss breach → kill switch**: collegare `DAILY_LOSS_BREACH_TRIGGERED` all'emergency stop | Piccolo: monitoring ed eventi esistono già (B4) |
| 1.5 | **`replace_orders`** nel client live | Risolve la chiamata orfana di `order_manager.py:842` (B8) |

## FASE 2 — Copy/mirror trading completo (2–3 giorni)

| Task | Descrizione | Note |
|---|---|---|
| 2.1 | **Cashout mirror**, 3 pezzi: publisher `CASHOUT_SUCCESS` all'esecuzione; routing `signal_type` CASHOUT → `REQ_EXECUTE_CASHOUT`; executor per `CMD_EXECUTE_CASHOUT`. Bus REQ→CMD e matematica (`pnl_engine`, `dutching`) già pronti | Senza questo il copy parziale è più pericoloso di nessun copy (B5) |
| 2.2 | **Best price su percorso DIRECT**: rilettura book per i MASTER SIGNAL (riuso logica resolver) + tolleranza massima di deviazione configurabile + gestione ordini non matchati (TTL/cancel) | B6 |
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
2. **EventBus** (`core/event_bus.py`): drain vs lossy shutdown, isolamento
   subscriber avvelenato, metriche di pressione sotto carico
3. **BetfairService session gate**: re-auth bounded, fail-closed su sessione
   invalida (integra i 17 test di `test_session_expiry_recovery.py`)
4. **RuntimeController control-path** (solo dopo 1-3): start/stop/pause/
   emergency non bloccanti — alta autorità, va toccato per ultimo

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
| I | **Observability secret redaction**: password/api_key/session_string/cert mai nei log/alert; correlation_id obbligatorio | |
| L | **Chaos/stress**: partition post-submit, DB locked, kill a metà ordine, clock skew, 100 segnali/5s, restart con INFLIGHT | |
| M | **Hard-verify PR gate**: PASS/BLOCK/MALFORMED/EDGE su scope guard, current-head, forbidden files | |

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
