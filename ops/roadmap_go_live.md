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
| 0.1 | Ambiente riproducibile: venv + `requirements-lock.txt` + pytest installato; doc ambiente CI/audit | Senza questo nessuna evidence è producibile (UFA-001 valido) |
| 0.2 | Fix issue test: rename file duplicati (`test_betfair_client_failures.py`, `test_trading_engine.py`), fix `FakeBatchManager.get_open_batches()` | Sblocca la baseline verde (B9) |
| 0.3 | Lint baseline live-critical: 38 errori ruff, 22 auto-fixabili | Cosmetico, mezz'ora (UFA-003) |
| 0.4 | Rebase/merge del branch copy-trading su main aggiornato | Attenzione: `telegram_module.py` +600 righe, ~80 PR di distanza |

## FASE 1 — Blocker live (3–4 giorni)

| Task | Descrizione | Note |
|---|---|---|
| 1.1 | **Listener Telethon reale**: client da credenziali DB (auth già pronta in `telegram_controller.py`), handler NewMessage sui `monitored_chats`, aggiorna `last_successful_message_ts`, reconnect/FloodWait | L'autoheal (`recovery/telegram_autoheal.py`) è già pronto a gestirlo (B1) |
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

## Backlog (non bloccante)

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
