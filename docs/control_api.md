# Control API v1 — ponte di sola lettura (proposta per Claude B)

Stato: **PROVISIONAL**, consegnato da Claude C come handoff (pickfair-mcp-, `docs/handoff/`).
Il server, l'autenticazione e il contratto sono nel package `pickfair_control_api` del repository
pickfair-mcp- (solo libreria standard). In questo repository stanno soltanto il ponte che legge il
`RuntimeController` e l'aggancio opzionale a `headless_main`.

## Componenti

| File | Ruolo |
|---|---|
| `control_api/views.py` | letture pure del runtime (mai `get_status()`, che legge i fondi Betfair, aggiorna la cassa e fa girare il monitor della perdita giornaliera) |
| `control_api/bridge.py` | collega le viste al server `pickfair_control_api` |
| `control_api/launcher.py` | avvio opzionale; **disattivato per impostazione predefinita** |
| `headless_main.py` | 16 righe: avvio dopo un `runtime.start()` riuscito, arresto per primo nello `stop()` |

Route servite: `GET /v1/meta`, `GET /v1/status`, `GET /v1/reconciliation/status` e, da MCP-01e
(contratto 1.2.0), `GET /v1/account` e `GET /v1/exposure`. Nessun comando. Le altre route di
lettura approvate rispondono `501 NOT_IMPLEMENTED` (mai un dato); ogni metodo diverso da GET `405`.

### MCP-01e — bankroll, cassa, fondi, tavoli Roserpina, esposizione

| Route | Sezione | Fonte nel core (una sola chiamata) |
|---|---|---|
| `/v1/account` | `execution_mode` | `RuntimeController.execution_mode` (SIM = saldo fittizio del broker simulato) |
| | `bankroll` | `RiskDesk.debug_snapshot()` |
| | `cash` | `RiskDesk.cash_basis()` + `cash_equity_reason` se la cassa non è verificata |
| | `exchange_funds` | ultimo `_account_snapshot` registrato dal runtime (nessuna nuova lettura dell'exchange); `age_s` da `_account_snapshot_read_at` |
| | `desk_mode` | `RoserpinaMoneyManagement.determine_desk_mode` con gli argomenti di `_desk_mode()`, sui valori della sezione `bankroll` |
| `/v1/exposure` | `tables` | `TableManager.snapshot()`; esce solo id, stato, occupato sì/no, id di mercato e selezione, esposizione, perdita da recuperare (mai nome dell'evento né `meta`) |
| | `total_exposure` | `TableManager.total_exposure()` (valore del cap `max_open_exposure`; non finito ⇒ indisponibile) |
| | `recovery` | `RuntimeController._active_recovery_count()` (fail-closed del core) + `max_recovery_tables`, `allow_recovery` |

### MCP-01f — diagnostica del motore, protezioni finanziarie, posizioni (contratto 1.3.0)

`GET /v1/status` aggiunge due sezioni; `GET /v1/positions` è nuova. Importi e quote delle nuove
sezioni sono **stringhe decimali canoniche** (`decimal_str`): lossless dal `Decimal` del core, dal
float del core la rappresentazione più corta che lo identifica.

| Sezione | Contenuto | Fonte |
|---|---|---|
| `engine.trading_engine` | stato e componenti (calcolati dal core all'avvio del motore), risk gate cablato | `TradingEngine.readiness()`, `risk_gate_wired` |
| `engine.betfair` | connesso, cliente presente, SIMULATION/LIVE, errore presente sì/no, keepalive | attributi del `BetfairService` (nessun metodo che tocchi il cliente) + `keepalive_status()` |
| `engine.stream`, `io`, `event_bus`, `queues` | feed, I/O del runtime, pressione del bus, cashout in volo/incerti, ordini DIRECT | `streaming_feed.status()`, `runtime_io_snapshot()`, `bus.pressure_snapshot()`, contatori del runtime |
| `engine.modes` | modalità configurata, flag SIM, LIVE abilitato, readiness LIVE, cliente effettivo; `read_only_mode = NOT_IN_CORE` | attributi del runtime |
| `protections` | 22 protezioni, ognuna `ACTIVE`/`INACTIVE`/`TRIGGERED`/`UNKNOWN`/`UNAVAILABLE` con motivo, origine, limite, istante; elenchi `disabled` e `unverifiable` | regole del core chiamate direttamente: `_entry_funds_reason`, `validators.risk_pct_reason`, `risk_stop.exposure_stop_reason`, `_session_loss_block_reason`, `determine_desk_mode`, monitor della perdita giornaliera |
| `/v1/positions` | mercato, selezione, BACK/LAY/FLAT, size, prezzo medio, P&L realizzato e non realizzato (ultimo mark), esposizione, liability | registro `PositionLedger` (Decimal) sotto il lock del `PnLEngine`; senza registro solo prezzo e size (`CORE_FLOAT`) |

Regole: una lettura fallita o un lock occupato (attesa massima 0,25 s) è `UNKNOWN`, mai `ACTIVE`;
ciò che il core non espone è `UNAVAILABLE` (stato negoziabile del mercato: F-23, Claude D). Lo stop
drawdown percentuale è `INACTIVE (ENFORCED_ONLY_IN_LIVE)` in SIM perché il core lo applica solo in
LIVE. Le percentuali obbligatorie assenti sono `TRIGGERED` (il core nega ogni entrata). Nomi di
eventi, `customer_ref`, `reservation_id`, testi d'errore e credenziali non escono mai.

Consistenza `PER_SECTION_LOCKED`: `bankroll` e `cash` nella stessa acquisizione del lock del
RiskDesk; `tables`, `total_exposure` e `recovery` nella stessa acquisizione del lock dei tavoli.
Attesa massima `LOCK_WAIT_S = 0,25 s`: se il runtime tiene il lock più a lungo, quelle sezioni sono
indisponibili (null + `unavailable`), mai bloccate né stimate. Metodi esclusi perché non puri:
`get_status()`, `pending_exposure()`, `read_funds_for_controller_entry()`, `snapshot()` del broker
SIM (aggiorna cache interne), `duplication_guard.snapshot()`.

## Attivazione

Solo con entrambi i flag espliciti:

```bash
mkdir -m 700 -p ~/.pickfair
python headless_main.py --sim --control-api-port 8765 --control-api-token-file ~/.pickfair/control_api.token
```

Nessuna variabile d'ambiente e nessuna chiave di `config.json` attivano la Control API. Senza flag:
nessun import di `pickfair_control_api`, nessun socket, nessun file (provato su processo reale).

Prerequisiti (altrimenti la Control API NON parte e il motore continua, causa nel log
`CONTROL_API_NOT_STARTED: <codice>`):
- `pickfair_control_api` installato ESATTAMENTE alla versione e allo schema hash attesi da
  `control_api/bridge.py` (`PACKAGE_MISSING`, `CONTRACT_PIN_MISMATCH`); nessuna risoluzione di
  dipendenze all'avvio: il package è solo importato e verificato;
- porta 1–65535 libera (`PORT_INVALID`, `BIND_FAILED`);
- percorso del token assoluto, cartella privata dell'utente, non scrivibile da altri (`TOKEN_DIR_UNSAFE`);
- SHA del codice dimostrabile: `.git` del checkout o file `BUILD_SHA` scritto dal packaging (`GIT_SHA_UNRESOLVABLE`).

Ordine di avvio, senza servizi parziali: pin del package → token nuovo in memoria → bind del
listener → pubblicazione atomica del token (`0600`) → servizio. Pin errato: nessun socket e nessun
file; bind fallito: nessun file; pubblicazione fallita: listener chiuso. Il token di un'esecuzione
precedente non resta mai valido. Il MCP si collega con
`pickfair-mcp serve --adapter http --control-api-port 8765 --control-api-token-file ~/.pickfair/control_api.token`.

## Arresto

`HeadlessApp.stop()` ferma la Control API **prima** del runtime: nessuna lettura durante il teardown.
Il processo che termina chiude la porta (provato: connessione rifiutata dopo SIGTERM).

## Isolamento

- Bind solo su `127.0.0.1` letterale; nessuna risoluzione di nomi (anche il DNS inverso di
  `HTTPServer.server_bind` è disattivato); Host e token Bearer obbligatori.
- Letture senza effetti: provato su runtime avviato in SIM (DB, bus, saldi, chiamate a
  Betfair/broker, monitor di rischio, stato profondo invariati) e senza attesa sui lock di ammissione
  e dei tavoli. Le viste di status e riconciliazione prendono solo per un istante il lock del registro
  delle ambiguità (copia); account ed exposure prendono il lock del RiskDesk e quello dei tavoli con
  attesa massima di 0,25 s e li rilasciano subito dopo la copia (provato da test e mutazioni).
- Ogni risposta è validata contro lo schema del contratto prima dell'invio
  (`CONTRACT_SELF_CHECK`).
- La Control API non può fermare il motore: ogni suo errore all'avvio è registrato e ignorato.

## Rollback

1. Togliere i flag `--control-api-*`: comportamento identico a prima (nessuna porta).
2. Rollback del codice: revert del commit del ponte; `headless_main.py` torna alla versione precedente.
3. Rimuovere il file del token (`~/.pickfair/control_api.token`); non contiene altri dati.

## Limiti noti

- Lettura non atomica fra campi per status e riconciliazione (`consistency = PER_FIELD_NON_ATOMIC`),
  istante dichiarato in `observed_at`; account/exposure per sezione sotto lock (`PER_SECTION_LOCKED`).
- `desk_mode` di `/v1/account` segue la regola di `get_status` (bankroll rispetto al picco): le
  decisioni del Money Management per il singolo ordine considerano anche perdita giornaliera e
  crescita del ciclo e possono differire.
- `recovery_state` resta indisponibile finché il runtime non scrive per la prima volta lo stato di
  recovery: `_recovery_persist_failed` nasce solo lì (proposta: inizializzarlo nel costruttore).
- Token bearer locale: chi legge il file come lo stesso utente ottiene l'accesso in lettura.
- Packaging non incluso: `pyproject.toml` e `pickfair.spec` non distribuiscono `control_api/`.
