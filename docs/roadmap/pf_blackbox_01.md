# PF-BLACKBOX-01: scatola nera universale (architettura)

Direttiva owner del 08/11/2026. Questo documento è il progetto; il codice arriva per PR successive, una alla volta.

La scatola nera è una funzionalità permanente di Pickfair-nogui, non uno strumento di debug.
- Non pubblica su GitHub e non dipende dalla finestra dei log.
- Nessun servizio VPS, LIVE OFF.

## 1. Punto di partenza verificato (main `89af178e`)

| Esistente | Stato | Cosa ne facciamo |
|---|---|---|
| `core/event_journal.py` | JSONL append-only, fsync per evento, coda troncata gestita, ~122 tipi in `EVENT_CATEGORIES`, redazione. **Nessun chiamante a runtime** (il ponte `app.py` è stato rimosso con #435) | si riusano il **vocabolario** e la **redazione**; il formato di persistenza cambia (§3) |
| `core/session_recorder.py` | `session_id`, `seq` sotto lock, `mono`, catene `root`/`corr` via contextvars, `ALWAYS_RECORDED`. **Nessun chiamante** | diventa la base dell'API di scrittura (`seq`, catene, sessione) |
| `audit_events` (SQLite, `database.py:2175`) | l'unica traccia viva (engine `_emit`, residui del cashout). Niente sequenza globale, sessione o redazione; possibile doppia scrittura (DB + async writer) | resta per compatibilità; ogni sua scrittura passa dal punto unico della scatola nera (§4) |
| `core/event_bus.py` | pub/sub in memoria, non persistente, eventi senza subscriber scartati | **non** è la fonte della scatola nera: gli eventi critici si scrivono in modo sincrono, non via bus |
| `safety_logger.py`, `core/event_log.py` | log testuali senza fsync | restano log umani; gli eventi equivalenti entrano nella scatola nera |
| `core/journal_view.py`, `ui_panels/audit_panel.py` | CLI e pannello Tk non collegati | base del visualizzatore (§6) |
| eccezioni non gestite | nessun `sys.excepthook`, `threading.excepthook` o `faulthandler` | da aggiungere (§5) |
| impostazioni | `save_settings` sovrascrive la riga: nessun vecchio/nuovo valore | da aggiungere (§4) |

### 1-bis. Destino di ogni componente (una sola scatola nera)

Legenda: **MANTENERE** (resta com'è), **UNIFICARE** (assorbito nella scatola nera), **ADATTARE** (resta, ma scrive o legge tramite la scatola nera), **DISMETTERE** (rimosso dopo che il sostituto è su main con test di parità). Nessun comportamento esistente cambia in silenzio: ogni ADATTARE e ogni DISMETTERE ha un test di parità nella sua PR.

| Componente | Destino | Dettaglio |
|---|---|---|
| `core/event_journal.py` | **UNIFICARE** | vocabolario `EVENT_CATEGORIES`, `category_of` e redazione passano a `core/blackbox`; il writer JSONL viene dismesso (§DISMETTERE) dopo BB-1, perché oggi non ha chiamanti |
| `core/session_recorder.py` | **UNIFICARE** | `session_id`, `seq` per sessione, catene `root`/`corr` (contextvars), campionamento ed `ALWAYS_RECORDED` diventano l'API di `BlackBox.record` |
| `core/event_log.py` | **ADATTARE** | `redact_secrets`/`register_secret` restano la fonte unica della redazione; il log testuale giornaliero resta come log umano, senza valore di evidenza |
| `core/redaction.py`, `observability/sanitizers.py`, `telegram_sanitizer.py` | **MANTENERE** | chiamati dalla redazione della scatola nera; nessuna seconda implementazione |
| `audit_events` (SQLite) + `Database.insert_audit_event` | **ADATTARE** | `TradingEngine._emit`, `_emit_critical` e la catena residui del cashout scrivono tramite `BlackBox.record`; la tabella resta (stessa forma, stessi lettori) finché i suoi lettori non sono migrati; la doppia scrittura DB + async writer sparisce |
| `core/async_db_writer.py` (ramo `audit_event`) | **ADATTARE** | non scrive più audit in proprio: inoltra a `BlackBox.record`; gli eventi critici non passano mai da una coda asincrona |
| `core/event_bus.py` | **MANTENERE** | resta il pub/sub in memoria del runtime; **non** è la fonte di verità; un subscriber della scatola nera registra solo gli eventi non critici del bus (RUNTIME_*, TELEGRAM_STATUS, SIGNAL_*, QUICK_BET_*), con gli stessi handler di log di oggi |
| `headless_main._wire_bus` (handler di log) | **MANTENERE** | i log su stderr restano; la scatola nera si aggiunge, non li sostituisce |
| `safety_logger.py` | **ADATTARE** | ogni `log_event` produce anche l'evento tipizzato equivalente (SAFE_MODE_*, AI_BLOCKED, LIQUIDITY_*); file testuale invariato |
| `core/journal_view.py` | **ADATTARE** | diventa la CLI di consultazione della scatola nera (stessi filtri, ordine per `seq` invece che per `ts`) |
| `ui_panels/audit_panel.py` | **ADATTARE** | base del pannello «Scatola nera / Registro attività» (BB-5), letto da `core/blackbox_query` |
| `observability/retention_manager.py` | **ADATTARE** | ottiene la regola di retention della scatola nera, con il vincolo «mai eventi di ordini non riconciliati» |
| `orders`, `order_saga`, `startup_orders`, `outbox`, `settlement_effects`, `risk_position_history`, `received_signals`, `telegram_outbox_log` | **MANTENERE** | sono **stato** operativo, non registro; ogni loro transizione significativa produce un evento con `state_before`/`state_after`, senza cambiarne lo schema |
| writer JSONL di `event_journal` (`append_event`, `prune_events`, `clear`), `config_store.recorder_enabled` | **DISMETTERE** | dopo BB-1: oggi senza chiamanti (percorso del ponte rimosso in #435); i test esistenti si spostano sull'API unificata |

Risultato: un solo store (`blackbox.sqlite`), una sola API di scrittura (`BlackBox.record`), una sola API di lettura (`core/blackbox_query`) usata da GUI, CLI, Control API e MCP.

**Regola:** un solo sistema. Non nasce una terza traccia accanto alle due esistenti; `event_journal` e `session_recorder` vengono assorbiti.

## 2. Requisiti → decisioni

| Requisito owner | Decisione |
|---|---|
| parte prima dei componenti operativi | aperta come prima cosa in `headless_main.main()`, prima di `build()`/`Database()`; il primo evento è `PROCESS_START` |
| ordinamento affidabile anche multiprocesso | `seq` globale assegnato dal DB (`INTEGER PRIMARY KEY`), più `session_seq` e `mono` per sessione |
| persistente e resistente ai crash | SQLite dedicato `blackbox.sqlite` nella config dir, WAL, `synchronous=FULL`, una transazione per evento critico |
| modifiche non autorizzate e record mancanti o corrotti | catena di hash: `hash = sha256(prev_hash ‖ JSON canonico)`; verifica di continuità di `seq`, catena e checksum; ogni lacuna diventa un evento `BLACKBOX_GAP` e uno stato visibile |
| nessun segreto | redazione **in scrittura** (chiavi sensibili e segreti letterali registrati, `core/redaction` + `event_log.redact_secrets`); test che fanno fallire la build se un token entra |
| richiesta ≠ conferma | ogni operazione ha due eventi distinti (`*_REQUESTED` / `*_CONFIRMED` / `*_REJECTED` / `*_UNKNOWN`) con `outcome` ∈ VERIFIED / PENDING / UNKNOWN |
| fail-closed sulle operazioni finanziarie | prima di inviare un ordine, un cancel, un hedge o un cashout, l'evento `*_REQUESTED` deve essere durevole; se la scrittura fallisce ⇒ rifiuto `BLACKBOX_UNAVAILABLE`, nessun invio. Le operazioni già inviate non si bloccano: l'esito va in un buffer critico in memoria con marcatore di lacuna e la riconciliazione resta obbligatoria |
| disco pieno | `BLACKBOX_DEGRADED`: niente nuovi ingressi; uscite protettive e riconciliazione consentite; allerta; scrittura ritentata. Mai perdita silenziosa |
| rotazione e retention | per sessione o per età, configurabile; mai cancellare eventi legati a ordini non riconciliati o ad AMBIGUOUS aperti; un archivio ruotato conserva la catena di hash (il primo record cita l'ultimo hash del precedente) |
| letture MCP registrate senza ricorsione | ogni richiesta di lettura produce **un** evento `READ_REQUEST` aggregato (chi, filtro, conteggio); le letture della scatola nera non generano altri eventi; rate-limit per client |

## 3. Record

Tabella `events`, append-only. Un trigger SQLite vieta UPDATE e DELETE fuori dalla retention autorizzata.

| Campo | Contenuto |
|---|---|
| `seq` | progressivo globale persistente (PK) |
| `event_id` | uuid |
| `ts_utc` | ISO-8601 UTC al microsecondo (la visualizzazione converte nell'ora locale) |
| `mono`, `session_id`, `session_seq`, `pid` | sessione e processo |
| `source` | USER, SYSTEM, MCP, CONTROL_API, STRATEGY, EXCHANGE (più TELEGRAM, CLI) |
| `component`, `category`, `type`, `severity` | vocabolario di `EVENT_CATEGORIES`, esteso e versionato |
| `state_before`, `state_after`, `values_before`, `values_after` | quando applicabile |
| `reason_code`, `reason` | motivazione |
| `correlation_id`, `causation_id` | causation = `event_id` dell'evento che l'ha causato |
| `table_id`, `market_id`, `selection_id`, `order_ref` (customer_order_ref), `bet_id`, `strategy` | indicizzati |
| `phase` | ATTEMPT (richiesta inviata) / RESULT (esito) / OBSERVATION (fatto osservato, per esempio un book o un saldo) |
| `outcome` | VERIFIED / PENDING / UNKNOWN / REJECTED / FAILED / NOT_APPLICABLE (l'AMBIGUOUS dell'exchange vale UNKNOWN) |
| `data` | JSON redatto |
| `prev_hash`, `hash` | catena |

## 4. Scrittura: un solo punto

- **API:** `core/blackbox.py`, `BlackBox.record(type, *, source, ..., critical=False)`.
- `critical=True` ⇒ scrittura sincrona e durevole prima del ritorno; un fallimento ritorna un errore che il chiamante finanziario deve rispettare.
- `audit_events` si alimenta da `record`, così la doppia scrittura e le divergenze spariscono.

**Punti di aggancio, in ordine di PR:**

| PR | Contenuto |
|---|---|
| BB-1 | nucleo: store, catena, `seq`, redazione, verifica, disco pieno, retention protetta; ciclo di vita (`PROCESS_START`, init dei componenti, connessioni, `SHUTDOWN`, `CRASH` con excepthook, threading.excepthook, faulthandler, fatale in `main()`); riavvio con lettura della sessione precedente e chiusura «non pulita» rilevata |
| BB-2 | impostazioni (vecchio/nuovo valore, origine, esito, nel punto unico `Database.save_settings` / `SettingsService`); comandi utente, CLI, Telegram; arresto d'emergenza con causa |
| BB-3 | catena del denaro: segnali, Roserpina e Money Management, controlli rischio, rifiuti, Dutching e microstake, ordini (request, response, cancel, match, partial fill), hedge e cashout, saldo, riserve, esposizione, P&L, riconciliazione, retry, timeout; gate fail-closed `BLACKBOX_UNAVAILABLE` prima dell'invio |
| BB-4 | SL, TP, trailing, protezione sospesa e ripresa, mercato sospeso o riaperto. Dopo la catena F-02/F-04/F-03, per non agganciare codice non ancora su main |
| BB-5 | visualizzatore nell'app (Windows/Linux, Tk) e CLI: tempo reale, sessioni precedenti, filtri, ricerca, dettaglio, ciclo di vita di un ordine o posizione, esportazione redatta; `docs/design/design_handoff.md` aggiornato; screenshot Xvfb |
| BB-6 | API di lettura **read-only** `core/blackbox_query.py` per Control API e MCP (Claude C): elenco, intervallo, sessione, tavolo, ordine, motivazione di una decisione, stato del registro e lacune; nessuna scrittura esposta |

### 4-bis. Contratto di lettura (allineato a Claude C, `pickfair-mcp-` `docs/design/BLACKBOX_READ_CONTRACT.md` @ `74e61a0`)

`core/blackbox_query` (nel processo, sola lettura):

| Funzione | Ritorna |
|---|---|
| `status()` | salute, ultimo `seq`, lacune, modalità degradata |
| `sessions()` | elenco delle sessioni |
| `query(filters, after_seq, limit)` | `(records, next_after_seq, gaps_in_range, as_of_seq)` |
| `get(event_id, chain_depth)` | l'evento con la sua catena causale |

Hook `request_hook(record) -> bool` (contratto Claude C 1.13.0 @ `2350850`), obbligatorio: senza hook la Control API non parte (`REQUEST_HOOK_REQUIRED`). Una richiesta servita produce un `ATTEMPT` prima della lettura e un `RESULT` dopo l'invio; una richiesta rifiutata produce solo il `RESULT`. Campi: `phase`, `route` (il modello della route, mai il percorso grezzo), `params` (solo valori validati), `status`, `code`, `request_id` (`X-Request-Id`), `duration_ms`, `coalesced_before`. Mai un record per ogni evento letto. Se ritorna `False` o solleva un'eccezione, la Control API risponde 503 `BLACKBOX_UNAVAILABLE`, tranne `/v1/blackbox/status`. Oltre 20 record/s (raffica 40), le letture si aggregano in un solo record con il conteggio (`coalesced_before`). Paginazione: `next_after_seq = 0` quando non ci sono altre pagine; `as_of_seq = 0` con registro vuoto.

Regole per i campi:
- un campo non registrato vale `null` ed è elencato in `not_recorded` (nello stato: `not_reported`); `unavailable` resta riservato al validatore del contratto («null se e solo se elencato»);
- `customer_ref` è conservato per la riconciliazione, ma la vista esterna (Control API/MCP) non lo espone mai.

## 5. Crash e ripristino

- **All'avvio:** se la sessione precedente non ha `SHUTDOWN`, si registra `UNCLEAN_TERMINATION`, poi si verificano catena e `seq` e si apre la nuova sessione.
- **Ordini:** la riconciliazione già prevista resta prima di ogni nuovo ordine; i suoi esiti entrano nella scatola nera.
- **Coda corrotta** (scrittura interrotta): rilevata dal checksum, segnalata come `BLACKBOX_CORRUPT_TAIL`, mai riscritta in silenzio.

## 6. Criterio finale

«Mostrami tutto ieri dalle 15:00 alle 17:00» si traduce in `range(t0, t1)` sul solo registro, ordinato per `seq`. Gli eventi mancanti compaiono come lacune dichiarate (`BLACKBOX_GAP`, `outcome UNKNOWN`), mai ricostruiti o inventati.

## 7. Test (in ogni PR, PASS + BLOCK)

| Area | Test |
|---|---|
| ciclo di vita | avvio e arresto; crash simulato (kill durante una scrittura) con coda corrotta rilevata; riavvio con cronologia precedente leggibile |
| impostazioni e comandi | modifica impostazioni con vecchio/nuovo valore; comandi manuali, Telegram, CLI, MCP (lettura); arresto d'emergenza |
| tavoli e denaro | apertura e chiusura tavoli; microstake multipli; ordini e partial fill; SL/TP/trailing; mercato sospeso e riaperto; crash durante l'invio di un ordine ⇒ `REQUESTED` presente, esito UNKNOWN, riconciliazione registrata |
| concorrenza | multitavolo e due processi: `seq` senza buchi né duplicati |
| guasti di scrittura | disco pieno (filesystem pieno simulato) ⇒ nessun nuovo ingresso, uscite consentite; errore di persistenza ⇒ fail-closed prima dell'invio |
| sicurezza | redazione: nessun token, password o chiave nel DB e negli export; manomissione (UPDATE diretto) ⇒ catena rotta rilevata |
| consultazione | Linux e Windows (Windows: test sul codice; avvio reale MANUAL_ONLY finché non c'è un runner) e via MCP (Claude C) |

**Mutanti BLOCK:**
- `critical` ignorato;
- redazione rimossa;
- `seq` non controllato;
- fail-closed disattivato;
- retention che cancella eventi non riconciliati.

## 8. Ruoli

- **Claude B:** architettura, BB-1…BB-5, integrazione nel motore.
- **Claude C:** BB-6 lato Control API/MCP (strumenti read-only), sul contratto di `core/blackbox_query.py` fissato in BB-1.
- **Claude D:** test finanziari e oracolo di completezza (ogni ordine della campagna G7 ha la catena REQUESTED → esito → riconciliazione nella scatola nera).

Una PR alla volta; handoff tramite i canali già autorizzati.

## 9. Ordine nella coda

Dopo #610 F-16 (in attesa della decisione owner sul mutante), F-R06 e `pfckpt`, la PR BB-1 è la successiva. BB-4 segue la catena SL/TP. Le patch MCP-A si integrano prima di BB-6, che ne riusa il ponte `control_api/`.
