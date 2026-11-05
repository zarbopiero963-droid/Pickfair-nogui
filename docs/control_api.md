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

### MCP-01g — ordini (contratto 1.4.0)

`GET /v1/orders` legge il ledger durevole `orders` con una connessione SQLite **distinta e in sola
lettura** (`mode=ro` + `PRAGMA query_only`): ogni scrittura è impossibile a livello di database. Una
sola transazione di lettura (istantanea WAL coerente), attesa massima 0,25 s. Righe: tutti gli
ordini non finalizzati più gli ultimi 100, al massimo 500.

| Campo | Fonte |
|---|---|
| `local_id`, `core_status`, `created_at`, `updated_at`, `finalized`, `ambiguity_reason` | colonne del ledger |
| `bet_id` | risposta salvata, nelle stesse forme di `_open_order_candidates` (test di confronto) |
| `market_id`, `selection_id`, `side`, `price`, `stake`, `table_id` | payload salvato; importi in Decimal canonico |
| `size_matched`, `avg_price_matched`, `size_remaining` | primo `instructionReport` della risposta, solo se presenti |
| `role` | `HEDGE` (`core/hedge_ref.py`), `DUTCHING_LEG` (batch), altrimenti `ENTRY` |
| `in_engine_queue` | `ReconciliationEngine.ambiguity_snapshot()` |
| `broker_view` | SIM: betId presente/assente negli ordini del broker simulato (attributi, lock con attesa limitata); LIVE: `NOT_CHECKED` |

Stati: mappa esplicita (`ORDER_STATUS_MAP`, pubblicata nel contratto e confrontata da un test):
INFLIGHT/HEDGE_PENDING → PENDING; SUBMITTED/ACCEPTED_FOR_PROCESSING/HEDGE_PLACED → ACCEPTED, o
UNMATCHED/PARTIALLY_MATCHED/MATCHED solo con quantità abbinata e residua esplicite; HEDGE_UNMATCHED →
UNMATCHED; COMPLETED → MATCHED; FAILED/HEDGE_FAILED → FAILED; DENIED → REJECTED; DUPLICATE_BLOCKED;
AMBIGUOUS/HEDGE_AMBIGUOUS → AMBIGUOUS; altro → UNKNOWN. APPROVED, CANCELLED e SETTLED non esistono nel
ledger. Snapshot: OK, PARTIAL (righe illeggibili contate o elenco troncato), STALE (riconciliazione
d'avvio non conclusa), UNKNOWN (database occupato), UNAVAILABLE (non leggibile). Nessun importo
mancante diventa zero; nessun betId viene inventato.

### MCP-01h — SL/TP, settlement, barriere di cashout (contratto 1.5.0)

- `GET /v1/exposure`, ogni tavolo: `sl_tp` dal `meta` del tavolo (solo le chiavi `stop_loss`,
  `take_profit`, `stop_loss_pct`, `take_profit_pct`, `trailing_stop_pct`, `sl_tp_triggered`): NOT_ARMED,
  ARMED, TRIGGERED o UNKNOWN (valore presente ma illeggibile o negativo). Sul `main` attuale nessun
  percorso arma SL/TP, quindi ogni tavolo è NOT_ARMED: è lo stato reale. La forma scritta da F-02
  (Claude D) è già coperta dai test. Il trailing compare solo come percentuale: il core non lo applica.
- `GET /v1/reconciliation/status`, sezione `settlement` (solo conteggi): settlement non registrati
  (`_unrecorded_settlements`, sotto `_settlement_lock`), lotti trattenuti (`_held_batch_ids`, sotto
  `_risk_admission_lock`), `settlement_effects` per esito e non realizzati, `cycle_recovery_checkpoints`
  per fase (fase sconosciuta = UNKNOWN), cicli e invii ambigui, settlement del broker SIM (null in LIVE).
  Lettura del DB con la stessa connessione in sola lettura degli ordini; lock con attesa limitata.
- `GET /v1/status`, `engine.queues.cashout_barriers`: barriere di cashout attive.

### MCP-01j — impostazioni operative redatte (contratto 1.6.0)

- `GET /v1/config`: le impostazioni di `RoserpinaConfig` (`rc.config`) da un'allowlist esplicita di 42
  chiavi (`control_api.views.CONFIG_KEYS`, identica a `semantics.config.keys` del contratto). Ogni voce:
  `SET` con il valore esatto (decimali come stringhe canoniche, interi, booleani, profilo di rischio),
  `NOT_SET` (solo per i campi `Optional` del core: non configurato) o `INVALID` (presente ma
  illeggibile: tipo errato, NaN/inf, fuori scala; mai zero).
- Un solo riferimento a `rc.config` (`reload_config` lo sostituisce intero): consistenza
  `SINGLE_REFERENCE_READ`. Nessun lock, nessuna scrittura, nessuna chiamata a `settings_service`.
- Credenziali Betfair e Telegram, certificati, token e impostazioni persistite non sono mai lette. Un
  campo del core fuori allowlist compare solo per nome in `unlisted` (massimo 64); un nome sensibile
  (`core.redaction.is_sensitive_key` o un frammento del contratto) o non canonico è soltanto contato in
  `redacted`. Nessun valore fuori allowlist esce mai.

### MCP-01k — compatibilità con le correzioni di Claude D (contratto 1.7.0)

- `GET /v1/orders`: per le righe con esito non confermato (`AMBIGUOUS`, `HEDGE_AMBIGUOUS`, `INFLIGHT`,
  `HEDGE_PENDING`, `UNCONFIRMED_STATUSES`) gli importi abbinati/residui non si leggono dal report conservato
  nella risposta (il fix P0 di Claude D persiste la risposta senza esito, che può avere `sizeMatched`): restano
  null. Il betId resta, perché è l'identità usata dalla riconciliazione.
- `GET /v1/status`, `protections.funds_freshness.limit`: non si ricalcola la regola del core. Con l'accessore
  unico `funds_max_age_s()` (richiesto a Claude D) il suo valore; con la soglia F-16 `LIVE_FUNDS_MAX_AGE_S` ma
  senza accessore, limite null; sul `main` attuale il TTL delle riserve (120 s). Lo stato viene sempre dalla
  regola del core.

### MCP-01l — mercati e book (contratto 1.8.0)

- `GET /v1/markets`: i mercati del `MarketTracker` (ultimo book ricevuto per mercato), copiati in una sola
  operazione: id, stato del book (dato Betfair: OPEN/SUSPENDED/CLOSED/INACTIVE, altrimenti UNKNOWN), in-play,
  numero di runner, abbinato totale, `lastMatchTime`. Id non validi o book illeggibili contati in `omitted`;
  oltre 256 mercati `truncated`.
- `GET /v1/markets/{id}/book` (prima route con parametro: il server accetta solo `^[0-9]{1,4}\.[0-9]{1,15}$`,
  altrimenti 404, e passa al provider il valore già validato): per ogni runner stato, ultimo prezzo, abbinato e i
  tre migliori livelli BACK/LAY come stringhe decimali. Mercato non seguito: `NOT_TRACKED` (nessuna chiamata di
  rete per procurarlo). Livelli illeggibili o fuori ordine: runner omesso e contato, mai riordinato.
- Lo stato del book non è un giudizio di negoziabilità (F-23, protezione `market_tradability`). Il tracker non
  registra l'istante di ricezione: l'età del book è nota solo tramite `lastMatchTime`.

### MCP-SEC-01 — irrobustimento del package (contratto 1.9.0)

Solo il pin cambia nel ponte: le correzioni sono nel package `pickfair_control_api`, dopo una revisione di
sicurezza indipendente (05/11/2026). Token vuoti, corti o fuori alfabeto rifiutati alla costruzione; rotazioni
serializzate per intero; al più 16 connessioni servite insieme, ognuna chiusa entro 10 s (anche senza token);
errori di protocollo in JSON fisso (`BAD_REQUEST`, `METHOD_NOT_ALLOWED`) senza eco dell'input; nessun traceback
per richiesta su stderr del processo Pickfair.
### MCP-01i — stato della chiusura SL/TP (contratto 1.10.0, dipende da F-04)

- `GET /v1/exposure`, sezione `sl_tp_closes`: i record persistiti da F-04 (`risk.sl_tp_close.<customer_ref>` in
  `settings`) letti nella stessa transazione di sola lettura degli ordini della posizione e delle righe d'hedge.
  Per ogni chiusura: tavolo, mercato, selezione, lato, evento (STOP_LOSS/TAKE_PROFIT), stato
  (CLAIMED/ROUTED/NOT_EXECUTED/UNCERTAIN, illeggibile UNKNOWN), ritentabile come decide il core
  (`REROUTABLE_STATES`), dettaglio come codice, betId della posizione, riepilogo degli ordini (stessa mappa di
  `/v1/orders`), esposizione residua solo dal tavolo che porta ancora la posizione (stessa acquisizione del lock
  di `tables`), hedge della chiusura legati dal record (`cashout_request_ids`) alla riga del ledger
  (`cashout_request_id`), con le richieste senza riga in `not_in_ledger`; null se il record non porta il legame.
- Le chiusure respinte prima della rivendicazione non sono persistite dal core e non compaiono. Il
  `customer_ref` non esce mai. Senza F-04 la sezione è `UNAVAILABLE` `CORE_PENDING_F04`.

### MCP-01m — trailing (F-03), percentuali F-02, impostazioni non valide F-18 (contratto 1.11.0)

- `GET /v1/exposure`, `tables[].sl_tp.trailing`: stato del trailing dal `meta` del tavolo (picco in memoria) o dal picco
  persistito `risk.trailing_peak.<customer_ref>` (letto fuori dal lock dei tavoli); percentuale, soglia e lettura del
  picco con `core.trailing_stop` (nessuna regola replicata). `sl_tp_closes[].trigger` anche `TRAILING_STOP`.
- `GET /v1/config`: `stop_loss_pct`, `take_profit_pct`, `trailing_stop_pct` (F-02); `invalid_persisted` da
  `RoserpinaConfig.invalid_settings` (F-18), nomi `roserpina.<nome>`, sensibili solo contati.
- `GET /v1/status`, `protections.config_validity`: TRIGGERED quando `invalid_settings` non è vuoto (il core nega ogni
  nuova entrata), UNAVAILABLE `CORE_PENDING_F18` senza F-18.

Nota per i test di assenza di effetti: la lettura in WAL aggiorna l'indice `-shm` (marcatori di lettura
di SQLite, nessun dato); `.db` e `-wal` restano identici byte per byte.

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
