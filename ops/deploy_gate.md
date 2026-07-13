# Deploy Gate LIVE — readiness al boot vs a runtime

Il deploy gate decide se il runtime può partire in **LIVE**. La valutazione usa
`RuntimeController.evaluate_live_readiness` (prerequisiti strutturali) **più** il
**probe** di readiness (`observability/runtime_probe.py::get_live_readiness_report`),
che aggrega lo stato dei componenti (betfair_service, database, runtime_controller,
shutdown_manager, trading_engine, …).

## Boot vs runtime (phase-aware)

Il probe è consultato in **due fasi diverse**, con severità diversa. La fase è
cablata con un flag esplicito `boot` che attraversa tutta la catena del gate:
`get_deploy_gate_status(boot=...)` → `_get_probe_live_readiness_report(boot=...)`
→ `get_live_readiness_report(tolerate_pending_connection=boot)`.

| Fase | Chiamante | `boot` | Comportamento |
|---|---|---|---|
| **Boot** (pre-connessione) | `RuntimeController.start()` (deploy gate di avvio) e il preflight (`headless_main.py --preflight`, check pre-connessione) | `True` | Tollera i componenti *pending connection* |
| **Runtime** (monitoraggio/enforcement) | `watchdog_service`, `RuntimeController.is_live_allowed`, `RuntimeController._on_signal_received` | `False` (default) | Piena severità — fail-closed |

Il flag `boot` è `True` **solo** nel gate di bootstrap di `start()` e nel
preflight; ogni chiamante di runtime usa il default `False`. Così una
disconnessione **durante** il trading fa fallire il gate (fail-closed), mentre
al boot — quando la connessione non è ancora avvenuta — non è un falso blocker.

Il kwarg `tolerate_pending_connection` viene passato al probe **solo se il getter
lo accetta davvero** (verifica via `inspect.signature`): non si usa
`try/except TypeError`, che mascherebbe un `TypeError` sollevato *dentro*
`get_live_readiness_report` degradando silenziosamente a strict.

### Perché la tolleranza al boot (#361)

La connessione a Betfair avviene **dentro `start()`** (`RuntimeController.start` →
`betfair_service.connect(...)`), che gira **dopo** il deploy gate. Al momento del
gate, quindi, `betfair_service` è legittimamente **disconnesso**. Senza tolleranza
il probe risulterebbe `NOT_READY`/`DEGRADED` e il gate bloccherebbe LIVE — ma la
connessione non può avvenire prima del gate: **deadlock**. La presenza e la
connettibilità di Betfair sono già verificate a monte da `evaluate_live_readiness`
(`has_live_dependency`), e un fallimento reale della connessione viene gestito in
`start()` (raise). Quindi al boot un componente `DEGRADED` **solo** perché
`disconnected` non è un vero blocker.

Al boot il rilassamento dei `DEGRADED` `disconnected` è ristretto a una
**allowlist** esplicita (`_BOOT_PENDING_CONNECTION_COMPONENTS`, oggi il solo
`betfair_service`) **e** solo se non c'è alcun degrado reale nei details
(`external_io` DEGRADED/UNAVAILABLE/SLOW, oppure `streaming_feed` con
`auth_degraded`/`keepalive_failure_count`/`degraded_503`): un `DEGRADED` misto
(disconnected + IO/streaming degradato) **non** viene promosso a READY. Ogni
altro `DEGRADED` reale (es. `is_ready()` → False = `unhealthy`) **resta bloccante**
anche al boot. A runtime (watchdog e gate di enforcement, default `False`) la
disconnessione torna a essere segnalata.

### Componenti senza checker (#361)

Un componente presente ma **senza metodo `is_ready`** viene marcato dal probe
come `UNKNOWN` con reason `no-checker` (`_probe_ready_component` →
`_unknown_probe`, che imposta `fallback_status=READY`). Questi componenti —
presenti ma privi di un'interfaccia di readiness — **non** contano come blocker,
**né al boot né a runtime** (rilassamento *non* phase-aware). Il motivo: un
componente `no-checker` non diventerà **mai** `ready` (non espone il segnale),
quindi renderlo bloccante a runtime disabiliterebbe LIVE in modo **permanente**
dopo un avvio riuscito (`is_live_allowed`/`_on_signal_received`/watchdog
rifiuterebbero ogni segnale). Non è un fail-open: l'assenza di checker non è un
degrado reale, e la presenza/connettibilità dei componenti è già verificata da
`evaluate_live_readiness`. Il rilassamento è ristretto a reason `no-checker` con
`fallback_status=READY`: altri `UNKNOWN` (es. trading_engine
`ready_without_health`) **restano fail-closed** in ogni fase.

> Differenza chiave con `disconnected` (B-2): la disconnessione **è** un degrado
> reale a runtime (ordini verso un servizio down), quindi è tollerata **solo** al
> boot. Il `no-checker` (B-1) non è un degrado, quindi è tollerato **sempre**.

### Checker reali per database e shutdown_manager (#363)

Il rilassamento `no-checker` (B-1) è sicuro **solo** per componenti la cui
salute è verificata altrove o che non hanno stato sanitario proprio. Per
`database` e `shutdown_manager` non era così: erano `no-checker` ma con uno
stato di salute reale (DB raggiungibile? shutdown in corso?) che il gate a
runtime non poteva vedere. #363 li dota di `is_ready` reali:

- **`Database.is_ready()`** — health-check **non distruttivo** e in sola
  lettura: verifica che le **tabelle critiche** del money path
  (`_READINESS_CRITICAL_TABLES`: `settings`, `order_saga`) siano presenti. Non
  basta il motore (`SELECT 1`): un DB vuoto/ricreato senza schema non deve
  risultare pronto. Apre una connessione **read-only** via URI
  (`file:…?mode=ro`): se il file manca `connect` solleva (fail-closed) e **non
  lo crea** → nessun TOCTOU di ricreazione; non scrive nulla e non tocca la
  connessione persistente `self._local.conn` (nessuna riapertura/resurrezione
  durante lo shutdown, nessuna affinità di thread). Il DB è sempre in
  `journal_mode=WAL` (durability profile), quindi la lettura è concorrente col
  writer senza contendere il lock. Fail-closed su errore o schema incompleto →
  un DB compromesso è `DEGRADED` (`unhealthy`) e **blocca LIVE** in ogni fase.
  Il ramo `:memory:` (solo test, single-thread) usa la connessione persistente
  perché un DB in-memory non è apribile via URI file.
- **`ShutdownManager.is_ready()`** — `not _has_run`, lettura **lock-free**
  (flag atomico, nessun `self._lock`: niente contesa/deadlock con `shutdown()`
  o con gli hook). Durante/dopo lo shutdown → `DEGRADED` → il gate a runtime
  **non** permette nuovi ordini LIVE.

Con questi checker i due componenti **non** rientrano più in B-1: sono valutati
a piena severità. `RuntimeController` resta invece volutamente `no-checker`: la
sua prontezza è già coperta da `evaluate_live_readiness`
(`runtime_initialized`/`mode`/`half_started`), quindi B-1 su di esso non lascia
scoperto nulla. Il probe non cambia (`_probe_ready_component` consuma già
`is_ready`).

## Postura di sicurezza

Il gate resta significativo: verifica prerequisiti strutturali, kill switch,
key source, hard-stop config, e i degradi **reali** dei componenti (streaming/IO,
`unhealthy`). La modifica #361 rimuove **solo** il deadlock di bootstrap, senza
indebolire il monitoraggio a runtime: i gate di enforcement (`is_live_allowed`,
`_on_signal_received`) e il watchdog restano strict, quindi una disconnessione o
un componente non-ready **durante** il trading fa fallire il gate (fail-closed).
