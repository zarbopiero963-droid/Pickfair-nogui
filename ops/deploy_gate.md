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
`_unknown_probe`, che imposta `fallback_status=READY`). **Solo al boot** questi
componenti — presenti e sani ma senza interfaccia di readiness — non contano come
blocker (prima erano blocker spuri che contribuivano al deadlock LIVE al boot). Il
rilassamento è ristretto a reason `no-checker` con `fallback_status=READY`: altri
`UNKNOWN` (es. trading_engine `ready_without_health`) **restano fail-closed**. A
runtime (default) anche i `no-checker` tornano bloccanti: piena severità.

## Postura di sicurezza

Il gate resta significativo: verifica prerequisiti strutturali, kill switch,
key source, hard-stop config, e i degradi **reali** dei componenti (streaming/IO,
`unhealthy`). La modifica #361 rimuove **solo** il deadlock di bootstrap, senza
indebolire il monitoraggio a runtime: i gate di enforcement (`is_live_allowed`,
`_on_signal_received`) e il watchdog restano strict, quindi una disconnessione o
un componente non-ready **durante** il trading fa fallire il gate (fail-closed).
