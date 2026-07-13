# Deploy Gate LIVE — readiness al boot vs a runtime

Il deploy gate decide se il runtime può partire in **LIVE**. La valutazione usa
`RuntimeController.evaluate_live_readiness` (prerequisiti strutturali) **più** il
**probe** di readiness (`observability/runtime_probe.py::get_live_readiness_report`),
che aggrega lo stato dei componenti (betfair_service, database, runtime_controller,
shutdown_manager, trading_engine, …).

## Boot vs runtime (phase-aware)

Il probe è consultato in **due fasi diverse**, con severità diversa:

| Fase | Chiamante | Comportamento |
|---|---|---|
| **Boot** (pre-connessione) | `RuntimeController._get_probe_live_readiness_report` → `get_live_readiness_report(tolerate_pending_connection=True)` | Tollera i componenti *pending connection* |
| **Runtime** (monitoraggio) | `watchdog_service` → `get_live_readiness_report()` (default) | Piena severità |

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

`tolerate_pending_connection=True` rilassa **esclusivamente** i `DEGRADED` con
reason `disconnected`. Ogni altro `DEGRADED` reale (es. `is_ready()` → False =
`unhealthy`, oppure degrado di streaming/IO) **resta bloccante** anche al boot.
A runtime (watchdog, default `False`) la disconnessione torna a essere segnalata:
una disconnessione **durante** il trading resta rilevata.

### Componenti senza checker (#361)
Un componente presente ma **senza metodo `is_ready`** viene marcato dal probe
come `UNKNOWN` con reason `no-checker` (`_probe_ready_component` →
`_unknown_probe`). Questi componenti sono presenti e sani ma non espongono
un'interfaccia di readiness: **non** contano come blocker (prima erano blocker
spuri che contribuivano al deadlock LIVE al boot). Il rilassamento è ristretto a
reason `no-checker`: altri `UNKNOWN` (es. trading_engine `ready_without_health`)
**restano fail-closed**.

## Postura di sicurezza
Il gate resta significativo: verifica prerequisiti strutturali, kill switch,
key source, hard-stop config, e i degradi **reali** dei componenti (streaming/IO,
`unhealthy`). La modifica #361 rimuove **solo** il deadlock di bootstrap, senza
indebolire il monitoraggio a runtime.
