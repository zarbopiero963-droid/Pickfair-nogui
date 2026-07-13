# Go-Live Preflight (`--preflight`)

Comando **read-only** che risponde a una sola domanda: *«il sistema è pronto
per andare LIVE, e se no, cosa manca?»* — senza connettersi a Betfair, senza
avviare il trading, senza avviare i servizi di osservabilità e senza modificare
alcuno stato.

Nasce da #359 (Config & Go-Live Console): prima, il motivo per cui il runtime
rifiutava LIVE (deploy gate NO-GO) restava **sepolto nel log**; il preflight lo
rende **visibile a schermo** come checklist con blocker + rimedio.

## Uso

```bash
python3 headless_main.py --preflight --live --live-enabled
```

Il preflight usa gli **stessi flag** di un avvio normale per determinare lo
stato richiesto:

- `--live` / `--simulation` → `execution_mode`
- `--live-enabled` / `--live-disabled` → `live_enabled`
- `live_readiness_ok` è letto dal DB (`load_live_readiness_ok`)

Poi interroga **lo stesso gate che usa `start()`** —
`RuntimeController.get_deploy_gate_status` (la variante *read-only* di
`enforce_deploy_gate`: nessun side effect di log/attributi) — così il verdetto
riflette **esattamente** ciò che `start()` deciderebbe, inclusi i probe LIVE
(streaming/bankroll/reconciliation).

Se lo esegui **senza `--live`** (quindi in SIMULATION), il preflight LIVE non è
applicabile: stampa una nota esplicita, non dichiara mai «pronto per LIVE» ed
esce con codice **3** (non 0), così un uso come gate CI/script senza i flag LIVE
**non passa "verde"** (fail-closed).

## Read-only garantito

- Il flag `--preflight` è intercettato **prima** dell'avvio dei servizi: la
  costruzione avviene con `build(start_services=False)`, quindi i thread di
  osservabilità (watchdog/cleanup) **non** partono.
- Non viene eseguito il boot recovery e non parte alcun trading: dopo la
  stampa il processo esce con il codice appropriato.

## Output

- **Pronto** (solo in LIVE, gate GO): intestazione + `PRONTO PER LIVE — nessun blocker.`
- **Non pronto**: per ogni blocker vengono stampati **codice**, *cosa significa*
  e *come si rimedia*. Esempio:

```text
================================================================
PICKFAIR — GO-LIVE PREFLIGHT
================================================================
execution_mode richiesto : LIVE
live_enabled             : True
live_readiness_ok        : False
readiness level          : NOT_READY
----------------------------------------------------------------
NON PRONTO:
  [X] LIVE_READINESS_FLAG_NOT_OK
        cosa   : Il flag live_readiness_ok non e' confermato
        rimedio: Conferma la readiness LIVE (live_readiness_ok=True nel DB) ...
  [X] LIVE_HARD_STOP_CONFIG_MISSING
        cosa   : Config hard-stop giornaliero mancante
        rimedio: Imposta i campi hard-stop (perdita giornaliera max) ...
================================================================
```

## Exit code

| Code | Significato |
|------|-------------|
| `0`  | Pronto per LIVE (gate GO) |
| `2`  | Non pronto (uno o più blocker) — vedi checklist |
| `3`  | Preflight eseguito senza `--live` (LIVE non valutato) |
| `1`  | Errore di bootstrap/build (il runtime non è stato costruito) |

L'exit code `2` rende il preflight utilizzabile come **gate in uno script o nel
servizio** prima di avviare il runtime in LIVE (usa `--preflight --live --live-enabled`).

## Blocker catalogati

I codici provengono dal deploy gate (`RuntimeController.get_deploy_gate_status`
→ `evaluate_live_readiness` + probe LIVE); il preflight li mappa a descrizione +
rimedio (vedi `HeadlessApp._BLOCKER_REMEDIATION`): `LIVE_NOT_ENABLED`,
`LIVE_READINESS_FLAG_NOT_OK`, `LIVE_KEY_SOURCE_UNSAFE`,
`LIVE_HARD_STOP_CONFIG_MISSING/INVALID`, `KILL_SWITCH_ACTIVE`,
`SAFE_MODE_BLOCKING`, `LIVE_DEPENDENCY_MISSING`, `INVALID_EXECUTION_MODE`,
`CONTRADICTORY_STATE`, `RUNTIME_NOT_INITIALIZED`, `RUNTIME_HALF_STARTED`,
`STARTUP_FAILED`, `READINESS_SIGNAL_UNKNOWN`, `RUNTIME_LOCKDOWN`,
`LIVE_PROBE_NOT_READY`, e le reason di alto livello `DEPLOY_BLOCKED_*`. I codici
non catalogati vengono comunque mostrati (con guida generica), quindi la
checklist non tace mai un blocker.

## Vincoli

- **Read-only**: non si connette, non avvia trading/servizi, non persiste nulla,
  non modifica la logica dei gate. Espone soltanto lo stato di readiness.
- La logica dei prerequisiti resta in `RuntimeController` (deploy gate #350 /
  go-live FASE 3.2): il preflight la **mostra**, non la duplica né la indebolisce.

## Diagnostica NO-GO all'avvio (#358)

Anche **fuori** dal preflight, un avvio normale in LIVE (`--live`) il cui deploy
gate risulta NO-GO **stampa a schermo** la stessa checklist (blocker + rimedio),
non solo un warning sepolto nel log. Così, ad esempio, avviare con `--live` ma
**senza** `--live-enabled` mostra esplicitamente:

```text
NON PRONTO:
  [X] LIVE_NOT_ENABLED
        cosa   : live_enabled e' False
        rimedio: Avvia con --live-enabled oppure imposta live_enabled=True nel DB.
```

È **puramente diagnostico** (`HeadlessApp._emit_live_nogo_diagnostics`, che riusa
`_format_preflight_report`): non modifica la logica del gate né l'esito di
`start()` — l'enforcement fail-closed resta in `RuntimeController.start`. Serve a
rendere subito visibile *cosa manca per andare LIVE* invece di lasciare il motivo
nel log.

> Contesto #358: la Phase 0 ha stabilito che `--live` **non** connette in
> SIMULATION (con i prerequisiti mancanti il gate **rifiuta** LIVE, fail-closed);
> il sintomo osservato era codice *stale* sul VPS. Questa PR non cambia la
> sicurezza: rende solo evidente il motivo del NO-GO.
