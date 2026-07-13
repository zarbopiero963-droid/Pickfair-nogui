# Go-Live Preflight (`--preflight`)

Comando **read-only** che risponde a una sola domanda: *«il sistema e' pronto
per andare LIVE, e se no, cosa manca?»* — senza connettersi a Betfair, senza
avviare il trading e senza modificare alcuno stato.

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

Poi valuta i prerequisiti tramite `RuntimeController.evaluate_live_readiness`
(la **fonte autorevole** del deploy gate) e stampa il risultato.

## Output

- **Pronto**: intestazione + `PRONTO PER LIVE — nessun blocker.`
- **Non pronto**: per ogni blocker vengono stampati **codice**, *cosa significa*
  e *come si rimedia*. Esempio:

```
================================================================
PICKFAIR — GO-LIVE PREFLIGHT
================================================================
execution_mode richiesto : LIVE
live_enabled             : True
live_readiness_ok        : False
readiness level          : NOT_READY
----------------------------------------------------------------
NON PRONTO — 2 blocker:
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
| `0`  | Pronto per LIVE (nessun blocker) |
| `2`  | Non pronto (uno o piu' blocker) — vedi checklist |
| `1`  | Errore di bootstrap/build (il runtime non e' stato costruito) |

L'exit code `2` rende il preflight utilizzabile come **gate in uno script o nel
servizio** prima di avviare il runtime in LIVE.

## Blocker catalogati

I codici provengono da `RuntimeController.evaluate_live_readiness`; il preflight
li mappa a descrizione + rimedio (vedi `HeadlessApp._BLOCKER_REMEDIATION`):
`LIVE_NOT_ENABLED`, `LIVE_READINESS_FLAG_NOT_OK`, `LIVE_KEY_SOURCE_UNSAFE`,
`LIVE_HARD_STOP_CONFIG_MISSING/INVALID`, `KILL_SWITCH_ACTIVE`,
`SAFE_MODE_BLOCKING`, `LIVE_DEPENDENCY_MISSING`, `INVALID_EXECUTION_MODE`,
`CONTRADICTORY_STATE`, `RUNTIME_NOT_INITIALIZED`, `RUNTIME_HALF_STARTED`,
`STARTUP_FAILED`, `READINESS_SIGNAL_UNKNOWN`.

## Vincoli

- **Read-only**: non si connette, non avvia trading, non persiste nulla, non
  modifica la logica dei gate. Espone soltanto lo stato di readiness.
- La logica dei prerequisiti resta in `RuntimeController.evaluate_live_readiness`
  (deploy gate #350 / go-live FASE 3.2): il preflight la **mostra**, non la
  duplica ne' la indebolisce.
