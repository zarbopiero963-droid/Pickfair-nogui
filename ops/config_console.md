# Config & Go-Live Console (#359)

Punto unico per **configurazioni** e **prontezza LIVE**, con **contenuto
identico** su GUI (Windows) e headless (Linux). Filosofia: l'operativita' si
sistema **via configurazione**, non via patch/deploy.

Questo documento copre **PR-A**: il registro di configurazione (fonte-dato
unica). Le superfici (headless `--config-report`, tab GUI) lo consumeranno nelle
PR successive senza duplicare l'enumerazione.

## `config_registry.py` — fonte-dato unica

Modulo **di sola lettura**: non scrive impostazioni e **non modifica nessun
gate**. Legge da `SettingsService` (DB) e da `trading_config` (costanti).

### `ConfigRegistry(settings_service, runtime=None)`

- `entries() -> list[ConfigEntry]`: enumera ogni parametro configurabile per
  dominio (esecuzione/safety, Betfair, trading & money-management, Telegram,
  simulazione) come:

  | campo | significato |
  |-------|-------------|
  | `key` | chiave stabile (es. `betfair.app_key`) |
  | `label` | etichetta leggibile |
  | `value` | valore attuale (**mascherato** se segreto) |
  | `valid` | il valore e' valido/utilizzabile |
  | `required_for_live` | prerequisito per andare LIVE |
  | `source` | `DB` / `code` / `runtime` |
  | `remedy` | come sistemarlo, se non valido |
  | `is_secret` | il valore reale non e' mai esposto |

- `readiness(...)`: scorciatoia per `readiness_report` sul `runtime` associato.

### Mascheramento segreti (obbligatorio)

`app_key`, `certificate`, `private_key`, `password` non sono **mai** esposti in
chiaro: la entry mostra solo `(impostato)` / `(non impostato)` e `valid`
riflette la presenza. Vincolo coperto da test (`test_secrets_never_appear_in_plaintext`).

## `readiness_report(runtime, *, execution_mode, live_enabled, live_readiness_ok)`

Checklist ✅/❌ dei prerequisiti LIVE. **Riusa** l'autorita' unica
`RuntimeController.evaluate_live_readiness` (#350): non reimplementa ne'
indebolisce la logica del gate. Mappa i `blockers` restituiti dal gate sulla
lista ordinata dei prerequisiti, ognuno con il rimedio da `BLOCKER_REMEDIATION`.

Ritorna `{ready, level, blockers, items, details}` dove `items` e' una lista di
`ReadinessItem{key, label, ok, blocker, remedy}`. Prerequisiti coperti:
execution_mode valido, `live_enabled`, `live_readiness_ok`, sorgente chiave
sicura, hard-stop presente/valido, kill switch off, safe mode, dipendenza
Betfair, runtime inizializzato/non half-started, nessuno stato contraddittorio,
segnale readiness noto, nessun errore di avvio.

`BLOCKER_REMEDIATION` e' la **fonte unica** dei rimedi: in PR-B
`headless_main._BLOCKER_REMEDIATION` verra' deduplicato importando da qui.

## Roadmap epic #359

- [x] **PR-A** — `ConfigRegistry` + `readiness_report()` (questo modulo, solo dato + test).
- [ ] **PR-B** — headless `--config-report` (+ `--preflight` gia' presente) che consuma il registry.
- [ ] **PR-C/D** — tab GUI (Go-Live Readiness + config) sopra lo stesso registry.
- [ ] **PR-E** — logging in chiaro delle decisioni gate + preflight nel servizio.

## Vincoli (safety)

- Non modifica la logica dei gate (deploy gate, fail-closed #350): **espone e
  configura**, non allenta. Nessun prerequisito LIVE indebolito o bypassato.
- Nessun segreto stampato in chiaro.
