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

## Hard-stop giornalieri editabili in GUI (tab Roserpina)

I tre hard-stop giornalieri — `max_daily_loss`, `max_open_exposure`,
`max_drawdown_hard_stop_pct` (prerequisiti del gate LIVE
`_validate_live_hard_stop_config`) — sono editabili dalla GUI nel tab
**Roserpina** (`mini_gui.py`), oltre che via DB/`config.json`. Semantica
**fail-closed**:

- **campo vuoto = non impostato (`None`)**: il salvataggio **preserva** il
  valore gia' persistito (`save_roserpina_config` scrive gli hard-stop solo se
  non-`None`) e, se il campo non e' mai stato configurato, il gate LIVE resta
  **bloccante** (`LIVE_HARD_STOP_CONFIG_MISSING`). Un campo vuoto non azzera un
  limite di sicurezza e non produce un valore fasullo;
- **valore presente**: deve essere numerico, finito e **> 0** (per il drawdown
  `%` anche **≤ 100**), coerente con il gate; un valore non valido interrompe il
  salvataggio con errore (nessuna scrittura), invece di far passare `0`/negativi
  che aggirerebbero il gate.

La GUI **non** setta `live_readiness_ok` ne' bypassa il deploy gate: si limita a
scrivere gli stessi `roserpina.*` che il gate legge. Il valore reale con
Betfair/LIVE resta verificato dal gate a runtime.

## Book % gate (over-round) editabile in GUI (tab Roserpina) — PR2a

Le soglie book% `book_warning` (default `trading_config.BOOK_WARNING`=105) e
`book_block` (default `trading_config.BOOK_BLOCK`=110) sono editabili dal tab
**Roserpina** (campi "Book Warning %" / "Book Block %"), oltre che via DB
(`roserpina.book_warning` / `roserpina.book_block`).

**Enforcement reale (enforce-first).** Prima queste costanti erano *dead* (il
`book_pct` veniva calcolato ma mai confrontato: nessun blocco). Ora
`controllers/dutching_controller.precheck` **blocca il submit** del dutching se
`book_pct >= book_block`, eseguito **prima di ogni side-effect** (duplication
acquire). Il flag non-bloccante `book_warning_exceeded` (`book_pct >=
book_warning`) è aggiunto al risultato del precheck.

**Fonte-dato + fallback fail-safe.** La soglia è letta da `RoserpinaConfig`
(editabile da GUI); se il valore config è assente / non numerico / non finito /
`<= 0`, si ricade sulla **costante di sicurezza** `trading_config.BOOK_BLOCK`:
una config rotta o corrotta **non disattiva il gate** né lo sposta su un valore
assurdo. Editare `book_block` a un valore alto è una scelta consapevole
dell'operatore (allenta il gate); il fallback protegge solo i casi invalidi.

## Liquidity guard editabile in GUI (tab Roserpina) — PR2b

Le costanti liquidita' (`LIQUIDITY_GUARD_ENABLED`, `LIQUIDITY_MULTIPLIER`,
`MIN_LIQUIDITY_ABSOLUTE`, `LIQUIDITY_WARNING_ONLY`) erano *dead*. Ora sono
editabili dal tab **Roserpina** (2 campi numerici "Liquidity: Moltiplicatore" /
"Liquidity: Floor assoluto €" + 2 toggle "Liquidity Guard Enabled" / "Liquidity
Warning Only") e applicate come **gate reale** al submit dutching.

**Enforcement** (`controllers/dutching_controller.precheck`, dopo il book% gate e
**prima di ogni side-effect**): per ogni gamba legge la liquidita' disponibile
dal **market book** — lato OPPOSTO (BACK → `availableToLay`, LAY →
`availableToBack`, coerente con `simulation_order_book`) — sommando le `size`,
via `runtime.market_tracker.get_market` (cache) con fallback
`betfair_service.get_market_book_snapshot`. Richiesta = `stake * multiplier` (per
LAY = `stake*(price-1) * multiplier`); **blocca** se `available < max(min_absolute,
required)`, oppure **avvisa** (flag `liquidity_warning`/`liquidity_shortfall` nel
risultato, nessun blocco) se `warning_only`.

**FAIL-OPEN su dato mancante (decisione owner).** Se la liquidita' non e'
ottenibile (book assente/freddo, snapshot fallito, selezione non nel book), il
gate **non blocca**: un feed freddo non ferma mai le scommesse (gli altri gate —
book%, esposizione, hard-stop — restano attivi). Il guard blocca solo con
liquidita' **nota** e insufficiente.

**Toggle e fail-safe.** `Liquidity Guard Enabled = False` (scelta esplicita
dell'operatore) **disattiva** il gate — non e' trattato come dato corrotto. I
valori numerici hanno fallback fail-safe alle costanti `trading_config`:
`multiplier` deve essere `> 0`, `min_absolute` `>= 0` (0 = nessun floor assoluto).
`MIN_LIQUIDITY` (trading_config) resta non usata (superata da
`min_liquidity_absolute`) per evitare due floor concorrenti.

## Vincoli (safety)

- Non modifica la logica dei gate (deploy gate, fail-closed #350): **espone e
  configura**, non allenta. Nessun prerequisito LIVE indebolito o bypassato.
- Nessun segreto stampato in chiaro.
