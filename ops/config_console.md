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

## Liquidity guard editabile in GUI (tab Roserpina) — PR2b + BLOCCO reale (#383)

Le costanti liquidita' (`LIQUIDITY_GUARD_ENABLED`, `LIQUIDITY_MULTIPLIER`,
`MIN_LIQUIDITY_ABSOLUTE`, `LIQUIDITY_WARNING_ONLY`) erano *dead*. Ora sono
editabili dal tab **Roserpina**: 2 campi numerici ("Liquidity avviso:
Moltiplicatore" / "Liquidity avviso: Floor assoluto €") + 2 toggle:
"**Liquidity Guard attivo**" e "**Liquidity: solo avviso (⚠ togliere =
BLOCCA il submit)**".

**BLOCCO reale (opt-in) — #383.** Il guard ora puo' **bloccare** il submit se la
liquidita' eseguibile e' insufficiente. E' **opt-in**: `liquidity_warning_only`
di default e' **True** (`trading_config.LIQUIDITY_WARNING_ONLY = True`), quindi il
rilascio parte in **AVVISO** e non blocca a sorpresa; l'operatore **arma il
blocco** togliendo la spunta "solo avviso" in GUI (`warning_only=False`). In
modalita' avviso segnala soltanto (`liquidity_warning`/`liquidity_shortfall` nel
`precheck`); in modalita' blocco fa `_fail("Liquidità insufficiente…")` PRIMA di
ogni side-effect, come il book% gate.

**Calcolo** (`controllers/dutching_controller.precheck`, dopo il book% gate): per
ogni gamba somma la liquidita' del lato **Betfair-standard** del book — **BACK →
`availableToBack`**, **LAY → `availableToLay`** (e' la liquidita' che l'ordine
matcha davvero sul mercato reale; coerente con `direct_best_price`/
`betfair_client`) — **filtrata per prezzo eseguibile** (BACK conta i livelli con
`book_price >= order_price`, LAY con `book_price <= order_price`), letta dalla
**cache** `runtime.market_tracker.get_market` — **nessun I/O di rete** nel path di
submit. Richiesta = `stake * multiplier` (le `size` del book e lo `stake` sono
backer-stake, omogenei; per il LAY lo `stake` e' backer-stake, la liability e' un
campo separato). Shortfall se `available < max(min_absolute, required)`.

> Nota: la direzione ladder e' **invertita** rispetto al mirror del matcher
> interno del simulatore (che modella ordini in attesa: BACK→availableToLay). Sul
> book reale (feed Betfair) i campi sono quelli standard, quindi il blocco usa
> `availableToBack` per un BACK. Decisione owner in #383.

**FAIL-OPEN su dato mancante.** Se il book non e' in cache o la selezione non e'
nel book, non si emette shortfall (dato ignoto) => **nessun blocco** (cache
fredda/riconnessione e' transitoria; fail-closed affamerebbe la strategia).
**Toggle/fail-safe:** `Liquidity Guard Enabled = False` disattiva del tutto
(scelta operatore); `multiplier` fallback fail-safe `> 0`, `min_absolute` `>= 0`
(0 = nessun floor); `liquidity_warning_only` assente ricade su
`trading_config.LIQUIDITY_WARNING_ONLY` (True = avviso). `MIN_LIQUIDITY`
(trading_config) resta non usata (superata da `min_liquidity_absolute`).

**Safety.** Il blocco e' money-management (submit path): questa PR e'
**safety-critical → merge manuale dell'owner**, e la checkbox "solo avviso" ha
label di warning.

## Quota minima / Min Price editabile in GUI (tab Roserpina) — PR2c

`trading_config.MIN_PRICE` (1.02) era *dead*; il floor reale al submit era il
solo `price <= 1.01` hardcoded. Ora il tab **Roserpina** espone "Quota minima /
Min Price (>= 1.01)": un **floor di strategia editabile** applicato al submit
dutching **SOPRA** il minimo Betfair inviolabile 1.01.

**Enforcement** (`controllers/dutching_controller.validate`, quindi anche
`precheck`/`preview`): oltre al floor **hard** `price <= 1.01` (minimo Betfair,
**immune-da-config**), rifiuta `price < min_price` (config-driven). Comparatore
`<`: col default **1.02** il comportamento e' **invariato** rispetto a prima
(sulla ladder Betfair — step 0.01 in 1.01–2.00 — non esiste un tick tra 1.01 e
1.02), quindi il floor "morde" solo quando l'operatore lo alza (es. 1.5).

**Fail-safe.** `_min_price` clampa a `max(1.01, trading_config.MIN_PRICE)` se la
config e' assente / non numerica / non finita / `< 1.01`: una config rotta non
puo' abbassare il floor sotto il minimo Betfair. Il floor hard 1.01 resta
comunque un controllo indipendente. La validazione GUI richiede `>= 1.02` (il
minimo realmente raggiungibile: 1.01 e' rifiutato dal floor hard, quindi
impostare 1.01 sarebbe identico a 1.02 e fuorviante).

**Ambito.** Il floor di strategia si applica al **path dutching automatico**;
`manual_bet()` resta col solo hard-floor 1.01 (una bet manuale e' una scelta
esplicita dell'operatore). `MIN_LIQUIDITY` resta non usata (superata da
`min_liquidity_absolute`, PR2b).

## Max Win: cap vincita/payout per gamba editabile in GUI (tab Roserpina) — G5

`trading_config.MAX_WIN` (10000.0) era *dead* (solo voce display in
`config_registry`). Ora il tab **Roserpina** espone il campo "**Max Win €**" +
la checkbox "**Max Win: solo avviso (⚠ togliere = BLOCCA il submit)**": un **cap
di sicurezza sulla vincita/payout potenziale per gamba**, applicato sia al submit
**dutching** sia al **bet manuale**.

**Grandezza.** La vincita potenziale di UNA gamba e': **BACK** → `stake * price`
(payout lordo restituito se la selezione vince); **LAY** → `stake` (backer-stake
incassato se la selezione perde; il rischio LAY e' la *liability*, grandezza
diversa non confrontata col cap). Il cap e' **per-gamba**: gli esiti di un set
dutching sono mutuamente esclusivi (vince una sola selezione), quindi si valuta
la vincita **massima** tra le gambe, **non** la somma. Non si usa
`profitIfWins`/`avg_profit` (profitto equalizzato netto-del-totale, ordine di
grandezza diverso).

**Enforcement** (`controllers/dutching_controller.precheck` e `manual_bet`):
il gate gira **PRIMA di ogni side-effect** (duplication acquire), accanto ai gate
book%/liquidity. Se una gamba supera il cap **e** il guard non e' in sola
osservazione (`max_win_warning_only=False`) => `_fail` (blocco). In modalita'
avviso espone `max_win_warning`/`max_win_breaches` nel risultato **senza mai
bloccare**.

**Opt-in (default).** `max_win_warning_only=True` di default (come il liquidity
guard #383): il cap parte in **AVVISO**; l'owner lo **arma** a blocco reale
togliendo la spunta in GUI. Cosi' il rilascio non inizia a bloccare a sorpresa.

**Fail-safe.** `_max_win` (riusa `_book_threshold`) ricade su
`trading_config.MAX_WIN` se la config e' assente / non numerica / non finita /
`<= 0`: una config rotta **non** puo' disattivare il cap. La validazione GUI
richiede un numero finito `> 0` (niente drift GUI-vs-enforcement). Persistenza:
chiavi DB `roserpina.max_win` / `roserpina.max_win_warning_only`.

**Nota vs `max_stake_abs`.** `max_stake_abs` (money-management) e' un cap sullo
**stake** (sizing); `MAX_WIN` e' un cap sul **payout/vincita** potenziale: assi
diversi, complementari, nessun doppio-conteggio.

## Configurazione Simulazione editabile in GUI (tab Simulazione) — G1

I parametri del **broker simulato** erano configurabili solo via DB. Ora il tab
**Simulazione** li espone (attivi solo in modalita' SIMULAZIONE):

- **Bankroll iniziale simulazione** (`simulation.starting_balance`, default 1000,
  validato `> 0`)
- **Partial fill abilitato** (`simulation.partial_fill_enabled`)
- **Consuma liquidita' del book** (`simulation.consume_liquidity`)
- **Persisti stato simulazione** (`simulation.persist_state`)

**Gia' enforced.** Questi valori sono gia' consumati dal `SimulationBroker`
(`services/betfair_service.py`): la PR e' pura **esposizione + validazione**,
nessun nuovo enforcement e nessuna modifica al percorso LIVE.

**Campi NON esposti (preservati al salvataggio):**

- `simulation.commission_pct` e' **policy-locked a 4.5%** (Betfair Italia,
  fail-closed in `core/simulation_state.py`): esporlo editabile farebbe fallire
  il settlement PnL se ≠ 4.5. Quindi **non e' in GUI**; il salvataggio ricarica
  la config corrente e **preserva** il valore persistito.
- `simulation.enabled` non e' esposto qui (la modalita' SIM/LIVE si governa dalla
  top-bar): viene anch'esso preservato.

## Toggle Watchdog anomalie editabili in GUI (tab Watchdog) — G2

I tre toggle del watchdog anomalie erano configurabili solo via DB. Ora il tab
**Watchdog** li espone (toggle **LIVE**: il watchdog li rilegge ad ogni tick, si
applicano entro **~5s senza riavvio**):

- **Watchdog anomalie ATTIVO** (`anomaly_enabled`) — ⚠ guardia di safety:
  disattivarlo sopprime le scansioni anomalie e registra un incidente fail-loud
  ad ogni tick.
- **Notifiche Telegram delle anomalie** (`anomaly_alerts_enabled`).
- **Azioni automatiche su anomalie** (`anomaly_actions_enabled`) — ⚠ arma il
  seam delle azioni automatiche (oggi il hook di escalation non e' collegato in
  produzione: imposta solo il flag di escalation + log).

**Gia' enforced.** Consumati da `observability/watchdog_service.py`
(`_is_anomaly_*`) e caricati da `headless_main.py`. La PR e' pura **esposizione +
validazione**: il service (`services/settings_service.py`,
`load/save_anomaly_*`) e' gia' completo, **nessuna modifica al backend**.

**Semantica tri-state.** `anomaly_enabled` non configurato (chiave assente) =
**default-ON**: la checkbox parte spuntata. Al primo salvataggio si scrive un
booleano **esplicito** (`0`/`1`): togliere la spunta persiste `False` = watchdog
disattivato (True e None restano entrambi = attivo). Non esiste un ritorno allo
stato "non configurato" via GUI (per il watchdog e' irrilevante: None == attivo).

**Safety.** Espone un off-switch di una guardia di safety e un gate di azioni
automatiche: le checkbox critiche hanno label di **warning** e il merge di questa
PR e' **manuale dell'owner** (safety-relevant), pur toccando solo GUI+test+doc.

## Impostazioni Alert Telegram editabili in GUI (tab Alert) — G3

Il routing e le soglie delle notifiche di alert su Telegram erano configurabili
solo via DB. Ora il tab **Alert** le espone (distinte dalle credenziali di
sessione nel tab Telegram). Effetto **LIVE** (il service rilegge ad ogni alert,
nessun riavvio):

- **Alert Telegram ATTIVI** (`alerts_enabled`) — ⚠ disattivarli sopprime anche
  gli alert di anomalia/incidente su cui l'operatore fa affidamento.
- **Chat ID destinazione** (`alerts_chat_id`, testo libero — **obbligatorio se
  alert attivi**) e **Nome chat** (`alerts_chat_name`, facoltativo).
- **Severita' minima** (`min_alert_severity`) — dropdown vincolato:
  `INFO / WARNING / ERROR / HIGH / CRITICAL`.
- **Cooldown anti-spam** (`alert_cooldown_sec`, intero `>= 0`).
- **Deduplica** (`alert_dedup_enabled`) e **Formato ricco** (`alert_format_rich`).

**Gia' enforced.** Consumati live da `services/telegram_alerts_service.py`
(`notify_alert`, `_should_send`, cooldown/dedup, `_format_alert_text`). La PR e'
pura **esposizione + validazione**: il service
(`services/settings_service.py`/`setting_service.py`: `load_telegram_config_row`
+ `save_telegram_alert_settings`) e' gia' completo, **nessuna modifica al
backend**. Il saver dedicato **read-merge** la riga telegram, quindi le
credenziali di sessione **sono preservate**.

**Validazione.** Severita' vincolata dal dropdown; cooldown intero `>= 0`;
`chat_id` obbligatorio quando gli alert sono attivi (altrimenti il service farebbe
un no-op silenzioso `alerts_chat_id_missing`).

**Safety.** Espone un off-switch di notifiche di safety: la checkbox principale
ha label di **warning** e il merge di questa PR e' **manuale dell'owner**
(safety-relevant), pur toccando solo GUI+test+doc.

## Vincoli (safety)

- Non modifica la logica dei gate (deploy gate, fail-closed #350): **espone e
  configura**, non allenta. Nessun prerequisito LIVE indebolito o bypassato.
- Nessun segreto stampato in chiaro.
