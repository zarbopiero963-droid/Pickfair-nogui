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
editabili dal tab **Roserpina**: 2 campi numerici ("Liquidity avviso:
Moltiplicatore" / "Liquidity avviso: Floor assoluto €") + 1 toggle "Liquidity
Guard: avviso liquidita' (no blocco)".

> **`liquidity_warning_only` non e' esposto in GUI in questa fase.** Poiche' il
> guard e' solo osservazionale (non blocca mai), un toggle "Warning Only" sarebbe
> un controllo di safety visibile ma **inerte/fuorviante** (un operatore con
> `warning_only=False` crederebbe di avere un blocco che non esiste). Il campo
> resta nel config/DB (default `False`) riservato alla PR di follow-up che abilita
> il blocco; verra' esposto in GUI insieme al blocco reale.

**Modalita' OSSERVAZIONALE (warning-only) — decisione owner.** In questa fase il
guard **NON blocca** il submit: calcola la liquidita' **eseguibile** per gamba e
segnala uno shortfall come **warning** (`liquidity_warning`/`liquidity_shortfall`
nel risultato del `precheck`). Il **BLOCCO** reale — che onorera'
`liquidity_warning_only=False` — e' rimandato a una **PR di follow-up** dopo aver
verificato la semantica esatta (direzione ladder sul book reale e unita' di
misura), per non rischiare di bloccare bet valide o passare quelle da fermare.

**Calcolo** (`controllers/dutching_controller.precheck`, dopo il book% gate):
per ogni gamba somma la liquidita' del lato OPPOSTO del book (BACK →
`availableToLay`, LAY → `availableToBack`, mirror del matcher
`simulation_order_book`), **filtrata per prezzo eseguibile** (mirror di
`_crosses`: BACK conta i livelli con `book_price <= order_price`, LAY il
contrario), letta dalla **cache** `runtime.market_tracker.get_market` — **nessun
I/O di rete** nel path di submit. Richiesta = `stake * multiplier` (size da
matchare = stake, non la liability). Shortfall se `available < max(min_absolute,
required)`.

**FAIL-OPEN su dato mancante.** Se il book non e' in cache o la selezione non e'
nel book, non si emette warning (dato ignoto). **Toggle/fail-safe:**
`Liquidity Guard Enabled = False` disattiva anche l'osservazione (scelta
operatore, non dato corrotto); `multiplier` fallback fail-safe `> 0`,
`min_absolute` `>= 0` (0 = nessun floor). `MIN_LIQUIDITY` (trading_config) resta
non usata (superata da `min_liquidity_absolute`).

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

## Vincoli (safety)

- Non modifica la logica dei gate (deploy gate, fail-closed #350): **espone e
  configura**, non allenta. Nessun prerequisito LIVE indebolito o bypassato.
- Nessun segreto stampato in chiaro.
