# PF-CONTINUOUS-FORMAL-VERIFICATION-01: standard permanente di verifica

Decisione owner del 19/10/2026, registrata su #426, #461, #506 e #351. Questo file è la **specifica
versionata**: vale per ogni lavoro presente e futuro su Pickfair e si aggiorna solo tramite PR.

Lo standard non chiede più test, ma **le proprietà giuste verificate sulle interazioni reali**. Ogni
requisito sotto deve corrispondere a una verifica eseguibile. Una verifica non eseguita è `NOT_RUN`, mai
`PASS`.

## 1. Le sei metodologie e quando si applicano

| # | Metodologia | Si applica a | Si esclude quando | Strumento |
|---|---|---|---|---|
| M1 | Test unitari e d'integrazione | ogni modifica di codice (già obbligatori, CLAUDE.md) | mai | pytest |
| M2 | Property-based testing | funzioni pure di denaro (stake, liability, commissioni, cap) e macchine a stati con un modello di riferimento (tavoli, riserve, saga) | moduli senza invarianti numeriche o di stato (UI pura, logging) | Hypothesis (`@given`, `RuleBasedStateMachine`), seed registrato |
| M3 | Fault injection | persistenza, invio ordini, cashout, settlement, riavvio | codice senza I/O né stato durevole | processo figlio con `os._exit` (`tests/f001/child_crash.py`, `tests/f026/child_f026.py`), eccezioni iniettate, broker che perde o duplica risposte |
| M4 | Model checking | protocolli concorrenti: riserve atomiche, barriera cashout contro ingressi, lock di ammissione e reload config, ciclo di vita dell'ordine con AMBIGUOUS | logica sequenziale senza concorrenza | TLA+ con TLC (Java presente; `tla2tools.jar` da fissare a versione) oppure Apalache; in Python uno scheduler a punti di sincronizzazione sul codice reale |
| M5 | Verifica formale di proprietà | funzioni pure con limiti dimostrabili (liability LAY ≤ cap, stake ≥ 0, nessun arrotondamento verso l'alto) | codice con I/O | CrossHair o z3 sulle funzioni pure; `mypy --strict` sui moduli monetari |
| M6 | End-to-end sul software reale | ogni requisito finanziario: runtime reale, `SimulationBroker` reale, riavvio reale | nessuna esclusione per il denaro; la GUI reale solo dove c'è superficie visibile (Xvfb) | campagne F026/F001/Roserpina41, `headless_main` in processo figlio, screenshot Xvfb |

**Limite dichiarato.** Un modello astratto (M4) che supera il model checking dimostra il **protocollo**,
non il codice Python. Per legare il modello al codice serve almeno una verifica M2, M3 o M6 sulla stessa
proprietà, eseguita sul codice reale. Il codice non si dichiara "formalmente dimostrato" per il solo M4.

## 2. Invarianti critiche e copertura al 19/10/2026

Copertura misurata sull'head `6371030` della #522, da un inventario in sola lettura. Legenda: **F** forte,
**P** parziale, **A** assente.

| ID | Invariante | Verifiche esistenti principali | M1 | M2 | M3 | M4 | M5 | M6 | Copertura |
|---|---|---|---|---|---|---|---|---|---|
| I1 | Nessuna esposizione oltre i fondi verificati | `tests/test_microstake_engine_native.py` (fondi e headroom), `tests/f026/test_f026_units.py::test_account_invalid_or_incomplete_funds_fail_closed` | ✓ | – | – | – | – | parziale | **P**: controllo esplicito solo sul Microstake; `SimulationBroker` non rifiuta per fondi insufficienti |
| I2 | Liability LAY calcolata e coperta | `tests/acceptance/test_issue437_pr28.py` (LAY), `tests/integration/test_top30_math_gaps_part2.py::test_lay_liability_never_understated_and_monotonic`, `F026_02` | ✓ | – | – | – | – | ✓ | **F** su esempi; monotonia solo a campione |
| I3 | Nessuna doppia prenotazione | `tests/test_microstake_safe_recovery.py::test_concurrent_tables_reserve_atomically_and_never_exceed_cap`, `tests/acceptance/test_issue437_pr28d.py::test_concurrent_orders_cannot_bypass_total_cap` | ✓ | – | – | – | – | – | **P**: gare tra thread a campione |
| I4 | Nessun duplicato dopo timeout, crash o risposta persa | `tests/f001/test_b_red_f001.py::test_B_RED_09`, `::test_B_RED_12` (`os._exit`), `tests/acceptance/test_issue437_pr26.py` | ✓ | – | ✓ | – | – | ✓ | **F** |
| I5 | Nessun rilascio anticipato di riserve o tavoli | `tests/f001/test_f001_release_and_wait_units.py`, `tests/f026/test_f028_units.py`, `F026_04` | ✓ | – | – | – | – | ✓ | **F** su casi enumerati, non su tutte le sequenze |
| I6 | AMBIGUOUS conservati fino a riconciliazione | `tests/invariant/test_trading_engine_ambiguous_inflight_lock.py`, `F028_04`, `tests/chaos/test_trading_engine_chaos_kill_mid_submit.py` | ✓ | – | ✓ | – | – | ✓ | **F** |
| I7 | Cashout non contabilizzato senza conferma | `tests/unit/test_cashout_executor.py`, `tests/test_pr512_codex_034af59.py::test_c14_*`, `F026_10`, `F026_12` | ✓ | – | – | – | – | ✓ | **P**: nessun crash durante il cashout |
| I8 | Nessuna posizione fantasma o persa dopo restart | `tests/f026/test_f018_rebuild_units.py`, `F026_11b/c`, `F031` | ✓ | – | ✓ | – | – | ✓ | **F** |
| I9 | Contabilità di vincite, perdite, commissioni e settlement | `tests/f026/test_f026_financial.py` (oracolo indipendente), `tests/unit/test_pnl_engine_cleared_settlement.py` | ✓ | – | ✓ | – | – | ✓ | **F** sugli scenari scritti |
| I10 | Cap opzionali configurati rispettati | `tests/acceptance/test_issue437_pr28d.py`, `..._rectification.py`, `..._integration_512.py` | ✓ | – | – | – | – | parziale | **F** |
| I11 | Nessun cap monetario arbitrario attivo di default | `tests/acceptance/test_issue437_pr28d_rectification.py` (i 4 cap PR28-d e, dalla #522, `max_stake_abs`/`max_win`: default, migrazione, GUI, catena SIM reale) | ✓ | – | – | – | – | ✓ | **F** per i parametri noti; nessuna verifica generativa sui parametri futuri (G1) |
| C1 | Multi-tavolo concorrente | `tests/f001/test_b_red_f001.py::test_B_RED_15`, `::test_B_RED_14` | ✓ | – | – | – | – | ✓ | **P** |
| C2 | Cashout e ingressi sullo stesso evento | `tests/acceptance/test_issue437_pr28d.py` (barriere), `tests/acceptance/test_issue437_pr28d_integration_512.py` (barriera per partita e fino all'esito dell'hedge) | ✓ | – | – | – | – | – | **P** |
| C3 | Reload config durante l'ammissione | `tests/acceptance/test_issue437_pr28d_integration_512.py::test_config_reload_during_network_window_blocks_the_entry` (+ auto-next) | ✓ | – | – | – | – | – | **P** |
| C4 | Eventi fuori ordine o duplicati | `F026_08`, `tests/unit/test_pnl_engine_cleared_settlement.py`, `tests/chaos/test_resilience_replay_determinism.py` | ✓ | – | – | – | – | ✓ | **P**: duplicati sì, riordino arbitrario no |
| C5 | Crash durante invio, cashout, settlement, persistenza | `tests/f001/child_crash.py`, `tests/f026/child_f026.py` (`crash_now`, `crash_after_*`) | – | – | ✓ | – | – | ✓ | **P**: punti di crash scelti a mano; manca il crash durante il cashout |
| C6 | Isolamento SIM/LIVE | `tests/safety/test_live_gate_fail_closed_matrix.py` (esaustivo), `tests/integration/*_live_sim_parity.py` | ✓ | – | – | – | – | ✓ | **F** sul gate, **P** sulla parità dei fondi |

Altri fatti rilevati:
- **Strumenti assenti:** Hypothesis, TLA+/Apalache, CrossHair/z3 e `mypy --strict` non esistono oggi nel repository.
- **Mutation testing:** fino alla #525 `scripts/run_mutation_guardrails.py` mutava le specifiche JSON in `guardrails/`, non il codice Python. Dalla #525 applica mutazioni reali al sorgente da `guardrails/mutation_catalog.json`: 36 mutazioni su 11 aree obbligatorie (finanziarie e gate LIVE), soglia all'80%, nessun PASS su esiti inconcludenti o non applicabili. Dettagli in `docs/ci_orchestrators.md`.

## 3. Gap reali e verifiche da aggiungere

| Gap | Invarianti | Verifica concreta richiesta | Metodo |
|---|---|---|---|
| G1 | I2, I9, I10 | proprietà su `RoserpinaMoneyManagement.calculate` e `validators.order_exposure`: liability LAY ≥ 0, monotonia rispetto a quota e stake, nessuno stake sopra il cap configurato, nessun arrotondamento verso l'alto | M2 (+ M5 sulle funzioni pure) |
| G2 | I3, I5, I6 | `RuleBasedStateMachine` su `TableManager`, riserve e saga Microstake contro un modello di riferimento: acquisizione, attivazione, AMBIGUOUS, settlement, rilascio, riavvio | M2 |
| G3 | I3, C1, C2, C3 | specifiche TLA+ per riserva e tavolo, barriera cashout (con esito asincrono dell'hedge) contro ingressi, lock di ammissione e reload config; invarianti: nessuna doppia prenotazione, nessun ingresso nell'ambito di un cashout non concluso, nessun ordine con config obsoleta | M4 |
| G4 | C2, C3 | esplorazione deterministica degli interleaving sul codice reale: scheduler a punti di sincronizzazione nei test del `RuntimeController` (ammissione, finestra di rete, cashout, esito dell'hedge) | M4 sul codice |
| G5 | I7, C5 | crash a ogni passo di persistenza e durante il cashout: `os._exit` parametrico su ogni commit SQLite e su ogni passo di router, bridge ed executor, riusando `tests/f026/child_f026.py` | M3 + M6 |
| G6 | C4 | sequenze di eventi (settlement, FILLED, CANCELLED, CASHOUT_*) riordinate e duplicate, generate con seed, verificate contro l'oracolo F026 | M2 + M6 |
| G7 | I1 | parità SIM/LIVE sui fondi: il `SimulationBroker` deve poter rifiutare per fondi insufficienti come l'exchange; test E2E che lo dimostra | M6 |
| G8 | I1 – I11 | mutation testing sul sorgente dei moduli monetari (`core/money_management.py`, `core/validators.py`, `core/table_manager.py`, percorsi ordine del `RuntimeController`) con soglia di uccisione dichiarata | M1 (qualità) |
| G9 | I2, I10 | `mypy --strict` sui moduli monetari, come gate in CI | M5 |

## 4. Piano di adozione incrementale

Lo standard non blocca lo sviluppo: si integra nelle PR della roadmap #461 nell'ordine già approvato.
Resta una sola PR implementativa alla volta.

1. **#522 (in corso).** Si applicano M1, M3 e M6 ai difetti trovati (barriere, config, identità evento),
   già fatti. Questa specifica entra nel repository con la #522.
2. **PR strumenti (dopo la #522, a merge owner).** Si aggiungono `hypothesis` (e `mutmut` o `cosmic-ray`)
   a `requirements-test.txt`, `tla2tools.jar` a versione fissata e un workflow che li esegue. Manifest di
   dipendenze e `.github/workflows/` sono a merge owner (CLAUDE.md, MANUAL_AUTHORITY_PATTERNS).
3. **Finding #516–#521 (fase F).** Ogni fix porta i test M1 e M3 del difetto, più la proprietà M2 pertinente:
   - #516 cashout microstake → G2 e G5;
   - #517 soglia minima → G1;
   - #518 tooling → solo M1, motivato;
   - #519 auto-trade → G4;
   - #520 SIM broker → G7;
   - #521 settlement → G6.
4. **Schede #461.** Ogni scheda indica nella sua issue quali invarianti tocca e quali metodologie applica.
   Le esclusioni vanno motivate.
5. **Gate A/B (#506) e certificazione #351.** Vedi §5.

## 5. Integrazione con Gate A, Gate B e certificazione

- **Gate A (specifica):** per ogni PR o scheda, l'elenco delle invarianti toccate (§2), con proprietà,
  modello, test e oracolo applicabili. Ogni esclusione è motivata. Il Gate A globale resta distinto ed è
  NON PASS finché A01–A13 non hanno evidenze e revisione indipendente.
- **Gate B (esecuzione):** le verifiche del Gate A eseguite sul codice effettivo della PR, con evidenze nel
  formato del §7.
- **Certificazione #351:** si riesegue l'intera suite integrata (M1–M6) sulla baseline candidata al LIVE.
- **Blocco della promozione:** P0 o P1 operativi irrisolti, P2 con rischio finanziario non mitigato, oppure
  un'invariante critica di §2 non verificata. Nessun LIVE senza certificazione e autorizzazione owner.

## 6. Decisioni owner applicate dall'inventario

- **D-1 (risolta, rettifica owner del 19/10/2026).** `roserpina.max_stake_abs` (RSR-15) e `roserpina.max_win`
  (RSR-26) avevano un default di 10000 €: erano tetti monetari arbitrari attivi di default. Con la #522 sono
  opzionali e disattivati di default, e un valore presente ma non valido blocca (fail-closed). La verifica è in
  `tests/acceptance/test_issue437_pr28d_rectification.py` (default, migrazione del default storico, GUI,
  Money Management, catena SIM reale), `tests/core/test_risk_gate.py`,
  `tests/integration/test_dutching_max_win_guard.py` e `tests/roserpina41/test_rsr_targeted.py`.
  L'invariante I11 resta sotto verifica continua: ogni nuovo parametro monetario parte non attivo.

## 7. Formato delle evidenze

Ogni verifica registrata (nel registro `docs/traceability/requirements.json` o nella PR) riporta:

- requirement ID e invariante (§2);
- commit SHA e file interessati;
- versione degli strumenti, ambiente e dipendenze;
- comando di esecuzione;
- seed dei test generativi;
- fixture, fault schedule e trace;
- oracolo indipendente;
- esito `PASS` / `FAIL` / `BLOCKED` / `NOT_RUN`;
- log e artefatti;
- collegamento a finding e PR.

Per i modelli (M4) si aggiungono:

- assunzioni;
- stati e transizioni;
- invariante;
- limiti del modello;
- esito;
- eventuale controesempio riproducibile, convertito in un test M1, M3 o M6 sul codice reale.

## 8. Criteri di accettazione riproducibili

- Ogni nuova verifica gira da sola con il comando registrato, su un clone pulito, senza rete né credenziali.
- I test generativi registrano il seed. Un fallimento si riproduce con lo stesso seed.
- Un modello M4 dichiara la dimensione dello spazio degli stati esplorato e i limiti delle costanti.
- Ogni fix porta un test che fallisce sul codice precedente (RED) e passa dopo la correzione (GREEN). La
  mutazione della correzione fa fallire almeno un test.
- Nessuna verifica si dichiara `PASS` se non è stata eseguita sull'head dichiarato.
